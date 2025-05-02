import bz2
import datetime

from dataclasses import dataclass
from logging import Logger
from typing import Callable, Dict, List, Optional

from etxlib.core.common.auto_version import auto_version

import dxpy
import dxpy.api

from tenacity import retry
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential


from etxlib.etxdagster.common.etx_batch_append_delta_processor import (
    ETXBatchDeltaProcessor,
)

import pandas as pd
import pytz

import requests
from dagster import OpExecutionContext
from pyspark.sql import DataFrame
import glob
import json
import os
import re
import shutil
import subprocess
import time


@dataclass
class ETXBatchHDFSParams:
    batch_id: str
    hdfs_input_root_path: str
    hdfs_output_root_path: str


@dataclass
class UploadTarsParams:
    project_id: str  # dnax project id
    source_root_dir_hdfs: str  # hdfs root dir containing data (i.e. mt or delta file)
    output_filename: str  # filename of mt, delta file, etc
    dnax_output_root_path: str  # root project directory on dnax to place above file


@dataclass
class ETXBatchOutputInfo:
    batch_id: str
    result_df: Optional[
        DataFrame
    ]  # optional dataframe to append to target delta table (e.g. variant_qc)
    upload_tars: Optional[UploadTarsParams]
    metadata: Dict[str, str]


class PipelineContext:
    def get_job_name(self) -> Optional[str]:
        pass

    def get_step(self) -> Optional[str]:
        pass

    def get_parent_run_id(self) -> Optional[str]:
        pass

    def get_tags(self) -> Optional[str]:
        pass

    def get_logger(self) -> Logger:
        return Logger("default")

    def get_pipeline_framework_name(self) -> Optional[str]:
        pass


class DagsterPipelineContext(PipelineContext):
    def __init__(self, dagster_context: OpExecutionContext):
        self.dagster_context = dagster_context

    def get_job_name(self):
        return self.dagster_context.job_name

    def get_step(self):
        return self.dagster_context.get_step_execution_context().step.key

    def get_parent_run_id(self):
        return self.dagster_context.run_id

    def get_tags(self):
        return str(self.dagster_context.op.tags)

    def get_logger(self) -> Logger:
        return self.dagster_context.log

    def get_pipeline_framework_name(self):
        return "dagster"



def dnax_delete_file(project_id, file_id):
    cmd = f"dx rm {project_id}:{file_id}"
    subprocess.run(cmd, shell=True)


def clean_up_dnax_folder(foldername, dx_project_id, delete_files=False):
    print(f"{foldername=}")

    def _lambda_get_human_readable_timestamp(x):
        tz = pytz.timezone("America/Los_Angeles")
        your_dt = datetime.datetime.fromtimestamp(
            int(x) / 1000, tz
        )  # using the local timezone
        output_date_string = your_dt.strftime("%Y-%m-%d %H:%M:%S")
        return output_date_string

    api_call_ids = dxpy.api.container_list_folder(
        dx_project_id,
        {"folder": foldername, "describe": {"id": True, "name": True, "parts": False}},
        always_retry=True,
    )

    fileinfo = api_call_ids["objects"]
    data = []
    for f in fileinfo:
        dx_id = f["describe"]["id"]
        filename = f["describe"]["name"]
        folder = f["describe"]["folder"]
        size = f["describe"]["size"]
        created = f["describe"]["created"]
        data.append([dx_id, filename, folder, size, created])
    summary = pd.DataFrame(data, columns=["id", "name", "folder", "size", "created"])
    summary["created_date_string"] = summary["created"].apply(
        lambda x: _lambda_get_human_readable_timestamp(x)
    )

    def _get_files_to_delete(summary):
        vc = summary["name"].value_counts()
        duplicate_items = summary[(summary["name"].isin(vc[vc > 1].index))].sort_values(
            ["name", "created"], ascending=[True, False]
        )
        to_delete = duplicate_items.drop_duplicates(subset=["name"], keep="first")
        num_to_remove = to_delete.shape[0]
        print(f"{num_to_remove=}")
        return to_delete, duplicate_items

    to_delete, duplicate_items = _get_files_to_delete(summary)
    if delete_files:
        if to_delete.shape[0] > 0:
            threshold = 10
            increment = 10
            count = 0
            for fid in to_delete["id"].tolist():
                dnax_delete_file(dx_project_id, fid)
                if count == threshold:
                    print(f"deleted {threshold} files")
                    threshold += increment
                count += 1

        else:
            print("no files to delete")

    return summary, to_delete, duplicate_items


def move_dir_to_hdfs(
        local_dir: str,
        outdir_hdfs: str = "/hdfs/inputs/",
        log: Optional[Logger] = None,
):
    local_dir_encoded = local_dir.replace(" ", "%20")
    if local_dir_encoded[-1] == "/":
        local_dir_encoded = local_dir_encoded[:-1]

    cmds = [
        f"hadoop fs -mkdir -p {outdir_hdfs}",
        f"hadoop fs -copyFromLocal -f -t 15 {local_dir_encoded}/* {outdir_hdfs}",
        f"hadoop fs -ls {outdir_hdfs}",
        f"ls {local_dir_encoded}/*",
        f"rm -rf {local_dir_encoded}/*",
    ]

    for cmd in cmds:
        result = subprocess.run(cmd, shell=True, capture_output=True)
        if log:
            log.debug(result)
        if cmd == cmds[1]:
            # Wait for the copy to complete
            time.sleep(5)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=3, max=60),
    retry=retry_if_exception_type(subprocess.CalledProcessError),
    reraise=True,
)
def untar_tarballs(local_dir_with_tars: str, log: Optional[Logger] = None):
    """Untar all tarballs in a directory.
    Args:
        local_dir_with_tars (str): Directory containing tarballs
        log (Logger): Logger object
    """
    if log:
        log.info(f"parallel untar in {local_dir_with_tars}")
    cmd1 = f"cd {local_dir_with_tars} && ls *.tar|parallel tar -xf"
    try:
        # Run with check=True to raise CalledProcessError if it fails
        result = subprocess.run(
            cmd1, shell=True, capture_output=True, check=True, text=True
        )
        if log:
            log.debug(result.stdout)

    except subprocess.CalledProcessError as e:
        if log:
            log.error(f"Failed to untar files: {e.stderr}")
        raise e
    time.sleep(5)

    if log:
        log.info("cleaning up tars")
    paths_to_remove = [
        os.path.join(local_dir_with_tars, f)
        for f in os.listdir(local_dir_with_tars)
        if f.endswith(".tar")
    ]
    for p in paths_to_remove:
        os.remove(p)


def download_dnax_files_dxda(manifest, outdir_local):
    """Wrapper for the dxda DNAnexus utility to download a set of files
    Args:
        manifest (str): Complete path to the manifest.json.bz2 file
        outdir (str): Directory to store the output of all downloads
        override_outdir (bool): if true, force outputs from the manifest into top level directory outdir (removing folder nesting) - useful for getting around weird naming
    """
    if not os.path.exists(outdir_local):
        os.mkdir(outdir_local)
    subprocess.run(["dx-download-agent-linux", "download", manifest], cwd=outdir_local)
    time.sleep(5)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=3, max=60),
    retry=retry_if_exception_type(subprocess.CalledProcessError),
    reraise=True,
)
def inspect_and_retry_download_dxda(manifest, outdir_local):
    """Uses the dxda DNAnexus utility to check MD5 hashes of a set of downloaded files,
    and retries downloading any files that fail.
    Args:
        manifest (str): Complete path to the manifest.json.bz2 file
        outdir (str): Directory to store the output of all downloads
        retries (int): Number of times to retry the download
    """
    print("Inspecting downloaded files...")
    try:
        subprocess.run(
            ["dx-download-agent-linux", "inspect", manifest],
            cwd=outdir_local,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print("Corrupt files detected, retrying download...")
        download_dnax_files_dxda(manifest, outdir_local)
        raise e


def generate_manifest_from_file_list(
        filename_folder_pairs, dx_project_id, mode="exact", limit=1
):
    """
    Generates a manifest for a list of filename and foldername pairs.

    Args:
    filename_folder_pairs (list): List of tuples containing (filename, folder).
    dx_project_id (str): DNAnexus project ID.
    mode (str): Mode for matching the filename ("exact" or "glob"). Defaults to "exact".
    limit (int): Limit on the number of files to fetch per filename. Defaults to 1.

    Returns:
    dict: A manifest of files with their details.
    """

    manifest = {dx_project_id: []}
    kwarg_fields = {
        "id": True,
        "project": True,
        "class": True,
        "sponsored": True,
        "name": True,
        "types": True,
        "state": True,
        "hidden": True,
        "links": True,
        "folder": True,
        "tags": True,
        "created": True,
        "modified": True,
        "createdBy": True,
        "media": True,
        "archivalState": True,
        "size": True,
        "cloudAccount": True,
        "parts": True,
    }

    for filename, folder in filename_folder_pairs:
        # Find files based on the filename and folder
        files = dxpy.find_data_objects(
            classname="file",
            name=filename,
            limit=limit,
            project=dx_project_id,
            name_mode=mode,
            folder=folder,
        )
        fids = [f["id"] for f in files]

        # For each file, fetch its details and store in the manifest
        for file_id in fids:
            file_desc: Dict = dxpy.describe(file_id, fields=kwarg_fields)
            file_info = {
                "id": file_id,
                "name": file_desc["name"],
                "folder": file_desc["folder"],
                "parts": {
                    pid: {k: v for k, v in pinfo.items() if k in ["md5", "size"]}
                    for pid, pinfo in file_desc.get("parts", {}).items()
                },
            }
            manifest[dx_project_id].append(file_info)

    return manifest


def generate_dxda_manifest(
        dx_project_id, folderpath, outfile, files_to_download=None, regex_filter=".*"
):
    """Creates a manifest file for DNAnexus dxda utility to download files
    from a particular folder within a project space
    Args:
        dx_project_id (str): DNAnexus project identifier
        folderpath (str): Location of the files to download
        outfile (str): Complete filepath of output manifest.json.bz2 file
        files_to_download (List[str], optional): List of specific files to download
        regex_filter (str, optional): Regex pattern to filter files
    """
    api_call_ids = dxpy.api.container_list_folder(
        dx_project_id,
        {"folder": folderpath, "describe": {"id": True, "name": True, "parts": True}},
        always_retry=True,
    )
    fileinfo = api_call_ids["objects"]

    manifest = {dx_project_id: []}
    counter = 0

    files_to_download_set = set(os.path.basename(f) for f in (files_to_download or []))

    for e in fileinfo:
        filename = e["describe"]["name"]
        if (
                not files_to_download_set or filename in files_to_download_set
        ) and re.search(regex_filter, filename):
            filemetadata = {
                "id": e["id"],
                "name": filename,
                "folder": folderpath,
                "parts": {
                    pid: {k: v for k, v in pinfo.items() if k in ["md5", "size"]}
                    for pid, pinfo in e["describe"]["parts"].items()
                },
            }
            manifest[dx_project_id].append(filemetadata)
            counter += 1

    if counter == 0:
        raise ValueError("No entries were added to the manifest dictionary!")

    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    with open(outfile, "wb") as f:
        json_manifest = json.dumps(manifest, indent=2, sort_keys=True)
        f.write(bz2.compress(json_manifest.encode("utf-8")))


def generate_manifest_and_download(
        dx_project_id,
        folderpath,
        outdir_local,
        files_to_download=None,
        regex_filter=".*",
        manifest_file="/tmp/manifest.json.bz2",
        override_outdir=True,
        inspect_downloads=False,
        use_file_list_manifest=False,
):
    os.makedirs(outdir_local, exist_ok=True)

    if os.path.exists(manifest_file):
        os.remove(manifest_file)

    if use_file_list_manifest and files_to_download:
        # Use the new generate_manifest_from_file_list function
        filename_folder_pairs = [
            (os.path.basename(f), folderpath) for f in files_to_download
        ]
        manifest_data = generate_manifest_from_file_list(
            filename_folder_pairs, dx_project_id
        )

        # Write the manifest to a file
        with open(manifest_file, "wb") as f:
            json_manifest = json.dumps(manifest_data, indent=2, sort_keys=True)
            f.write(bz2.compress(json_manifest.encode("utf-8")))
    else:
        # Use the regex generate_dxda_manifest function
        generate_dxda_manifest(
            dx_project_id=dx_project_id,
            folderpath=folderpath,
            outfile=manifest_file,
            files_to_download=files_to_download,
            regex_filter=regex_filter,
        )

    download_dnax_files_dxda(manifest_file, outdir_local)

    if inspect_downloads:
        inspect_and_retry_download_dxda(manifest_file, outdir_local)

    fn_dir_downloaded = os.path.join(outdir_local, folderpath[1:])
    if override_outdir:
        outdir_data = os.path.join(outdir_local, "data")
        os.rename(fn_dir_downloaded, outdir_data)
        return outdir_data

    return fn_dir_downloaded


def generate_manifest_and_download_from_ids(
        dx_project_id: str,
        dnax_file_ids: List[str],
        outdir_local: str,
        manifest_file: str = "/tmp/manifest_from_ids.json.bz2",
        override_outdir: bool = True,
        inspect_downloads: bool = False,
) -> str:
    """Like generate_manifest_and_download, but takes a list of file IDs
    instead of a regex and folder path.
    """
    manifest = {dx_project_id: []}

    for file_id in dnax_file_ids:
        file_desc = dxpy.describe(file_id)
        file_info = {
            "id": file_id,
            "name": file_desc["name"],
            "folder": file_desc["folder"],
            "parts": {
                pid: {k: v for k, v in pinfo.items() if k in ["md5", "size"]}
                for pid, pinfo in file_desc.get("parts", {}).items()
            },
        }
        manifest[dx_project_id].append(file_info)

    os.makedirs(os.path.dirname(manifest_file), exist_ok=True)
    with open(manifest_file, "wb") as f:
        json_manifest = json.dumps(manifest, indent=2, sort_keys=True)
        f.write(bz2.compress(json_manifest.encode("utf-8")))

    download_dnax_files_dxda(manifest_file, outdir_local)

    if inspect_downloads:
        inspect_and_retry_download_dxda(manifest_file, outdir_local)

    if override_outdir:
        outdir_data = os.path.join(outdir_local, "data")
        os.rename(os.path.join(outdir_local, file_desc["folder"][1:]), outdir_data)
        return outdir_data

    return os.path.join(outdir_local, file_desc["folder"][1:])


def download_and_move_fns_manifest(
        fns, outdir_local, dnax_project, dir_hdfs, logger: Logger
):
    for i, desired_fn in enumerate(fns):
        fn_manifest = f"/tmp/manifest.{i}.json.bz2"
        fn_dir_downloaded = generate_manifest_and_download(
            dx_project_id=dnax_project,
            folderpath="/Bulk/Exome sequences/Population level exome OQFE variants, pVCF format - final release",
            regex_filter=f"{desired_fn}",
            manifest_file=fn_manifest,
            override_outdir=True,
            outdir_local=outdir_local,
        )
        logger.info(f"{desired_fn} downloaded to {outdir_local}")
        move_dir_to_hdfs(fn_dir_downloaded, outdir_hdfs=dir_hdfs)


def check_file_sizes_hadoop(dir_hadoop):
    cmd = f"hadoop fs -ls {dir_hadoop}"
    # print("test hadoop stuff")
    sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    output = ""
    if sp.stdout is not None:
        for l in sp.stdout:
            l = l.decode("utf-8")
            output += l

    output_rows = output.split("\n")[0:-1]
    output_rows = [i.split() for i in output_rows]
    df = pd.DataFrame(output_rows)
    df = df.rename(columns={4: "size", 7: "fn"})
    df = df[df["fn"].notnull()]

    return df


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=3, max=60),
    retry=retry_if_exception_type(subprocess.CalledProcessError),
    reraise=True,
)
def upload_tars(
        project: str,
        outputs_folder_hdfs: str,
        output_filename: str,
        log: Logger,
        dnax_dir: str,
        delete_unclosed_files: bool = True,
):
    if delete_unclosed_files:
        delete_not_closed_dnax_files(project, dnax_dir)
    try:
        result = subprocess.check_output(
            [
                "/scripts/etx_upload_tars.sh",
                outputs_folder_hdfs,
                output_filename,
                project,
                dnax_dir,
            ]
        ).decode("utf-8")
    except subprocess.CalledProcessError as cpe:
        log.error(
            f"Error occurred while running /scripts/etx_upload_tars.sh: {cpe.output}"
        )
        log.error(f"Exit code: {cpe.returncode}")
        raise cpe

    time.sleep(5)
    log.info(f"etx_upload_tars.sh output: {result}")


@dataclass
class DnaxFileMetadata:
    size: int
    state: str
    created: int
    project: str
    id: str
    folder: str
    name: str


def list_open_files_in_folder(project_id: str, folder: str) -> List[DnaxFileMetadata]:
    files = dxpy.find_data_objects(
        classname="file",
        project=project_id,
        folder=folder,
        return_handler=True,
        describe={
            "fields": {
                "state": True,
                "size": True,
                "created": True,
                "project": True,
                "id": True,
                "folder": True,
                "name": True,
            }
        },
    )

    not_closed = []
    for f in files:
        # describe() method provides details of the file which also includes the 'state'
        file_description = f["describe"]
        if (
                file_description["state"] == "closing"
                or file_description["state"] == "open"
        ):  # or use 'open' based on what status you get for unclosed files
            not_closed.append(DnaxFileMetadata(**file_description))
    return not_closed


def delete_not_closed_dnax_files(project: str, folder: str):
    not_closed = list_open_files_in_folder(project, folder)
    dxpy.api.project_remove_objects(
        project, input_params={"objects": [f.id for f in not_closed], "force": True}
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=3, max=60),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def upload_to_dnanexus_dxpy(
        project: str,
        local_file_path: str,
        dnax_dir: str,
        log: Optional[Logger] = None,
):
    """Upload a file to DNAnexus using dxpy."""

    try:
        # Ensure the DNAnexus project exists
        proj = dxpy.DXProject(project)

        # Ensure the destination directory exists
        try:
            proj.list_folder(folder=dnax_dir)
        except dxpy.exceptions.DXAPIError as e:
            if "could not be found" in str(e):
                proj.new_folder(folder=dnax_dir, parents=True)
                if log:
                    log.info(f"Created folder {dnax_dir} in project {project}")

        # Upload the file
        dxpy.upload_local_file(
            filename=local_file_path,
            project=proj.get_id(),
            folder=dnax_dir,
            wait_on_close=True,
        )

        if log:
            log.info(
                f"File {local_file_path} uploaded to project {project} in folder {dnax_dir}"
            )

    except Exception as e:
        if log:
            log.error(f"Error uploading file: {e}")
        raise e


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=3, max=60),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def download_from_dnanexus_dxpy(
        project: str,
        remote_file_path: str,
        local_destination: str,
        dxid: Optional[str] = None,
        log: Optional[Logger] = None,
) -> str:
    """Download a file from DNAnexus using dxpy.
    Args:
        project (str): DNAnexus project ID
        remote_file_path (str): Path to the file within the project to download
        local_destination (str): Local directory to download the file to
        dxid (str, optional): DNAnexus file ID. Defaults to None. Useful if the file has duplicate names.
        log (Logger, optional): Logger object. Defaults to None.
    """

    # Ensure the DNAnexus project exists
    proj = dxpy.DXProject(project)

    # Ensure the destination directory exists
    if not os.path.exists(local_destination):
        os.makedirs(local_destination)

    # Extract folder and filename from the remote path
    remote_folder, remote_filename = os.path.split(remote_file_path)

    if not dxid:
        # Locate the file within the project
        search_results = list(
            dxpy.find_data_objects(
                project=proj.get_id(),
                folder=remote_folder,
                name=remote_filename,
                classname="file",
                return_handler=False,
            )
        )

        if not search_results:
            raise Exception(f"File {remote_file_path} not found in project {project}")

        dxid = search_results[0]["id"]

    # Download file
    try:
        dxpy.bindings.dxfile_functions.download_dxfile(
            dxid, os.path.join(local_destination, remote_filename)
        )

        if log:
            log.info(f"File {remote_file_path} downloaded to {local_destination}")

    except Exception as e:
        if log:
            log.error(f"Error downloading file: {e}")
        raise e
    return os.path.join(local_destination, remote_filename)


def download_and_install_dxda():
    """Download and install the dxda DNAnexus utility.
    A workaround for cases when the applet doesn't have the utility installed."""

    url = "https://github.com/dnanexus/dxda/releases/download/v0.6.0/dx-download-agent-linux"
    local_filename = "/tmp/dx-download-agent-linux"

    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    os.chmod(local_filename, 0o755)
    os.rename(local_filename, "/usr/bin/dx-download-agent-linux")


def copy_from_hdfs_to_local(hdfs_path, local_path, log: Optional[Logger] = None):
    """Copy a file from HDFS to local disk."""
    try:
        # Running the command
        cmd = ["hadoop", "fs", "-copyToLocal", hdfs_path, local_path]
        subprocess.check_call(cmd)
        if log:
            log.info(f"Copied {hdfs_path} to {local_path} successfully")
    except subprocess.CalledProcessError as e:
        if log:
            log.error(f"Error executing command: {e}")
    except Exception as e:
        if log:
            log.error(f"An error occurred: {e}")


def tar_data(file_path, log: Optional[Logger] = None):
    """Tar a directory containing a MatrixTable file. Useful for uploading to DNAnexus."""
    parent_dir, dir_to_tar = os.path.split(file_path)
    tar_file = os.path.join(parent_dir, dir_to_tar + ".tar")
    if log:
        log.info(f"Creating tar file {tar_file}")

    # Switch to parent directory, create tar, then switch back
    original_cwd = os.getcwd()
    try:
        os.chdir(parent_dir)
        subprocess.check_call(["tar", "-cf", tar_file, dir_to_tar])
    finally:
        os.chdir(original_cwd)

    return tar_file


def tar_and_upload_without_chunking(
        file_path: str,
        dnax_out_dir: str,
        project: str,
        clean_up_after_upload: bool = True,
        log: Optional[Logger] = None,
):
    """Tar a directory, and upload it to DNAnexus.
    Args:
        file_path (str): Path to the directory to tar
        dnax_out_dir (str): Destination directory on DNAnexus to upload the tar file
        project (str): DNAnexus project ID
        clean_up_after_upload (bool, optional): Whether to clean up local files after upload. Defaults to True.
        log (Logger, optional): Logger object. Defaults to None.
    """
    tmp_path = os.path.join("/tmp", os.path.basename(file_path))
    copy_from_hdfs_to_local(
        hdfs_path=file_path,
        local_path=tmp_path,
        log=log,
    )
    tar_file = tar_data(tmp_path)
    upload_to_dnanexus_dxpy(
        project=project,
        local_file_path=tar_file,
        dnax_dir=dnax_out_dir,
        log=log,
    )
    if log:
        log.info("Cleaning up after upload...")
    if clean_up_after_upload:
        os.remove(tar_file)
        shutil.rmtree(tmp_path)
    return tar_file
