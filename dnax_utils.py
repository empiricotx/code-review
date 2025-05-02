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

def default_tar_batch_dnax_runner(
        batch_processor: ETXBatchDeltaProcessor,
        batch_set: List,
        process_on_hdfs_func: Callable,
        pipeline_context = None,
        dnax_project_id: str = None,
        input_dir_hdfs_root: str = "/hdfs/inputs/",
        output_dir_hdfs_root: str = "/hdfs/output/",
        dnax_dir_metadata_field: str = "dnax_output_path",
):
    """Utility function to process a single batch with a client defined function (closure) process_on_hdfs_func.
    Handles downloading a specific batch, transfering it to HDFS, applying process_on_hdfs_func,
    optionally uploading tars to dnax project space, and optionally appending to a results delta table (e.g. variant_qc).

    In the case of glow genetic delta tables, and due to the limitations tied to this data and dnax,
    we are not appending raw genetic data to a delta tables on s3.  Instead, the process_on_hdfs_func
    will be responsible for writing out the results to a delta table on hdfs.  If UploadTarsParams
    is populated, then the results will be uploaded to dnax project space.

    Args:
        batch_processor (ETXProcessor): an initialized ETXProcessor
        batch_set (List[ETXDeltaBatch]): A collection of ETXDeltaBatches
        process_on_hdfs_func (Callable[[dict],dict]): A function that takes an ETXBatchHDFSParams and returns an ETXBatchOutputInfo
            typically, for a given batch, reference data existing on hdfs, process data, and write out results back to hdfs
            return dataframe and metadata wrapped in ETXBatchOutputInfo
        dnax_dir_metadata_field (str, optional): Each batch's metadata field, by convention, should have a reference to a dnax root location corresponding to that batches output. Defaults to the metadata field "dnax_output_path".
    """
    # Check if input and output dirs end with slash
    if not str(input_dir_hdfs_root).endswith('/'):
        input_dir_hdfs_root = input_dir_hdfs_root + '/'
    if not output_dir_hdfs_root.endswith('/'):
        output_dir_hdfs_root = output_dir_hdfs_root + '/'

    pc = pipeline_context
    l = pc.get_logger()

    # check for existing batches - this allows retries within batch sets
    b_ids = [b.batch_id for b in batch_processor.existing_batches()]
    existing_batch_ids = set(b_ids)
    target_batch_id_set = set(
        [b.batch_id for b in batch_set if b.batch_id not in existing_batch_ids]
    )

    # Print some info
    num = len(target_batch_id_set)
    s = "\n".join(target_batch_id_set)
    l.info(
        f"BATCH-GROUP-INFO- num_batches in batch_group: {num}- batches = {s}"
    )

    use_file_list_manifest = False
    inspect_downloads = False

    # Loop through all batches in the set
    for b in batch_set:
        if (
                b.batch_id in target_batch_id_set
        ):
            from etxlib.core.common.utils import get_pst_time as pst
            # Log batch processing
            l.info(f"processing batch {b}")
            t1 = pst()  # Start time

            # Set up download directory
            outdir_local = f"/tmp/downloads/{b.batch_id}/"  # TODO cleanup download dirs may be needed for smaller drivers

            # Get DNAX folder path from metadata
            l.info("generating manifest and downloading")
            dnax_folder = b.metadata.get(dnax_dir_metadata_field)
            if dnax_folder == None:
                print(f"batch {b} does not have metadata field {dnax_dir_metadata_field} needed to reference batch data on dnax, attempting legacy field, 'dnax_dir'")
                dnax_folder = b.metadata.get("dnax_dir")
                # if dnax_folder == None:
                #     raise ValueError(
                #         f"batch {b} does not have metadata field {dnax_dir_metadata_field} nor 'dnax_dir' needed to reference batch data on dnax"
                #     )
            # else:
            #     print(f"{dnax_folder=}")
            #     process_fn(dnax_folder)

            def get_batch_root_dir(dnax_path: str) -> str:
                """Helper function to get the root dir of a batch on dnax

                Args:
                    dnax_folder_path (str): path to a dataset or it's root dir

                Returns:
                    str: the root dir of a given path on dnax
                """
                return "/".join(dnax_path.split("/")[:-1]) + "/"

                # Download the files

            os.makedirs(outdir_local, exist_ok=True)

            manifest_file = f"/tmp/manifest.{b.batch_id}.json.bz2"
            if os.path.exists(manifest_file):
                os.remove(manifest_file)
            fns_downloaded = glob.glob(f"{dnax_folder}/*")

            if use_file_list_manifest and fns_downloaded:
                # Use the new generate_manifest_from_file_list function
                filename_folder_pairs = [
                    (os.path.basename(f), get_batch_root_dir(
                        dnax_folder
                    )) for f in fns_downloaded
                ]
                manifest_data = generate_manifest_from_file_list(
                    filename_folder_pairs, dnax_project_id
                )

                # Write the manifest to a file
                with open(manifest_file, "wb") as f:
                    json_manifest = json.dumps(manifest_data, indent=2, sort_keys=True)
                    f.write(bz2.compress(json_manifest.encode("utf-8")))
            else:
                # Use the regex generate_dxda_manifest function
                generate_dxda_manifest(
                    dx_project_id=dnax_project_id,
                    folderpath=get_batch_root_dir(
                        dnax_folder
                    ),
                    outfile=manifest_file,
                    files_to_download=fns_downloaded,
                    regex_filter=f"{b.batch_id}*",
                )

            download_dnax_files_dxda(manifest_file, outdir_local)

            if inspect_downloads:
                inspect_and_retry_download_dxda(manifest_file, outdir_local)

            fn_dir_downloaded = os.path.join(outdir_local, get_batch_root_dir(
                dnax_folder
            )[1:])
            downloaded_dir = os.path.join(outdir_local, "data")
            os.rename(fn_dir_downloaded, downloaded_dir)

            l.info(f"files deposited to local disk at {downloaded_dir}")

            # Check what files we got
            fns_downloaded = glob.glob(f"{downloaded_dir}/*")
            nfiles = len(fns_downloaded)

            l.info("before_untar: %s" % fns_downloaded)
            # sleep for 5 seconds
            time.sleep(5)

            # Untar the files
            # untar_tarballs(downloaded_dir)
            # fns = glob.glob(f"{downloaded_dir}/*/*")
            untar_tarballs(downloaded_dir)
            fns = glob.glob(f"{downloaded_dir}/*")
            l.info("untared tarballs")
            l.info("after untar: %s" % fns)

            # Move to HDFS
            l.info("moving data to hdfs")
            move_dir_to_hdfs(downloaded_dir, outdir_hdfs=input_dir_hdfs_root)
            l.info("moving data to hdfs complete")

            # Process on HDFS
            params = ETXBatchHDFSParams(
                batch_id=b.batch_id,
                hdfs_input_root_path=input_dir_hdfs_root,
                hdfs_output_root_path=output_dir_hdfs_root,
            )
            # call process_on_hdfs_func
            results = process_on_hdfs_func(params)

            # Handle metadata
            metadata = results.metadata

            if results.upload_tars != None:
                outpath = os.path.join(
                    results.upload_tars.source_root_dir_hdfs,
                    results.upload_tars.output_filename,
                )
                l.info(
                    "tar'ing and uploading {outpath} to {opr}".format(outpath=outpath, opr=results.upload_tars.dnax_output_root_path)
                )

                # Do the upload
                delete_unclosed_files = True
                force = True
                if delete_unclosed_files:
                    not_closed = list_open_files_in_folder(results.upload_tars.project_id, results.upload_tars.dnax_output_root_path)
                    dxpy.api.project_remove_objects(
                        results.upload_tars.project_id, input_params={"objects": [f.id for f in not_closed], "force": force}
                    )
                try:
                    result = subprocess.check_output(
                        [
                            "/scripts/etx_upload_tars.sh",
                            results.upload_tars.outputs_folder_hdfs,
                            results.upload_tars.output_filename,
                            results.upload_tars.project_id,
                            results.upload_tars.dnax_output_root_path,
                        ]
                    ).decode("utf-8")
                except:
                    print("Error occurred")
                    raise


                time.sleep(5) # sleep for 5 seconds
                l.info(f"etx_upload_tars.sh output: {result}")


                # Set the output path in metadata
                op = os.path.join(
                    results.upload_tars.dnax_output_root_path,
                    results.upload_tars.output_filename,
                )
                # Make sure there's a reference to where the data was uploaded
                metadata["dnax_output_path"] = op
                print("completed tar'ing and uploading")

            # Make metadata
            t2 = pst()
            diff = (t2 - t1)
            t = diff.total_seconds()
            metadata["batch_start_time"] = str(t1)
            metadata["batch_completed_processing_time"] = str(t2)
            metadata["elapsed_time_minutes"] = str(t)
            metadata["pipeline_job_name"] = str(pipeline_context.get_job_name())
            metadata["pipeline_step"] = str(pipeline_context.get_step())
            metadata["pipeline_parent_run_id"] = str(pipeline_context.get_parent_run_id())
            metadata["pipeline_tags"] = str(pipeline_context.get_tags())
            metadata["pipeline_framework_name"] = str(pipeline_context.get_pipeline_framework_name())
            metadata["githash"] = str(auto_version.get("GITHUB_SHA", "unknown")) # set githash
            metadata["part_of_batch_set"] = ", ".join(target_batch_id_set)

            # Add dnax-specific environment info to metadata
            if os.environ.get("DX_JOB_ID"): metadata["dnax_job_id"] = os.environ["DX_JOB_ID"]
            if os.environ.get("DNAX_INSTANCE_TYPE"): metadata["instance_type"] = os.environ["DNAX_INSTANCE_TYPE"]
            if os.environ.get("DNAX_INSTANCE_COUNT"): metadata["instance_count"] = os.environ["DNAX_INSTANCE_COUNT"]

            # Append the batch to the registry with the relevant metadata
            batch_processor.append_new_batch(
                batch_id=b.batch_id,
                df=results.result_df,
                metadata=metadata,
                include_batch_id_col=True,
                partitionBy=["locus.contig"],
            )
            l.info(f"completed appending batch {b.batch_id} with metadata {metadata}")
        else:
            l.warning(f"batch {b} already exists in batch registry")

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
