# todo_manager.py

from typing import List, Optional, Dict
from datetime import datetime
import json
from enum import Enum
from dataclasses import dataclass, asdict, field
from pathlib import Path


class Priority(Enum):
    """Priority levels for tasks.
    
    Attributes:
        LOW: Low priority task
        MEDIUM: Medium priority task (default)
        HIGH: High priority task
        URGENT: Urgent priority task
    """
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    URGENT = 4


@dataclass
class Task:
    """Represents a single task in the to-do list with enhanced features.
    
    Attributes:
        title: The title or name of the task
        description: Detailed description of the task
        completed: Whether the task has been completed
        priority: Priority level of the task (default: MEDIUM)
        due_date: Optional deadline for the task
        categories: List of categories/tags associated with the task
        created_at: Timestamp when the task was created
    """

    title: str
    description: str = ""
    completed: bool = False
    priority: Priority = Priority.MEDIUM
    due_date: Optional[datetime] = None
    categories: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if self.categories is None:
            self.categories = []
        if self.created_at is None:
            self.created_at = datetime.now()

    def mark_done(self) -> None:
        """Marks the task as completed.
        
        This method sets the completed flag to True, indicating that the task
        has been finished.
        """
        self.completed = True

    def add_category(self, category: str) -> None:
        """Adds a category to the task.
        
        Args:
            category: The category name to add to the task
            
        Note:
            Duplicate categories are automatically prevented.
        """
        if category not in self.categories:
            self.categories.append(category)

    def remove_category(self, category: str) -> None:
        """Removes a category from the task.
        
        Args:
            category: The category name to remove from the task
            
        Note:
            If the category doesn't exist, no action is taken.
        """
        if category in self.categories:
            self.categories.remove(category)

    def is_overdue(self) -> bool:
        """Checks if the task is overdue.
        
        Returns:
            bool: True if the task has a due date that has passed and is not completed,
                  False otherwise.
        """
        if self.due_date is None:
            return False
        return datetime.now() > self.due_date and not self.completed

    def __repr__(self) -> str:
        """Returns a string representation of the task.
        
        Returns:
            str: A string containing the task's title, priority, and completion status.
        """
        return f"Task(title={self.title!r}, priority={self.priority.name}, completed={self.completed})"


class TodoList:
    """Manages a collection of tasks with persistence and advanced querying capabilities.
    
    This class provides functionality to manage tasks including adding, removing,
    and querying tasks based on various criteria. It also handles persistence
    of tasks to a JSON file.
    
    Attributes:
        tasks: List of Task objects
        storage_path: Path to the JSON file for task persistence
    """

    def __init__(self, storage_path: str = "tasks.json"):
        """Initializes the TodoList with optional persistence.
        
        Args:
            storage_path: Path to the JSON file for storing tasks (default: "tasks.json")
        """
        self.tasks: List[Task] = []
        self.storage_path = Path(storage_path)
        self.load_tasks()

    def add_task(self, task: Task) -> None:
        """Adds a new task to the list and saves to storage.
        
        Args:
            task: The Task object to add to the list
        """
        self.tasks.append(task)
        self.save_tasks()

    def remove_task(self, t):
        """Removes a task from the list and updates storage.
        
        Args:
            task: The Task object to remove from the list
        """
        if t in self.tasks:
            self.tasks.remove(t)
            self.save_tasks()

    def list_pending(self) -> List[Task]:
        """Retrieves all incomplete tasks.
        
        Returns:
            List[Task]: A list of tasks that are not marked as completed
        """
        return [task for task in self.tasks if not task.completed]

    def list_completed(self) -> List[Task]:
        """Retrieves all completed tasks.
        
        Returns:
            List[Task]: A list of tasks that are marked as completed
        """
        return [task for task in self.tasks if task.completed]

    def list_overdue(self) -> List[Task]:
        """Retrieves all overdue tasks.
        
        Returns:
            List[Task]: A list of tasks that have passed their due date and are not completed
        """
        return [task for task in self.tasks if task.is_overdue()]

    def get_tasks_by_priority(self, priority: Priority) -> List[Task]:
        """Retrieves tasks with a specific priority level.
        
        Args:
            priority: The Priority level to filter tasks by
            
        Returns:
            List[Task]: A list of tasks matching the specified priority
        """
        return [task for task in self.tasks if task.priority == priority]

    def get_tasks_by_category(self, category: str) -> List[Task]:
        """Retrieves tasks belonging to a specific category.
        
        Args:
            category: The category name to filter tasks by
            
        Returns:
            List[Task]: A list of tasks that include the specified category
        """
        return [task for task in self.tasks if category in task.categories]

    def search_tasks(self, query: str) -> List[Task]:
        """Searches tasks by title or description.
        
        Args:
            query: The search string to match against task titles and descriptions
            
        Returns:
            List[Task]: A list of tasks where the query matches the title or description
        """
        query = query.lower()
        return [
            task for task in self.tasks
            if query in task.title.lower() or query in task.description.lower()
        ]

    def get_task_statistics(self) -> Dict:
        """Generates comprehensive statistics about the task list.
        
        Returns:
            Dict: A dictionary containing:
                - total_tasks: Total number of tasks
                - completed_tasks: Number of completed tasks
                - pending_tasks: Number of pending tasks
                - overdue_tasks: Number of overdue tasks
                - completion_rate: Percentage of completed tasks
                - priority_distribution: Count of tasks by priority level
        """
        total = len(self.tasks)
        completed = len(self.list_completed())
        pending = len(self.list_pending())
        overdue = len(self.list_overdue())
        
        priority_counts = {
            priority.name: len(self.get_tasks_by_priority(priority))
            for priority in Priority
        }
        
        return {
            "total_tasks": total,
            "completed_tasks": completed,
            "pending_tasks": pending,
            "overdue_tasks": overdue,
            "completion_rate": (completed / total * 100) if total > 0 else 0,
            "priority_distribution": priority_counts
        }

    def save_tasks(self) -> None:
        """Saves all tasks to a JSON file.
        
        The tasks are serialized to JSON format with proper handling of datetime
        objects and enums. The file is created if it doesn't exist.
        """
        tasks_data = []
        for task in self.tasks:
            task_dict = asdict(task)
            task_dict["created_at"] = task_dict["created_at"].isoformat()
            if task_dict["due_date"]:
                task_dict["due_date"] = task_dict["due_date"].isoformat()
            task_dict["priority"] = task_dict["priority"].name
            tasks_data.append(task_dict)
        
        with open(self.storage_path, 'w') as f:
            json.dump(tasks_data, f, indent=2)

    def load_tasks(self) -> None:
        """Loads tasks from a JSON file.
        
        Tasks are deserialized from JSON format with proper conversion of
        datetime strings and priority enums. If the file doesn't exist,
        no tasks are loaded.
        """
        if not self.storage_path.exists():
            return
            
        with open(self.storage_path, 'r') as f:
            tasks_data = json.load(f)
            
        for task_data in tasks_data:
            task_data["created_at"] = datetime.fromisoformat(task_data["created_at"])
            if task_data["due_date"]:
                task_data["due_date"] = datetime.fromisoformat(task_data["due_date"])
            task_data["priority"] = Priority[task_data["priority"]]
            self.tasks.append(Task(**task_data))

    def filter_keyword_task(self, kwrd):
        """Filters tasks that contain a keyword."""
        result = []
        for t in self.tasks:
            if not t.completed:
                if t.title.lower().find(kwrd.lower()) != -1:
                    result.append(t)
                else:
                    if t.description.lower().find(kwrd.lower()) != -1:
                        result.append(t)
        return result

    def get_matching_tasks_by_keyword(self, keyword):
        """Returns tasks matching keyword in title/description."""
        matches = []
        for x in self.tasks:
            if x.title.lower().find(keyword.lower()) >= 0 or x.description.lower().find(keyword.lower()) >= 0:
                matches.append(x)
        return matches

    def get_all_done_tasks_matching_keyword(self, keyword, include_completed):
        # this function will get matching keyword tasks
        result = []
        if include_completed:
            for i in self.tasks:
                if keyword.lower() in i.title.lower() or keyword.lower() in i.description.lower():
                    result.append(i)
        else:
            for i in self.tasks:
                if not i.completed:
                    if keyword.lower() in i.title.lower() or keyword.lower() in i.description.lower():
                        result.append(i)
        return result

    def get_task_statistics(self, level=1):
        """Generates statistics about the task list.
        
        Returns:
            Dict: A dictionary containing:
                - total_tasks
                - completed_tasks
                - pending_tasks
                - overdue_tasks
                - completion_rate
                - priority_distribution
        """
        total = len(self.tasks)
        completed = len(self.list_completed())
        pending = len(self.list_pending())
        overdue = len(self.list_overdue())
        
        # This should probably be factored out
        priority_counts = {
            priority.name: len([t for t in self.tasks if t.priority == priority])
            for priority in Priority
        }
        
        return {
            "total_tasks": total,
            "completed_tasks": completed,
            "pending_tasks": pending,
            "overdue_tasks": overdue,
            "completion_rate": (completed / total * 100) if total > 0 else 0,
            "priority_distribution": priority_counts
        }