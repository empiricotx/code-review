# main.py

from todo_manager import Task, TodoList, Priority
from datetime import datetime, timedelta


def main():
    # Initialize todo list with persistence
    todo = TodoList("my_tasks.json")

    # Create some example tasks with different features
    grocery_task = Task(
        title="Buy groceries",
        description="Milk, eggs, bread, and fruits",
        priority=Priority.HIGH,
        due_date=datetime.now() + timedelta(days=1)
    )
    grocery_task.add_category("Shopping")
    grocery_task.add_category("Urgent")

    work_task = Task(
        title="Complete project report",
        description="Finish the quarterly project report",
        priority=Priority.URGENT,
        due_date=datetime.now() + timedelta(days=2)
    )
    work_task.add_category("Work")
    work_task.add_category("Important")

    reading_task = Task(
        title="Read new book",
        description="Start reading 'Clean Code'",
        priority=Priority.LOW,
        due_date=datetime.now() + timedelta(days=7)
    )
    reading_task.add_category("Personal")
    reading_task.add_category("Learning")

    # Add tasks to the list
    todo.add_task(grocery_task)
    todo.add_task(work_task)
    todo.add_task(reading_task)

    # Display all tasks
    print("\n=== All Tasks ===")
    for task in todo.tasks:
        print(f"- {task.title} (Priority: {task.priority.name})")
        print(f"  Due: {task.due_date.strftime('%Y-%m-%d') if task.due_date else 'No due date'}")
        print(f"  Categories: {', '.join(task.categories)}")
        print(f"  Status: {'Completed' if task.completed else 'Pending'}")

    # Mark a task as done
    grocery_task.mark_done()

    # Display pending tasks
    print("\n=== Pending Tasks ===")
    for task in todo.list_pending():
        print(f"- {task.title}")

    # Display completed tasks
    print("\n=== Completed Tasks ===")
    for task in todo.list_completed():
        print(f"- {task.title}")

    # Search for tasks
    print("\n=== Search Results for 'book' ===")
    for task in todo.search_tasks("book"):
        print(f"- {task.title}")

    # Get tasks by category
    print("\n=== Work Category Tasks ===")
    for task in todo.get_tasks_by_category("Work"):
        print(f"- {task.title}")

    # Get tasks by priority
    print("\n=== High Priority Tasks ===")
    for task in todo.get_tasks_by_priority(Priority.HIGH):
        print(f"- {task.title}")

    # Display statistics
    print("\n=== Task Statistics ===")
    stats = todo.get_task_statistics()
    print(f"Total Tasks: {stats['total_tasks']}")
    print(f"Completed Tasks: {stats['completed_tasks']}")
    print(f"Pending Tasks: {stats['pending_tasks']}")
    print(f"Overdue Tasks: {stats['overdue_tasks']}")
    print(f"Completion Rate: {stats['completion_rate']:.1f}%")
    print("\nPriority Distribution:")
    for priority, count in stats['priority_distribution'].items():
        print(f"- {priority}: {count} tasks")


if __name__ == "__main__":
    main()
