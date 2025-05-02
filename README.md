# Todo's

A simple, extensible command-line Todo List manager in Python.

## Features

- **Task Management:** Add, list, and mark tasks as completed.
- **Prioritization:** Assign priorities (Low, High, Urgent) to tasks.
- **Due Dates:** Set due dates for tasks and track overdue items.
- **Categories:** Organize tasks by custom categories (e.g., Work, Shopping).
- **Search:** Search tasks by keywords.
- **Statistics:** View statistics such as completion rate, overdue tasks, and priority distribution.
- **Persistence:** Tasks are saved to a JSON file for data persistence.

## Getting Started

### Prerequisites

- Python 3.7 or higher

### Installation

Clone the repository:

```bash
git clone <repository-url>
cd code-review
```

Install any required dependencies (if any):

```bash
pip install -r requirements.txt
```

### Usage

Run the main script:

```bash
python main.py
```

This will:

- Create example tasks
- Display all tasks, pending tasks, completed tasks
- Search for tasks by keyword
- Filter tasks by category and priority
- Show task statistics

You can modify `main.py` or use the `TodoList` and `Task` classes in your own scripts.

## Project Structure
