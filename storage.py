from typing import Any

tasks: list[dict[str, Any]] = []

def find_task(task_id: int) -> dict[str, Any] | None:
    return next(
        (task for task in tasks if task['id'] == task_id),
        None
    )


def generate_task_id() -> int:
    return max(
        (task['id'] for task in tasks),
        default=0,
    ) + 1



