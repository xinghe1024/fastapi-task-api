import logging

logger = logging.getLogger(__name__)

def log_task_created(
        task_id:int,
        owner_id:int,
) -> None:
    logger.info(
        "Task created:task_id=%s, owner_id=%s",
        task_id,
        owner_id,
    )

