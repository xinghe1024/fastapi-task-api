from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from authentication import get_current_user
from database_models import TaskRecord, UserRecord
from dependencies import get_session


def get_owned_task_or_404(
    task_id: int,
    current_user: Annotated[
        UserRecord,
        Depends(get_current_user),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
) -> TaskRecord:
    statement = select(TaskRecord).where(
        TaskRecord.id == task_id,
        TaskRecord.owner_id == current_user.id,
    )
    task_record = session.scalar(statement)

    if task_record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    return task_record