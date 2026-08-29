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
    task_record = find_owned_task(
        session=session,
        task_id=task_id,
        owner_id=current_user.id,
    )

    if task_record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    return task_record

def find_owned_task(
    session: Session,
    task_id: int,
    owner_id: int,
) -> TaskRecord | None:
    statement = select(TaskRecord).where(
        TaskRecord.id == task_id,
        TaskRecord.owner_id == owner_id,
    )

    return session.scalar(statement)