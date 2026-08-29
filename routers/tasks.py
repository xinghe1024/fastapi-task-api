from typing import Annotated

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    status,
)

from authentication import get_current_user
from models import TaskCreate, TaskResponse, TaskUpdate
from audit_log import log_task_created

from sqlalchemy.orm import Session
from sqlalchemy import select

from database_models import TaskRecord, UserRecord
from dependencies import (
    get_pagination,
    get_session,
)
from task_dependencies import (
    get_owned_task_or_404,
)
from task_cache import TaskCache
from task_cache_dependencies import (
    get_cached_owned_task_or_404,
    get_task_cache,
    invalidate_cached_task,
)

router = APIRouter(
    prefix="/tasks",
    tags=["tasks"],
    dependencies=[
        Depends(get_current_user),
    ],
)

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
)
def create_task(
    task_create: TaskCreate,
    background_tasks: BackgroundTasks,
    current_user: Annotated[
        UserRecord,
        Depends(get_current_user),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
    task_cache: Annotated[
        TaskCache,
        Depends(get_task_cache),
    ],
) -> TaskResponse:
    task_record = TaskRecord(
        **task_create.model_dump(),
        completed=False,
        owner_id=current_user.id,
    )

    session.add(task_record)
    session.commit()
    session.refresh(task_record)
    invalidate_cached_task(
        task_cache=task_cache,
        owner_id=current_user.id,
        task_id=task_record.id,
    )
    background_tasks.add_task(
        log_task_created,
        task_record.id,
        current_user.id,
    )

    return TaskResponse.model_validate(task_record)


@router.get(
    "",
    status_code=status.HTTP_200_OK,
)
def get_tasks(
    pagination: Annotated[
        dict[str, int],
        Depends(get_pagination),
    ],
    current_user: Annotated[
        UserRecord,
        Depends(get_current_user),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
) -> list[TaskResponse]:
    statement = (
        select(TaskRecord)
        .where(
            TaskRecord.owner_id == current_user.id,
        )
        .order_by(TaskRecord.id)
        .offset(pagination["skip"])
        .limit(pagination["limit"])
    )

    task_records = session.scalars(statement).all()

    return [
        TaskResponse.model_validate(task_record)
        for task_record in task_records
    ]


@router.get(
    "/{task_id}",
    status_code=status.HTTP_200_OK,
)
def get_task(
    cached_task: Annotated[
        TaskResponse,
        Depends(get_cached_owned_task_or_404),
    ],
) -> TaskResponse:
    return cached_task


@router.patch(
    "/{task_id}",
    status_code=status.HTTP_200_OK,
)
def update_task(
    task_update: TaskUpdate,
    stored_task: Annotated[
        TaskRecord,
        Depends(get_owned_task_or_404),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
    task_cache: Annotated[
        TaskCache,
        Depends(get_task_cache),
    ],
) -> TaskResponse:
    owner_id = stored_task.owner_id
    task_id = stored_task.id

    update_data = task_update.model_dump(
        exclude_unset=True,
    )

    for field_name, field_value in update_data.items():
        setattr(
            stored_task,
            field_name,
            field_value,
        )

    session.commit()

    invalidate_cached_task(
        task_cache=task_cache,
        owner_id=owner_id,
        task_id=task_id,
    )

    session.refresh(stored_task)

    return TaskResponse.model_validate(
        stored_task,
    )


@router.delete(
    "/{task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_task(
    stored_task: Annotated[
        TaskRecord,
        Depends(get_owned_task_or_404),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
    task_cache: Annotated[
        TaskCache,
        Depends(get_task_cache),
    ],
) -> None:
    owner_id = stored_task.owner_id
    task_id = stored_task.id

    session.delete(stored_task)
    session.commit()

    invalidate_cached_task(
        task_cache=task_cache,
        owner_id=owner_id,
        task_id=task_id,
    )