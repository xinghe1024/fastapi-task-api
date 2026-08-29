import logging


from typing import Annotated

from config import Settings, get_settings
from redis_dependencies import get_redis_client
from task_cache import TaskCache

from anyio import from_thread
from fastapi import Depends, HTTPException, status
from redis.exceptions import RedisError
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from authentication import get_current_user
from database_models import UserRecord
from dependencies import get_session
from models import TaskResponse
from task_dependencies import find_owned_task

logger = logging.getLogger(__name__)

def get_task_cache(
    redis_client: Annotated[
        Redis,
        Depends(get_redis_client),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> TaskCache:
    return TaskCache(
        redis_client=redis_client,
        ttl_seconds=(
            settings.task_cache_ttl_seconds
        ),
        negative_ttl_seconds=(
            settings.task_negative_cache_ttl_seconds
        ),
        ttl_jitter_seconds=(
            settings.task_cache_ttl_jitter_seconds
        ),
    )

def get_cached_owned_task_or_404(
    task_id: int,
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
    cache_lookup = None

    try:
        cache_lookup = from_thread.run(
            task_cache.lookup_task,
            current_user.id,
            task_id,
        )
    except RedisError:
        logger.warning(
            "Redis task cache unavailable; "
            "querying database",
        )

    if (
        cache_lookup is not None
        and cache_lookup.cache_hit
    ):
        if cache_lookup.task is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )

        return cache_lookup.task

    task_record = find_owned_task(
        session=session,
        task_id=task_id,
        owner_id=current_user.id,
    )

    if task_record is None:
        try:
            from_thread.run(
                task_cache.set_task_not_found,
                current_user.id,
                task_id,
            )
        except RedisError:
            logger.warning(
                "Redis task cache unavailable; "
                "negative result was not cached",
            )

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    task_response = TaskResponse.model_validate(
        task_record,
    )

    try:
        from_thread.run(
            task_cache.set_task,
            current_user.id,
            task_response,
        )
    except RedisError:
        logger.warning(
            "Redis task cache unavailable; "
            "response was not cached",
        )

    return task_response


def invalidate_cached_task(
    task_cache: TaskCache,
    owner_id: int,
    task_id: int,
) -> None:
    try:
        from_thread.run(
            task_cache.delete_task,
            owner_id,
            task_id,
        )
    except RedisError:
        logger.warning(
            "Redis task cache unavailable; "
            "cached task was not invalidated",
        )