import logging


from typing import Annotated

from config import Settings, get_settings

from task_cache import (
    TaskCache,
    TaskCacheLookup,
    wait_for_task_cache_fill,
)

from anyio import from_thread

from fastapi import Depends, HTTPException, status

from redis_lock import RedisLock
from redis_dependencies import get_redis_client
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

def build_task_cache_lock_key(
    owner_id: int,
    task_id: int,
) -> str:
    return f"lock:task:{owner_id}:{task_id}"

def resolve_cached_task(
    cache_lookup: TaskCacheLookup,
) -> TaskResponse:
    if cache_lookup.task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    return cache_lookup.task

def load_owned_task_from_database(
    session: Session,
    task_cache: TaskCache,
    owner_id: int,
    task_id: int,
) -> TaskResponse:
    task_record = find_owned_task(
        session=session,
        task_id=task_id,
        owner_id=owner_id,
    )

    if task_record is None:
        try:
            from_thread.run(
                task_cache.set_task_not_found,
                owner_id,
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
            owner_id,
            task_response,
        )
    except RedisError:
        logger.warning(
            "Redis task cache unavailable; "
            "response was not cached",
        )

    return task_response

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
    cache_lock: Annotated[
        RedisLock,
        Depends(get_task_cache_lock),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> TaskResponse:
    try:
        initial_lookup = from_thread.run(
            task_cache.lookup_task,
            current_user.id,
            task_id,
        )
    except RedisError:
        logger.warning(
            "Redis task cache unavailable; "
            "querying database",
        )

        return load_owned_task_from_database(
            session=session,
            task_cache=task_cache,
            owner_id=current_user.id,
            task_id=task_id,
        )

    if initial_lookup.cache_hit:
        return resolve_cached_task(initial_lookup)

    lock_key = build_task_cache_lock_key(
        owner_id=current_user.id,
        task_id=task_id,
    )

    try:
        lock_token = from_thread.run(
            cache_lock.acquire,
            lock_key,
        )
    except RedisError:
        logger.warning(
            "Redis cache lock unavailable; "
            "querying database",
        )

        return load_owned_task_from_database(
            session=session,
            task_cache=task_cache,
            owner_id=current_user.id,
            task_id=task_id,
        )

    if lock_token is None:
        try:
            waited_lookup = from_thread.run(
                wait_for_task_cache_fill,
                task_cache,
                current_user.id,
                task_id,
                settings.task_cache_lock_wait_attempts,
                settings.task_cache_lock_wait_seconds,
            )
        except RedisError:
            logger.warning(
                "Redis task cache unavailable "
                "while waiting; querying database",
            )

            return load_owned_task_from_database(
                session=session,
                task_cache=task_cache,
                owner_id=current_user.id,
                task_id=task_id,
            )

        if waited_lookup.cache_hit:
            return resolve_cached_task(
                waited_lookup,
            )

        return load_owned_task_from_database(
            session=session,
            task_cache=task_cache,
            owner_id=current_user.id,
            task_id=task_id,
        )

    try:
        second_lookup = from_thread.run(
            task_cache.lookup_task,
            current_user.id,
            task_id,
        )

        if second_lookup.cache_hit:
            return resolve_cached_task(
                second_lookup,
            )

        return load_owned_task_from_database(
            session=session,
            task_cache=task_cache,
            owner_id=current_user.id,
            task_id=task_id,
        )
    finally:
        try:
            from_thread.run(
                cache_lock.release,
                lock_key,
                lock_token,
            )
        except RedisError:
            logger.warning(
                "Redis cache lock could not be released",
            )


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

def get_task_cache_lock(
    redis_client: Annotated[
        Redis,
        Depends(get_redis_client),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> RedisLock:
    return RedisLock(
        redis_client=redis_client,
        ttl_seconds=(
            settings.task_cache_lock_ttl_seconds
        ),
    )