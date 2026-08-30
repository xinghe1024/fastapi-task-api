import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

from config import Settings
from task_cache_dependencies import get_task_cache
from redis.asyncio import Redis

from models import TaskResponse
from task_cache import (
    TASK_NOT_FOUND_CACHE_MARKER,
    TaskCache,
    TaskCacheLookup,
    wait_for_task_cache_fill,
)


def create_task_response() -> TaskResponse:
    return TaskResponse(
        id=3,
        title="Learn Redis",
        description=None,
        priority=2,
        completed=False,
        due_date=date(2026, 8, 30),
    )


def test_task_cache_returns_none_on_cache_miss(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.get = AsyncMock(
        return_value=None,
    )

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
    )

    cached_task = asyncio.run(
        task_cache.get_task(
            owner_id=7,
            task_id=3,
        )
    )

    assert cached_task is None
    redis_client.get.assert_awaited_once_with(
        "task:7:3",
    )


def test_task_cache_stores_serialized_task(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=True,
    )

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
    )
    task = create_task_response()

    asyncio.run(
        task_cache.set_task(
            owner_id=7,
            task=task,
        )
    )

    redis_client.set.assert_awaited_once_with(
        "task:7:3",
        task.model_dump_json(),
        ex=300,
    )


def test_task_cache_restores_cached_task(
) -> None:
    task = create_task_response()

    redis_client = MagicMock(spec=Redis)
    redis_client.get = AsyncMock(
        return_value=task.model_dump_json(),
    )

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
    )

    cached_task = asyncio.run(
        task_cache.get_task(
            owner_id=7,
            task_id=3,
        )
    )

    assert cached_task == task
    assert isinstance(cached_task.due_date, date)


def test_task_cache_dependency_uses_configured_ttl(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=True,
    )

    settings = Settings(
        jwt_secret_key=(
            "test-secret-key-that-is-at-least-32-bytes"
        ),
        task_cache_ttl_seconds=120,
        task_cache_ttl_jitter_seconds=0,
    )

    task_cache = get_task_cache(
        redis_client=redis_client,
        settings=settings,
    )
    task = create_task_response()

    asyncio.run(
        task_cache.set_task(
            owner_id=7,
            task=task,
        )
    )

    redis_client.set.assert_awaited_once_with(
        "task:7:3",
        task.model_dump_json(),
        ex=120,
    )


def test_task_cache_deletes_invalid_cached_data(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.get = AsyncMock(
        return_value="not-valid-json",
    )
    redis_client.delete = AsyncMock(
        return_value=1,
    )

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
    )

    cached_task = asyncio.run(
        task_cache.get_task(
            owner_id=7,
            task_id=3,
        )
    )

    assert cached_task is None

    redis_client.get.assert_awaited_once_with(
        "task:7:3",
    )
    redis_client.delete.assert_awaited_once_with(
        "task:7:3",
    )

def test_task_cache_recognizes_negative_entry(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.get = AsyncMock(
        return_value=TASK_NOT_FOUND_CACHE_MARKER,
    )

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
    )

    lookup = asyncio.run(
        task_cache.lookup_task(
            owner_id=7,
            task_id=999,
        )
    )

    assert lookup.cache_hit is True
    assert lookup.task is None

def test_task_cache_stores_negative_entry(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=True,
    )

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
        negative_ttl_seconds=20,
    )

    asyncio.run(
        task_cache.set_task_not_found(
            owner_id=7,
            task_id=999,
        )
    )

    redis_client.set.assert_awaited_once_with(
        "task:7:999",
        TASK_NOT_FOUND_CACHE_MARKER,
        ex=20,
    )

def test_task_cache_adds_jitter_to_positive_ttl(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=True,
    )

    def fixed_jitter(
        minimum: int,
        maximum: int,
    ) -> int:
        assert minimum == 0
        assert maximum == 60
        return 17

    task_cache = TaskCache(
        redis_client=redis_client,
        ttl_seconds=300,
        negative_ttl_seconds=30,
        ttl_jitter_seconds=60,
        jitter_generator=fixed_jitter,
    )
    task = create_task_response()

    asyncio.run(
        task_cache.set_task(
            owner_id=7,
            task=task,
        )
    )

    redis_client.set.assert_awaited_once_with(
        "task:7:3",
        task.model_dump_json(),
        ex=317,
    )


def test_wait_for_task_cache_fill_returns_when_cache_is_filled(
) -> None:
    task = create_task_response()

    task_cache = MagicMock(spec=TaskCache)
    task_cache.lookup_task = AsyncMock(
        side_effect=[
            TaskCacheLookup(
                cache_hit=False,
                task=None,
            ),
            TaskCacheLookup(
                cache_hit=True,
                task=task,
            ),
        ],
    )
    sleeper = AsyncMock()

    cache_lookup = asyncio.run(
        wait_for_task_cache_fill(
            task_cache=task_cache,
            owner_id=7,
            task_id=3,
            attempts=5,
            delay_seconds=0.05,
            sleeper=sleeper,
        )
    )

    assert cache_lookup.cache_hit is True
    assert cache_lookup.task == task
    assert sleeper.await_count == 2
    assert task_cache.lookup_task.await_count == 2


def test_wait_for_task_cache_fill_stops_after_attempt_limit(
) -> None:
    task_cache = MagicMock(spec=TaskCache)
    task_cache.lookup_task = AsyncMock(
        return_value=TaskCacheLookup(
            cache_hit=False,
            task=None,
        ),
    )
    sleeper = AsyncMock()

    cache_lookup = asyncio.run(
        wait_for_task_cache_fill(
            task_cache=task_cache,
            owner_id=7,
            task_id=3,
            attempts=3,
            delay_seconds=0.05,
            sleeper=sleeper,
        )
    )

    assert cache_lookup.cache_hit is False
    assert cache_lookup.task is None
    assert sleeper.await_count == 3
    assert task_cache.lookup_task.await_count == 3