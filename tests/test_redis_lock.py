import asyncio
from unittest.mock import (
    ANY,
    AsyncMock,
    MagicMock,
)

from config import Settings

from task_cache_dependencies import (
    get_task_cache_lock,
)

from redis.asyncio import Redis

from redis_lock import RedisLock


def test_redis_lock_returns_token_when_acquired(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=True,
    )

    redis_lock = RedisLock(
        redis_client=redis_client,
        ttl_seconds=5,
        token_generator=lambda: "test-token",
    )

    lock_token = asyncio.run(
        redis_lock.acquire("lock:task:7:3")
    )

    assert lock_token == "test-token"
    redis_client.set.assert_awaited_once_with(
        "lock:task:7:3",
        "test-token",
        nx=True,
        ex=5,
    )


def test_redis_lock_returns_none_when_already_locked(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=None,
    )

    redis_lock = RedisLock(
        redis_client=redis_client,
        ttl_seconds=5,
        token_generator=lambda: "test-token",
    )

    lock_token = asyncio.run(
        redis_lock.acquire("lock:task:7:3")
    )

    assert lock_token is None


def test_redis_lock_releases_only_matching_token(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.eval = AsyncMock(
        return_value=1,
    )

    redis_lock = RedisLock(
        redis_client=redis_client,
        ttl_seconds=5,
    )

    released = asyncio.run(
        redis_lock.release(
            lock_key="lock:task:7:3",
            lock_token="owner-token",
        )
    )

    assert released is True

    redis_client.eval.assert_awaited_once_with(
        ANY,
        1,
        "lock:task:7:3",
        "owner-token",
    )

def test_task_cache_lock_dependency_uses_configured_ttl(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.set = AsyncMock(
        return_value=True,
    )

    settings = Settings(
        jwt_secret_key=(
            "test-secret-key-that-is-at-least-32-bytes"
        ),
        task_cache_lock_ttl_seconds=7,
    )

    redis_lock = get_task_cache_lock(
        redis_client=redis_client,
        settings=settings,
    )

    lock_token = asyncio.run(
        redis_lock.acquire(
            "lock:task:7:3",
        )
    )

    assert isinstance(lock_token, str)
    assert lock_token

    redis_client.set.assert_awaited_once_with(
        "lock:task:7:3",
        ANY,
        nx=True,
        ex=7,
    )