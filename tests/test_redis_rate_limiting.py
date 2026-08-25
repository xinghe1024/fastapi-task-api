import asyncio
from unittest.mock import (
    ANY,
    AsyncMock,
    MagicMock,
)

from redis.asyncio import Redis

from redis_rate_limiting import (
    RedisFixedWindowRateLimiter,
)


def test_redis_rate_limiter_allows_request_within_limit(
) -> None:
    redis_client = MagicMock(spec=Redis)
    eval_mock = AsyncMock(
        return_value=[
            3,
            45,
        ],
    )
    redis_client.eval = eval_mock

    rate_limiter = RedisFixedWindowRateLimiter(
        redis_client=redis_client,
        request_limit=3,
        window_seconds=60,
        key_prefix="rate_limit:login",
    )

    retry_after_seconds = asyncio.run(
        rate_limiter.check("test-user"),
    )

    assert retry_after_seconds is None

    eval_mock.assert_awaited_once_with(
        ANY,
        1,
        "rate_limit:login:test-user",
        60,
    )

def test_redis_rate_limiter_returns_ttl_when_exceeded(
) -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.eval = AsyncMock(
        return_value=[
            4,
            42,
        ],
    )

    rate_limiter = RedisFixedWindowRateLimiter(
        redis_client=redis_client,
        request_limit=3,
        window_seconds=60,
        key_prefix="rate_limit:login",
    )

    retry_after_seconds = asyncio.run(
        rate_limiter.check("test-user"),
    )

    assert retry_after_seconds == 42