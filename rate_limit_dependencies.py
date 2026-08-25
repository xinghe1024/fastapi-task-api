from typing import Annotated

from fastapi import (
    Depends,
    HTTPException,
    Request,
    status,
)
from redis.asyncio import Redis

from config import Settings, get_settings
from redis_dependencies import get_redis_client
from redis_rate_limiting import (
    RedisFixedWindowRateLimiter,
)


def get_login_rate_limiter(
    redis_client: Annotated[
        Redis,
        Depends(get_redis_client),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> RedisFixedWindowRateLimiter:
    return RedisFixedWindowRateLimiter(
        redis_client=redis_client,
        request_limit=settings.login_rate_limit,
        window_seconds=(
            settings.login_rate_window_seconds
        ),
        key_prefix="rate_limit:login",
    )


async def enforce_login_rate_limit(
    request: Request,
    rate_limiter: Annotated[
        RedisFixedWindowRateLimiter,
        Depends(get_login_rate_limiter),
    ],
) -> None:
    client_host = (
        request.client.host
        if request.client is not None
        else "unknown"
    )

    retry_after_seconds = await rate_limiter.check(
        client_host,
    )

    if retry_after_seconds is None:
        return

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many login attempts",
        headers={
            "Retry-After": str(retry_after_seconds),
        },
    )