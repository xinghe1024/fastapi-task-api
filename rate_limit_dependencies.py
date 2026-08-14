from typing import Annotated

from fastapi import (
    Depends,
    HTTPException,
    Request,
    status,
)

from rate_limiting import FixedWindowRateLimiter


_login_rate_limiter = FixedWindowRateLimiter(
    request_limit=5,
    window_seconds=60,
)


def get_login_rate_limiter(
) -> FixedWindowRateLimiter:
    return _login_rate_limiter


def enforce_login_rate_limit(
    request: Request,
    rate_limiter: Annotated[
        FixedWindowRateLimiter,
        Depends(get_login_rate_limiter),
    ],
) -> None:
    client_host = (
        request.client.host
        if request.client is not None
        else "unknown"
    )

    retry_after_seconds = rate_limiter.check(
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