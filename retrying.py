import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx

import random

ResultType = TypeVar("ResultType")

RETRYABLE_STATUS_CODES = frozenset({
    502,
    503,
    504,
})

def _is_retryable_http_error(
    error: httpx.HTTPError,
) -> bool:
    if isinstance(
        error,
        (
            httpx.TimeoutException,
            httpx.ConnectError,
        ),
    ):
        return True

    if isinstance(
        error,
        httpx.HTTPStatusError,
    ):
        return (
            error.response.status_code
            in RETRYABLE_STATUS_CODES
        )

    return False


def _full_jitter(
    maximum_delay: float,
) -> float:
    return random.uniform(
        0.0,
        maximum_delay,
    )


async def retry_http_operation(
    operation: Callable[
        [],
        Awaitable[ResultType],
    ],
    *,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
    jitter_function: Callable[
        [float],
        float,
    ] = _full_jitter,
    sleep_function: Callable[
        [float],
        Awaitable[None],
    ] = asyncio.sleep,
) -> ResultType:
    if max_attempts < 1:
        raise ValueError(
            "max_attempts must be at least 1",
        )

    if base_delay < 0:
        raise ValueError(
            "base_delay cannot be negative",
        )

    for attempt_index in range(max_attempts):
        try:
            return await operation()
        except httpx.HTTPError as error:
            if not _is_retryable_http_error(error):
                raise

            is_last_attempt = (
                    attempt_index == max_attempts - 1
            )

            if is_last_attempt:
                raise

            if max_delay < 0:
                raise ValueError(
                    "max_delay cannot be negative",
                )

            exponential_delay = (
                    base_delay
                    * 2 ** attempt_index
            )

            capped_delay = min(
                exponential_delay,
                max_delay,
            )

            delay = jitter_function(
                capped_delay,
            )

            await sleep_function(delay)

    raise RuntimeError(
        "Retry loop ended unexpectedly",
    )


