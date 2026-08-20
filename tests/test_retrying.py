import asyncio

import httpx

import pytest

from retrying import retry_http_operation

def disable_jitter(
    maximum_delay: float,
) -> float:
    return maximum_delay

def test_retry_http_operation_succeeds_after_retries(
) -> None:
    attempt_count = 0
    recorded_delays: list[float] = []

    async def operation() -> int:
        nonlocal attempt_count

        attempt_count += 1

        if attempt_count < 3:
            request = httpx.Request(
                "GET",
                "https://upstream.test/health",
            )

            raise httpx.ReadTimeout(
                "Upstream service timed out",
                request=request,
            )

        return 200

    async def fake_sleep(
        delay: float,
    ) -> None:
        recorded_delays.append(delay)

    result = asyncio.run(
        retry_http_operation(
            operation=operation,
            max_attempts=3,
            base_delay=0.5,
            jitter_function=disable_jitter,
            sleep_function=fake_sleep,
        )
    )

    assert result == 200
    assert attempt_count == 3
    assert recorded_delays == [
        0.5,
        1.0,
    ]

def test_retry_http_operation_raises_after_max_attempts(
) -> None:
    attempt_count = 0
    recorded_delays: list[float] = []

    async def operation() -> int:
        nonlocal attempt_count
        attempt_count += 1

        request = httpx.Request(
            "GET",
            "https://upstream.test/health",
        )

        raise httpx.ReadTimeout(
            "Upstream service timed out",
            request=request,
        )

    async def fake_sleep(
        delay: float,
    ) -> None:
        recorded_delays.append(delay)

    with pytest.raises(
        httpx.ReadTimeout,
    ) as exception_info:
        asyncio.run(
            retry_http_operation(
                operation=operation,
                max_attempts=3,
                base_delay=0.5,
                jitter_function=disable_jitter,
                sleep_function=fake_sleep,
            )
        )

    assert str(exception_info.value) == (
        "Upstream service timed out"
    )
    assert attempt_count == 3
    assert recorded_delays == [
        0.5,
        1.0,
    ]

def test_retry_http_operation_does_not_retry_400(
) -> None:
    attempt_count = 0
    recorded_delays: list[float] = []

    async def operation() -> int:
        nonlocal attempt_count
        attempt_count += 1

        request = httpx.Request(
            "GET",
            "https://upstream.test/health",
        )
        response = httpx.Response(
            status_code=400,
            request=request,
        )

        response.raise_for_status()

        return response.status_code

    async def fake_sleep(
        delay: float,
    ) -> None:
        recorded_delays.append(delay)

    with pytest.raises(
        httpx.HTTPStatusError,
    ) as exception_info:
        asyncio.run(
            retry_http_operation(
                operation=operation,
                max_attempts=3,
                base_delay=0.5,
                jitter_function=disable_jitter,
                sleep_function=fake_sleep,
            )
        )

    assert (
        exception_info.value.response.status_code
        == 400
    )
    assert attempt_count == 1
    assert recorded_delays == []

def test_retry_http_operation_retries_503_until_success(
) -> None:
    attempt_count = 0
    recorded_delays: list[float] = []

    async def operation() -> int:
        nonlocal attempt_count
        attempt_count += 1

        request = httpx.Request(
            "GET",
            "https://upstream.test/health",
        )

        status_code = (
            503
            if attempt_count < 3
            else 200
        )

        response = httpx.Response(
            status_code=status_code,
            request=request,
        )
        response.raise_for_status()

        return response.status_code

    async def fake_sleep(
        delay: float,
    ) -> None:
        recorded_delays.append(delay)

    result = asyncio.run(
        retry_http_operation(
            operation=operation,
            max_attempts=3,
            base_delay=0.5,
            jitter_function=disable_jitter,
            sleep_function=fake_sleep,
        )
    )

    assert result == 200
    assert attempt_count == 3
    assert recorded_delays == [
        0.5,
        1.0,
    ]

def test_retry_http_operation_applies_jitter(
) -> None:
    attempt_count = 0
    jitter_inputs: list[float] = []
    recorded_delays: list[float] = []

    async def operation() -> int:
        nonlocal attempt_count
        attempt_count += 1

        if attempt_count < 3:
            request = httpx.Request(
                "GET",
                "https://upstream.test/health",
            )

            raise httpx.ReadTimeout(
                "Upstream service timed out",
                request=request,
            )

        return 200

    def half_jitter(
        maximum_delay: float,
    ) -> float:
        jitter_inputs.append(
            maximum_delay,
        )

        return maximum_delay / 2

    async def fake_sleep(
        delay: float,
    ) -> None:
        recorded_delays.append(delay)

    result = asyncio.run(
        retry_http_operation(
            operation=operation,
            max_attempts=3,
            base_delay=0.5,
            jitter_function=half_jitter,
            sleep_function=fake_sleep,
        )
    )

    assert result == 200

    assert jitter_inputs == [
        0.5,
        1.0,
    ]

    assert recorded_delays == [
        0.25,
        0.5,
    ]