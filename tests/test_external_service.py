import asyncio
import pytest

import httpx

from circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)
from external_service import fetch_external_status


def test_fetch_external_status_returns_200() -> None:
    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        assert str(request.url) == (
            "https://upstream.test/health"
        )

        return httpx.Response(
            status_code=200,
            request=request,
        )

    async def run_test() -> int:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            return await fetch_external_status(
                http_client=http_client,
                url="https://upstream.test/health",
            )

    status_code = asyncio.run(run_test())

    assert status_code == 200

def test_fetch_external_status_raises_for_error_response() -> None:
    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        assert str(request.url) == (
            "https://upstream.test/health"
        )

        return httpx.Response(
            status_code=503,
            request=request,
        )

    async def run_test() -> int:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            return await fetch_external_status(
                http_client=http_client,
                url="https://upstream.test/health",
                max_attempts=1,
            )

    with pytest.raises(
            httpx.HTTPStatusError,
    ) as exception_info:
        asyncio.run(run_test())

    assert (
            exception_info.value.response.status_code
            == 503
    )


def test_fetch_external_status_raises_for_timeout() -> None:
    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        assert str(request.url) == (
            "https://upstream.test/health"
        )

        raise httpx.ReadTimeout(
            "Upstream service timed out",
            request=request,
        )

    async def run_test() -> int:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            return await fetch_external_status(
                http_client=http_client,
                url="https://upstream.test/health",
                max_attempts=1,
            )

    with pytest.raises(
            httpx.ReadTimeout,
    ) as exception_info:
        asyncio.run(run_test())

    assert str(exception_info.value) == (
        "Upstream service timed out"
    )

    assert str(exception_info.value.request.url) == (
        "https://upstream.test/health"
    )

def test_fetch_external_status_retries_until_success(
) -> None:
    attempt_count = 0

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        status_code = (
            503
            if attempt_count < 3
            else 200
        )

        return httpx.Response(
            status_code=status_code,
            request=request,
        )

    async def run_test() -> int:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            return await fetch_external_status(
                http_client=http_client,
                url=(
                    "https://upstream.test/health"
                ),
                max_attempts=3,
                base_delay=0.0,
            )

    result = asyncio.run(run_test())

    assert result == 200
    assert attempt_count == 3


def test_failed_retry_operation_records_one_circuit_failure(
) -> None:
    attempt_count = 0
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
    )

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        return httpx.Response(
            status_code=503,
            request=request,
        )

    async def run_test() -> None:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            with pytest.raises(httpx.HTTPStatusError):
                await fetch_external_status(
                    http_client=http_client,
                    url="https://upstream.test/health",
                    max_attempts=3,
                    base_delay=0.0,
                    circuit_breaker=circuit_breaker,
                )

    asyncio.run(run_test())

    assert attempt_count == 3
    assert circuit_breaker.failure_count == 1
    assert circuit_breaker.state is CircuitState.CLOSED


def test_successful_operation_resets_circuit_failures(
) -> None:
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
    )
    circuit_breaker.record_failure()

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            request=request,
        )

    async def run_test() -> int:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            return await fetch_external_status(
                http_client=http_client,
                url="https://upstream.test/health",
                circuit_breaker=circuit_breaker,
            )

    status_code = asyncio.run(run_test())

    assert status_code == 200
    assert circuit_breaker.failure_count == 0
    assert circuit_breaker.state is CircuitState.CLOSED


def test_open_circuit_blocks_external_request(
) -> None:
    request_count = 0
    circuit_breaker = CircuitBreaker(
        failure_threshold=1,
    )
    circuit_breaker.record_failure()

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            status_code=200,
            request=request,
        )

    async def run_test() -> int:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            return await fetch_external_status(
                http_client=http_client,
                url="https://upstream.test/health",
                circuit_breaker=circuit_breaker,
            )

    with pytest.raises(CircuitBreakerOpenError):
        asyncio.run(run_test())

    assert request_count == 0