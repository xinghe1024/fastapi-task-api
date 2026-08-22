import asyncio
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
)

import httpx
from fastapi.testclient import TestClient
from config import Settings, get_settings
from circuit_breaker import (
    CircuitBreaker,
    CircuitState,
)
from circuit_breaker_dependencies import (
    get_external_service_circuit_breaker,
)
from http_client_dependencies import get_http_client

class ManualClock:
    def __init__(self) -> None:
        self.current_time = 0.0

    def __call__(self) -> float:
        return self.current_time

    def advance(self, seconds: float) -> None:
        self.current_time += seconds

ExternalHandler = Callable[
    [httpx.Request],
    httpx.Response | Awaitable[httpx.Response],
]


def request_external_status(
    client: TestClient,
    handle_request: ExternalHandler,
) -> httpx.Response:
    async def override_get_http_client(
    ) -> AsyncGenerator[
        httpx.AsyncClient,
        None,
    ]:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            yield http_client

    client.app.dependency_overrides[
        get_http_client
    ] = override_get_http_client

    try:
        return client.get(
            "/external/status",
        )
    finally:
        client.app.dependency_overrides.pop(
            get_http_client,
            None,
        )

def test_external_status_returns_available(
    client: TestClient,
) -> None:
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

    async def override_get_http_client(
    ) -> AsyncGenerator[
        httpx.AsyncClient,
        None,
    ]:
        transport = httpx.MockTransport(
            handle_request,
        )

        async with httpx.AsyncClient(
            transport=transport,
        ) as http_client:
            yield http_client

    client.app.dependency_overrides[
        get_http_client
    ] = override_get_http_client

    try:
        response = client.get(
            "/external/status",
        )
    finally:
        client.app.dependency_overrides.pop(
            get_http_client,
            None,
        )

    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
    }

def test_external_status_returns_502_for_upstream_error(
    client: TestClient,
) -> None:
    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        return httpx.Response(
            status_code=503,
            request=request,
        )

    response = request_external_status(
        client=client,
        handle_request=handle_request,
    )

    assert response.status_code == 502
    assert response.json() == {
        "detail": "Upstream service unavailable",
    }

def test_external_status_returns_504_for_timeout(
    client: TestClient,
) -> None:
    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        raise httpx.ReadTimeout(
            "Upstream service timed out",
            request=request,
        )

    response = request_external_status(
        client=client,
        handle_request=handle_request,
    )

    assert response.status_code == 504
    assert response.json() == {
        "detail": "Upstream service timed out",
    }


def test_external_status_returns_504_when_retry_budget_is_exhausted(
    client: TestClient,
) -> None:
    attempt_count = 0

    async def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1

        # 模拟耗时较长的上游请求
        await asyncio.sleep(1.0)

        return httpx.Response(
            status_code=200,
            request=request,
        )

    original_settings_override = (
        client.app.dependency_overrides[get_settings]
    )
    original_settings = original_settings_override()

    budget_settings = original_settings.model_copy(
        update={
            "external_service_max_attempts": 5,
            "external_service_total_timeout": 0.01,
        },
    )

    def override_budget_settings() -> Settings:
        return budget_settings

    client.app.dependency_overrides[
        get_settings
    ] = override_budget_settings

    try:
        response = request_external_status(
            client=client,
            handle_request=handle_request,
        )
    finally:
        client.app.dependency_overrides[
            get_settings
        ] = original_settings_override

    assert response.status_code == 504
    assert response.json() == {
        "detail": "Upstream service timed out",
    }
    assert attempt_count == 1

def test_external_status_returns_503_when_circuit_is_open(
    client: TestClient,
) -> None:
    request_count = 0

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            status_code=503,
            request=request,
        )

    first_response = request_external_status(
        client=client,
        handle_request=handle_request,
    )
    second_response = request_external_status(
        client=client,
        handle_request=handle_request,
    )
    third_response = request_external_status(
        client=client,
        handle_request=handle_request,
    )

    assert first_response.status_code == 502
    assert second_response.status_code == 502

    assert third_response.status_code == 503
    assert third_response.json() == {
        "detail": (
            "Upstream service temporarily unavailable"
        ),
    }

    assert request_count == 2


def test_external_status_recovers_after_circuit_timeout(
    client: TestClient,
) -> None:
    request_count = 0
    manual_clock = ManualClock()

    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=10.0,
        clock=manual_clock,
    )

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        status_code = (
            503
            if request_count <= 2
            else 200
        )

        return httpx.Response(
            status_code=status_code,
            request=request,
        )

    original_circuit_breaker_override = (
        client.app.dependency_overrides[
            get_external_service_circuit_breaker
        ]
    )

    def override_circuit_breaker() -> CircuitBreaker:
        return circuit_breaker

    client.app.dependency_overrides[
        get_external_service_circuit_breaker
    ] = override_circuit_breaker

    try:
        first_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )
        second_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )

        assert circuit_breaker.state is CircuitState.OPEN

        manual_clock.advance(10.0)

        # 时间到达后不会自动切换，仍要由请求触发探测
        assert circuit_breaker.state is CircuitState.OPEN

        recovery_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )
    finally:
        client.app.dependency_overrides[
            get_external_service_circuit_breaker
        ] = original_circuit_breaker_override

    assert first_response.status_code == 502
    assert second_response.status_code == 502

    assert recovery_response.status_code == 200
    assert recovery_response.json() == {
        "status": "available",
    }

    assert request_count == 3
    assert circuit_breaker.state is CircuitState.CLOSED
    assert circuit_breaker.failure_count == 0


def test_failed_half_open_probe_reopens_circuit(
    client: TestClient,
) -> None:
    request_count = 0
    manual_clock = ManualClock()

    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=10.0,
        clock=manual_clock,
    )

    def handle_request(
        request: httpx.Request,
    ) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        return httpx.Response(
            status_code=503,
            request=request,
        )

    original_circuit_breaker_override = (
        client.app.dependency_overrides[
            get_external_service_circuit_breaker
        ]
    )

    def override_circuit_breaker() -> CircuitBreaker:
        return circuit_breaker

    client.app.dependency_overrides[
        get_external_service_circuit_breaker
    ] = override_circuit_breaker

    try:
        first_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )
        second_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )

        assert circuit_breaker.state is CircuitState.OPEN

        manual_clock.advance(10.0)

        probe_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )

        assert circuit_breaker.state is CircuitState.OPEN

        blocked_response = request_external_status(
            client=client,
            handle_request=handle_request,
        )
    finally:
        client.app.dependency_overrides[
            get_external_service_circuit_breaker
        ] = original_circuit_breaker_override

    assert first_response.status_code == 502
    assert second_response.status_code == 502

    # 探测请求访问了上游，因此属于上游响应错误
    assert probe_response.status_code == 502

    # 后续请求没有访问上游，由熔断器直接拒绝
    assert blocked_response.status_code == 503
    assert blocked_response.json() == {
        "detail": (
            "Upstream service temporarily unavailable"
        ),
    }

    assert request_count == 3
    assert circuit_breaker.state is CircuitState.OPEN