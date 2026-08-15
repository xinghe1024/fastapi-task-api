from collections.abc import (
    AsyncGenerator,
    Callable,
)

import httpx
from fastapi.testclient import TestClient

from http_client_dependencies import get_http_client

def request_external_status(
    client: TestClient,
    handle_request: Callable[
        [httpx.Request],
        httpx.Response,
    ],
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