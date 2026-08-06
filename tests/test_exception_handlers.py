from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from exception_handlers import (
    register_exception_handlers,
)
from middlewares import register_middlewares


def test_unexpected_exception_returns_safe_response(
) -> None:
    test_app = FastAPI()

    register_exception_handlers(test_app)
    register_middlewares(
        test_app,
        ["http://test-frontend"],
    )

    @test_app.get("/failure")
    def raise_unexpected_error() -> None:
        raise RuntimeError(
            "Sensitive internal information"
        )

    with TestClient(
        test_app,
        raise_server_exceptions=False,
    ) as client:
        response = client.get("/failure")

    assert response.status_code == 500
    assert response.json() == {
        "detail": "Internal server error",
    }
    assert (
        "Sensitive internal information"
        not in response.text
    )

    request_id = response.headers[
        "x-request-id"
    ]
    assert UUID(hex=request_id).version == 4