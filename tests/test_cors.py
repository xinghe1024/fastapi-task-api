from fastapi.testclient import TestClient


TEST_ORIGIN = "http://test-frontend"


def test_cors_allows_configured_origin(
    client: TestClient,
) -> None:
    response = client.options(
        "/tasks",
        headers={
            "Origin": TEST_ORIGIN,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": (
                "Authorization"
            ),
        },
    )

    assert response.status_code == 200
    assert (
        response.headers[
            "access-control-allow-origin"
        ]
        == TEST_ORIGIN
    )
    assert (
        "authorization"
        in response.headers[
            "access-control-allow-headers"
        ].lower()
    )


def test_cors_exposes_process_time_header(
    client: TestClient,
) -> None:
    response = client.get(
        "/auth/me",
        headers={
            "Origin": TEST_ORIGIN,
        },
    )

    assert response.status_code == 401
    assert (
        response.headers[
            "access-control-allow-origin"
        ]
        == TEST_ORIGIN
    )
    assert (
        "X-Process-Time"
        in response.headers[
            "access-control-expose-headers"
        ]
    )