from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from redis.exceptions import RedisError

from redis_dependencies import get_redis_client
from dependencies import get_session


class UnavailableSession:
    def execute(
        self,
        _statement: object,
    ) -> None:
        raise SQLAlchemyError(
            "Sensitive database information",
        )

class UnavailableRedisClient:
    async def ping(self) -> None:
        raise RedisError(
            "Sensitive Redis information",
        )


def override_unavailable_session(
) -> UnavailableSession:
    return UnavailableSession()


def test_liveness_returns_ok(
    client: TestClient,
) -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
    }


def test_readiness_returns_ok_when_database_available(
    client: TestClient,
) -> None:
    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
    }


def test_readiness_returns_503_when_database_unavailable(
    client: TestClient,
) -> None:
    original_override = (
        client.app.dependency_overrides[get_session]
    )
    client.app.dependency_overrides[
        get_session
    ] = override_unavailable_session

    try:
        response = client.get("/health/ready")
    finally:
        client.app.dependency_overrides[
            get_session
        ] = original_override

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Database unavailable",
    }
    assert (
        "Sensitive database information"
        not in response.text
    )


def test_redis_health_returns_ok_when_available(
    client: TestClient,
) -> None:
    response = client.get("/health/redis")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
    }


def test_redis_health_returns_503_when_unavailable(
    client: TestClient,
) -> None:
    original_override = (
        client.app.dependency_overrides[
            get_redis_client
        ]
    )

    def override_unavailable_redis(
    ) -> UnavailableRedisClient:
        return UnavailableRedisClient()

    client.app.dependency_overrides[
        get_redis_client
    ] = override_unavailable_redis

    try:
        response = client.get("/health/redis")
    finally:
        client.app.dependency_overrides[
            get_redis_client
        ] = original_override

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Redis unavailable",
    }
    assert (
        "Sensitive Redis information"
        not in response.text
    )