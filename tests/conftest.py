from collections.abc import Generator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from middlewares import register_middlewares
from authentication import get_current_user
from config import Settings, get_settings
from database_models import UserRecord
from dependencies import get_session
from routers.health import router as health_router
from routers.tasks import router as task_router
from routers.auth import router as auth_router
from database import (
    Base,
    enable_sqlite_foreign_keys,
)
from rate_limit_dependencies import (
    get_login_rate_limiter,
)
from rate_limiting import FixedWindowRateLimiter
from routers.external import router as external_router

@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    test_engine = create_engine(
        "sqlite://",
        connect_args={
            "check_same_thread": False,
        },
        poolclass=StaticPool,
    )
    TestSessionFactory = sessionmaker(
        bind=test_engine,
    )

    event.listen(
        test_engine,
        "connect",
        enable_sqlite_foreign_keys,
    )

    Base.metadata.create_all(
        bind=test_engine,
    )

    test_login_rate_limiter = FixedWindowRateLimiter(
        request_limit=5,
        window_seconds=60,
    )

    def override_get_login_rate_limiter(
    ) -> FixedWindowRateLimiter:
        return test_login_rate_limiter

    def override_get_session() -> Generator[
        Session,
        None,
        None
    ]:
        session = TestSessionFactory()

        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    test_settings = Settings(
        external_service_url=(
            "https://upstream.test/health"
        ),
        jwt_secret_key=(
            "test-secret-key-that-is-at-least-32-bytes"
        ),
        jwt_algorithm="HS256",
        access_token_expire_minutes=30,
        cors_allowed_origins=[
            "http://test-frontend",
        ],
        external_service_max_attempts=1,
        external_service_base_delay=0.0,
    )

    test_app = FastAPI()
    register_middlewares(
        test_app,
        test_settings.cors_allowed_origins,
    )
    test_app.include_router(task_router)
    test_app.include_router(auth_router)
    test_app.include_router(health_router)
    test_app.include_router(external_router)

    def override_get_settings() -> Settings:
        return test_settings

    test_app.dependency_overrides[
        get_session
    ] = override_get_session

    test_app.dependency_overrides[
        get_settings
    ] = override_get_settings

    test_app.dependency_overrides[
        get_login_rate_limiter
    ] = override_get_login_rate_limiter

    try:
        with TestClient(test_app) as client:
            yield client
    finally:
        test_app.dependency_overrides.clear()
        test_engine.dispose()

@pytest.fixture
def task_client(
    client: TestClient,
) -> Generator[TestClient, None, None]:
    username = "task-test-user"

    register_response = client.post(
        "/auth/register",
        json={
            "username": username,
            "password": "secure-test-password",
        },
    )
    assert register_response.status_code == 201

    registered_user = register_response.json()

    fake_user = UserRecord(
        id=registered_user["id"],
        username=username,
        hashed_password="not-used-in-task-tests",
        is_active=True,
    )

    def override_get_current_user() -> UserRecord:
        return fake_user

    client.app.dependency_overrides[
        get_current_user
    ] = override_get_current_user

    try:
        yield client
    finally:
        client.app.dependency_overrides.pop(
            get_current_user,
            None,
        )