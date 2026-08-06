import pytest
from fastapi.testclient import TestClient

def test_current_user_requires_token(
    client: TestClient,
) -> None:
    response = client.get("/auth/me")

    assert response.status_code == 401
    assert response.json() == {
        "detail": "Not authenticated",
    }

def test_current_user_rejects_invalid_token(
    client: TestClient,
) -> None:
    response = client.get(
        "/auth/me",
        headers={
            "Authorization": "Bearer invalid-token",
        },
    )

    assert response.status_code == 401
    assert response.json() == {
        "detail": "Could not validate credentials",
    }
    assert (
        response.headers["www-authenticate"]
        == "Bearer"
    )

def test_current_user_returns_authenticated_user(
    client: TestClient,
) -> None:
    register_response = client.post(
        "/auth/register",
        json={
            "username": "alice",
            "password": "secure-password",
        },
    )
    assert register_response.status_code == 201

    login_response = client.post(
        "/auth/token",
        data={
            "username": "alice",
            "password": "secure-password",
        },
    )
    assert login_response.status_code == 200

    access_token = login_response.json()[
        "access_token"
    ]

    current_user_response = client.get(
        "/auth/me",
        headers={
            "Authorization": (
                f"Bearer {access_token}"
            ),
        },
    )

    assert current_user_response.status_code == 200
    assert (
        current_user_response.json()
        == register_response.json()
    )

def test_register_user_does_not_expose_password(
    client: TestClient,
) -> None:
    response = client.post(
        "/auth/register",
        json={
            "username": "alice",
            "password": "secure-password",
        },
    )

    assert response.status_code == 201

    registered_user = response.json()

    assert isinstance(registered_user["id"], int)
    assert registered_user["username"] == "alice"
    assert registered_user["is_active"] is True
    assert "password" not in registered_user
    assert "hashed_password" not in registered_user

def test_register_rejects_duplicate_username(
    client: TestClient,
) -> None:
    user_payload = {
        "username": "alice",
        "password": "secure-password",
    }

    first_response = client.post(
        "/auth/register",
        json=user_payload,
    )
    second_response = client.post(
        "/auth/register",
        json=user_payload,
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 409
    assert second_response.json() == {
        "detail": "Username already registered",
    }

def test_login_returns_access_token(
    client: TestClient,
) -> None:
    register_response = client.post(
        "/auth/register",
        json={
            "username": "alice",
            "password": "secure-password",
        },
    )
    assert register_response.status_code == 201

    login_response = client.post(
        "/auth/token",
        data={
            "username": "alice",
            "password": "secure-password",
        },
    )

    assert login_response.status_code == 200

    token_response = login_response.json()

    assert token_response["token_type"] == "bearer"
    assert isinstance(
        token_response["access_token"],
        str,
    )
    assert token_response["access_token"]

@pytest.mark.parametrize(
    "username,password",
    [
        (
            "alice",
            "wrong-password",
        ),
        (
            "unknown-user",
            "secure-password",
        ),
    ],
    ids=[
        "wrong-password",
        "unknown-user",
    ],
)
def test_login_rejects_invalid_credentials(
    client: TestClient,
    username: str,
    password: str,
) -> None:
    register_response = client.post(
        "/auth/register",
        json={
            "username": "alice",
            "password": "secure-password",
        },
    )
    assert register_response.status_code == 201

    login_response = client.post(
        "/auth/token",
        data={
            "username": username,
            "password": password,
        },
    )

    assert login_response.status_code == 401
    assert login_response.json() == {
        "detail": "Incorrect username or password",
    }
    assert (
        login_response.headers["www-authenticate"]
        == "Bearer"
    )

def test_tasks_require_authentication(
    client: TestClient,
) -> None:
    response = client.get("/tasks")

    assert response.status_code == 401
    assert response.json() == {
        "detail": "Not authenticated",
    }