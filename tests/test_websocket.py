import pytest


from fastapi import (
    WebSocketDisconnect,
    status,
)
from fastapi.testclient import TestClient

def login_and_get_access_token(
    client: TestClient,
) -> str:
    register_response = client.post(
        "/auth/register",
        json={
            "username": "websocket-user",
            "password": "secure-password",
        },
    )
    assert register_response.status_code == 201

    login_response = client.post(
        "/auth/token",
        data={
            "username": "websocket-user",
            "password": "secure-password",
        },
    )
    assert login_response.status_code == 200

    return str(
        login_response.json()["access_token"],
    )


def test_websocket_echoes_multiple_messages(
    client: TestClient,
) -> None:
    with client.websocket_connect(
        "/ws/echo",
    ) as websocket:
        websocket.send_text("first message")
        assert (
            websocket.receive_text()
            == "first message"
        )

        websocket.send_text("second message")
        assert (
            websocket.receive_text()
            == "second message"
        )


def test_websocket_broadcasts_to_all_clients(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
    )
    websocket_url = (
        f"/ws/broadcast?token={access_token}"
    )

    with client.websocket_connect(
            websocket_url,
    ) as first_websocket:
        with client.websocket_connect(
                websocket_url,
        ) as second_websocket:
            first_websocket.send_text(
                "task updated",
            )

            assert (
                first_websocket.receive_text()
                == "task updated"
            )
            assert (
                second_websocket.receive_text()
                == "task updated"
            )


def test_websocket_broadcast_rejects_invalid_token(
    client: TestClient,
) -> None:
    with pytest.raises(
        WebSocketDisconnect,
    ) as exception_info:
        with client.websocket_connect(
            "/ws/broadcast?token=invalid-token",
        ):
            pass

    assert (
        exception_info.value.code
        == status.WS_1008_POLICY_VIOLATION
    )

