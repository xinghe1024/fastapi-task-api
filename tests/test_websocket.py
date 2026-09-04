import pytest

from realtime_models import RealtimeMessageEvent
from redis.exceptions import RedisError

from fastapi import (
    WebSocketDisconnect,
    status,
)
from fastapi.testclient import TestClient

class UnavailableRealtimeEventPublisher:
    async def publish_to_user(
        self,
        user_id: int,
        event: RealtimeMessageEvent,
    ) -> int:
        raise RedisError(
            "Redis is unavailable",
        )


def login_and_get_access_token(
    client: TestClient,
    username: str = "websocket-user",
) -> str:
    register_response = client.post(
        "/auth/register",
        json={
            "username": username,
            "password": "secure-password",
        },
    )
    assert register_response.status_code == 201

    login_response = client.post(
        "/auth/token",
        data={
            "username": username,
            "password": "secure-password",
        },
    )
    assert login_response.status_code == 200

    return str(
        login_response.json()["access_token"],
    )

def issue_websocket_ticket(
    client: TestClient,
    access_token: str,
) -> str:
    response = client.post(
        "/auth/websocket-ticket",
        headers={
            "Authorization": (
                f"Bearer {access_token}"
            ),
        },
    )

    assert response.status_code == 201

    return str(
        response.json()["ticket"],
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

    first_ticket = issue_websocket_ticket(
        client,
        access_token,
    )
    second_ticket = issue_websocket_ticket(
        client,
        access_token,
    )

    with client.websocket_connect(
        f"/ws/broadcast?ticket={first_ticket}",
    ) as first_websocket:
        with client.websocket_connect(
            f"/ws/broadcast?ticket={second_ticket}",
        ) as second_websocket:
            first_websocket.send_json(
                {
                    "content": "task updated",
                },
            )

            expected_event = {
                "type": "message",
                "content": "task updated",
            }

            assert (
                    first_websocket.receive_json()
                    == expected_event
            )
            assert (
                    second_websocket.receive_json()
                    == expected_event
            )


def test_websocket_broadcast_rejects_invalid_ticket(
    client: TestClient,
) -> None:
    with pytest.raises(
        WebSocketDisconnect,
    ) as exception_info:
        with client.websocket_connect(
            "/ws/broadcast?ticket=invalid-ticket",
        ):
            pass

    assert (
        exception_info.value.code
        == status.WS_1008_POLICY_VIOLATION
    )


def test_websocket_ticket_cannot_be_reused(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
    )
    ticket = issue_websocket_ticket(
        client,
        access_token,
    )
    websocket_url = (
        f"/ws/broadcast?ticket={ticket}"
    )

    with client.websocket_connect(
        websocket_url,
    ):
        pass

    with pytest.raises(
        WebSocketDisconnect,
    ) as exception_info:
        with client.websocket_connect(
            websocket_url,
        ):
            pass

    assert (
        exception_info.value.code
        == status.WS_1008_POLICY_VIOLATION
    )


def test_websocket_broadcast_is_isolated_by_user(
    client: TestClient,
) -> None:
    alice_access_token = login_and_get_access_token(
        client,
        username="alice-websocket",
    )
    bob_access_token = login_and_get_access_token(
        client,
        username="bob-websocket",
    )

    alice_ticket = issue_websocket_ticket(
        client,
        alice_access_token,
    )
    bob_ticket = issue_websocket_ticket(
        client,
        bob_access_token,
    )

    with client.websocket_connect(
        f"/ws/broadcast?ticket={alice_ticket}",
    ) as alice_websocket:
        with client.websocket_connect(
            f"/ws/broadcast?ticket={bob_ticket}",
        ) as bob_websocket:
            alice_websocket.send_json(
                {
                    "content": "alice update",
                },
            )
            assert alice_websocket.receive_json() == {
                "type": "message",
                "content": "alice update",
            }

            bob_websocket.send_json(
                {
                    "content": "bob update",
                },
            )
            assert bob_websocket.receive_json() == {
                "type": "message",
                "content": "bob update",
            }


def test_websocket_rejects_invalid_message_model(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
    )
    ticket = issue_websocket_ticket(
        client,
        access_token,
    )

    with client.websocket_connect(
        f"/ws/broadcast?ticket={ticket}",
    ) as websocket:
        websocket.send_json(
            {
                "content": "",
            },
        )

        with pytest.raises(
            WebSocketDisconnect,
        ) as exception_info:
            websocket.receive_json()

    assert (
        exception_info.value.code
        == status.WS_1007_INVALID_FRAME_PAYLOAD_DATA
    )


def test_websocket_rejects_malformed_json(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
    )
    ticket = issue_websocket_ticket(
        client,
        access_token,
    )

    with client.websocket_connect(
        f"/ws/broadcast?ticket={ticket}",
    ) as websocket:
        websocket.send_text(
            "this is not json",
        )

        with pytest.raises(
            WebSocketDisconnect,
        ) as exception_info:
            websocket.receive_json()

    assert (
        exception_info.value.code
        == status.WS_1007_INVALID_FRAME_PAYLOAD_DATA
    )


def test_websocket_rejects_extra_message_field(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
    )
    ticket = issue_websocket_ticket(
        client,
        access_token,
    )

    with client.websocket_connect(
        f"/ws/broadcast?ticket={ticket}",
    ) as websocket:
        websocket.send_json(
            {
                "content": "valid content",
                "unexpected": True,
            },
        )

        with pytest.raises(
            WebSocketDisconnect,
        ) as exception_info:
            websocket.receive_json()

    assert (
        exception_info.value.code
        == status.WS_1007_INVALID_FRAME_PAYLOAD_DATA
    )


def test_websocket_broadcast_uses_event_publisher(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
        username="publisher-test-user",
    )
    ticket = issue_websocket_ticket(
        client,
        access_token,
    )

    publisher = (
        client
        .app
        .state
        .realtime_event_publisher
    )

    with client.websocket_connect(
        f"/ws/broadcast?ticket={ticket}",
    ) as websocket:
        websocket.send_json(
            {
                "content": "published event",
            },
        )

        assert websocket.receive_json() == {
            "type": "message",
            "content": "published event",
        }

    assert len(publisher.published_events) == 1

    published_user_id, published_event = (
        publisher.published_events[0]
    )

    assert published_user_id >= 1
    assert published_event == RealtimeMessageEvent(
        content="published event",
    )


def test_websocket_closes_with_1013_when_publish_fails(
    client: TestClient,
) -> None:
    access_token = login_and_get_access_token(
        client,
        username="unavailable-publisher-user",
    )
    ticket = issue_websocket_ticket(
        client,
        access_token,
    )

    original_publisher = (
        client
        .app
        .state
        .realtime_event_publisher
    )

    client.app.state.realtime_event_publisher = (
        UnavailableRealtimeEventPublisher()
    )

    try:
        with client.websocket_connect(
            f"/ws/broadcast?ticket={ticket}",
        ) as websocket:
            websocket.send_json(
                {
                    "content": "task updated",
                },
            )

            with pytest.raises(
                WebSocketDisconnect,
            ) as exception_info:
                websocket.receive_json()
    finally:
        client.app.state.realtime_event_publisher = (
            original_publisher
        )

    assert (
        exception_info.value.code
        == status.WS_1013_TRY_AGAIN_LATER
    )
    assert (
        exception_info.value.reason
        == "Realtime service unavailable"
    )
