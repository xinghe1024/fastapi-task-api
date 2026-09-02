import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from connection_manager import ConnectionManager
from realtime_event_handler import RealtimeEventHandler


def test_realtime_event_handler_broadcasts_valid_event() -> None:
    connection_manager = MagicMock(
        spec=ConnectionManager,
    )
    connection_manager.broadcast_to_user = AsyncMock()

    event_handler = RealtimeEventHandler(
        connection_manager=connection_manager,
    )

    handled = asyncio.run(
        event_handler.handle(
            """
            {
                "user_id": 7,
                "event": {
                    "type": "message",
                    "content": "Task updated"
                }
            }
            """
        )
    )

    assert handled is True

    connection_manager.broadcast_to_user.assert_awaited_once()

    call_arguments = (
        connection_manager
        .broadcast_to_user
        .await_args
        .kwargs
    )

    assert call_arguments["user_id"] == 7
    assert json.loads(call_arguments["message"]) == {
        "type": "message",
        "content": "Task updated",
    }


@pytest.mark.parametrize(
    "invalid_event",
    [
        "not-json",
        '{"user_id": 7}',
    ],
)
def test_realtime_event_handler_ignores_invalid_event(
    invalid_event: str,
) -> None:
    connection_manager = MagicMock(
        spec=ConnectionManager,
    )
    connection_manager.broadcast_to_user = AsyncMock()

    event_handler = RealtimeEventHandler(
        connection_manager=connection_manager,
    )

    handled = asyncio.run(
        event_handler.handle(invalid_event)
    )

    assert handled is False
    (
        connection_manager
        .broadcast_to_user
        .assert_not_awaited()
    )