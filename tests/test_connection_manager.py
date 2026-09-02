import asyncio

from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)

from fastapi import (
    WebSocket,
    WebSocketDisconnect,
)

from connection_manager import ConnectionManager


def test_broadcast_removes_disconnected_client_and_continues(
) -> None:
    manager = ConnectionManager()

    disconnected_websocket = MagicMock(
        spec=WebSocket,
    )
    disconnected_websocket.accept = AsyncMock()
    disconnected_websocket.send_text = AsyncMock(
        side_effect=WebSocketDisconnect(
            code=1006,
        ),
    )

    active_websocket = MagicMock(
        spec=WebSocket,
    )
    active_websocket.accept = AsyncMock()
    active_websocket.send_text = AsyncMock()

    async def exercise_manager() -> None:
        await manager.connect(
            user_id=7,
            websocket=disconnected_websocket,
        )
        await manager.connect(
            user_id=7,
            websocket=active_websocket,
        )

        await manager.broadcast_to_user(
            user_id=7,
            message="first message",
        )
        await manager.broadcast_to_user(
            user_id=7,
            message="second message",
        )

    asyncio.run(exercise_manager())

    disconnected_websocket.send_text.assert_awaited_once_with(
        "first message",
    )
    assert (
        active_websocket.send_text.await_args_list
        == [
            call("first message"),
            call("second message"),
        ]
    )


def test_broadcast_sends_to_connections_concurrently(
) -> None:
    manager = ConnectionManager()

    first_websocket = MagicMock(
        spec=WebSocket,
    )
    first_websocket.accept = AsyncMock()
    first_websocket.send_text = AsyncMock()

    second_websocket = MagicMock(
        spec=WebSocket,
    )
    second_websocket.accept = AsyncMock()
    second_websocket.send_text = AsyncMock()

    async def exercise_manager() -> None:
        second_send_started = asyncio.Event()

        async def wait_for_second_send(
            message: str,
        ) -> None:
            assert message == "task updated"

            await asyncio.wait_for(
                second_send_started.wait(),
                timeout=0.2,
            )

        async def mark_second_send_started(
            message: str,
        ) -> None:
            assert message == "task updated"
            second_send_started.set()

        first_websocket.send_text.side_effect = (
            wait_for_second_send
        )
        second_websocket.send_text.side_effect = (
            mark_second_send_started
        )

        await manager.connect(
            user_id=7,
            websocket=first_websocket,
        )
        await manager.connect(
            user_id=7,
            websocket=second_websocket,
        )

        await manager.broadcast_to_user(
            user_id=7,
            message="task updated",
        )

    asyncio.run(exercise_manager())

    first_websocket.send_text.assert_awaited_once_with(
        "task updated",
    )
    second_websocket.send_text.assert_awaited_once_with(
        "task updated",
    )