from asyncio import TaskGroup

from fastapi import (
    WebSocket,
    WebSocketDisconnect,
)


class ConnectionManager:
    def __init__(self) -> None:
        self._connections_by_user: dict[
            int,
            list[WebSocket],
        ] = {}

    async def connect(
        self,
        user_id: int,
        websocket: WebSocket,
    ) -> None:
        await websocket.accept()

        user_connections = (
            self._connections_by_user.setdefault(
                user_id,
                [],
            )
        )
        user_connections.append(websocket)

    def disconnect(
        self,
        user_id: int,
        websocket: WebSocket,
    ) -> None:
        user_connections = (
            self._connections_by_user.get(
                user_id,
            )
        )

        if (
            user_connections is None
            or websocket not in user_connections
        ):
            return

        user_connections.remove(websocket)

        if not user_connections:
            self._connections_by_user.pop(
                user_id,
                None,
            )

    async def broadcast_to_user(
        self,
        user_id: int,
        message: str,
    ) -> None:
        connections_snapshot = tuple(
            self._connections_by_user.get(
                user_id,
                (),
            )
        )

        async with TaskGroup() as task_group:
            for connection in connections_snapshot:
                task_group.create_task(
                    self._send_to_connection(
                        user_id=user_id,
                        connection=connection,
                        message=message,
                    )
                )

    async def _send_to_connection(
        self,
        user_id: int,
        connection: WebSocket,
        message: str,
    ) -> None:
        try:
            await connection.send_text(message)
        except WebSocketDisconnect:
            self.disconnect(
                user_id=user_id,
                websocket=connection,
            )