from asyncio import TaskGroup

from fastapi import (
    WebSocket,
    WebSocketDisconnect,
)



class ConnectionManager:
    def __init__(self) -> None:
        self._active_connections: list[
            WebSocket
        ] = []

    async def connect(
        self,
        websocket: WebSocket,
    ) -> None:
        await websocket.accept()
        self._active_connections.append(
            websocket,
        )

    def disconnect(
        self,
        websocket: WebSocket,
    ) -> None:
        if websocket in self._active_connections:
            self._active_connections.remove(
                websocket,
            )

    async def broadcast(
            self,
            message: str,
    ) -> None:
        connections_snapshot = tuple(
            self._active_connections,
        )

        async with TaskGroup() as task_group:
            for connection in connections_snapshot:
                task_group.create_task(
                    self._send_to_connection(
                        connection=connection,
                        message=message,
                    )
                )

    async def _send_to_connection(
            self,
            connection: WebSocket,
            message: str,
    ) -> None:
        try:
            await connection.send_text(message)
        except WebSocketDisconnect:
            self.disconnect(connection)