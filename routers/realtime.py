from fastapi import (
    APIRouter,
    WebSocket,
    WebSocketDisconnect,
)
from typing import Annotated

from fastapi import Depends


from database_models import UserRecord
from websocket_authentication import (
    get_websocket_current_user,
)
from connection_manager import ConnectionManager

router = APIRouter(
    prefix="/ws",
)

connection_manager = ConnectionManager()

@router.websocket("/echo")
async def echo_messages(
    websocket: WebSocket,
) -> None:
    await websocket.accept()

    try:
        while True:
            message = await websocket.receive_text()
            await websocket.send_text(message)
    except WebSocketDisconnect:
        return


@router.websocket("/broadcast")
async def broadcast_messages(
    websocket: WebSocket,
    _current_user: Annotated[
        UserRecord,
        Depends(get_websocket_current_user),
    ],
) -> None:
    await connection_manager.connect(
        websocket,
    )

    try:
        while True:
            message = await websocket.receive_text()

            await connection_manager.broadcast(
                message,
            )
    except WebSocketDisconnect:
        connection_manager.disconnect(
            websocket,
        )