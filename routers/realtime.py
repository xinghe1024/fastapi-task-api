from fastapi import (
    APIRouter,
    Depends,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from typing import Annotated

from json import JSONDecodeError

from pydantic import ValidationError


from connection_manager import ConnectionManager
from database_models import UserRecord
from websocket_authentication import (
    get_websocket_current_user,
)

from realtime_models import (
    ClientBroadcastMessage,
    RealtimeMessageEvent,
)


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
    current_user: Annotated[
        UserRecord,
        Depends(get_websocket_current_user),
    ],
) -> None:
    await connection_manager.connect(
        user_id=current_user.id,
        websocket=websocket,
    )

    try:
        while True:
            try:
                payload = await websocket.receive_json()

                incoming_message = (
                    ClientBroadcastMessage.model_validate(
                        payload,
                    )
                )
            except (
                JSONDecodeError,
                ValidationError,
            ):
                await websocket.close(
                    code=(
                        status.WS_1007_INVALID_FRAME_PAYLOAD_DATA
                    ),
                    reason="Invalid message payload",
                )
                return

            outgoing_event = RealtimeMessageEvent(
                content=incoming_message.content,
            )

            await connection_manager.broadcast_to_user(
                user_id=current_user.id,
                message=(
                    outgoing_event.model_dump_json()
                ),
            )
    except WebSocketDisconnect:
        pass
    finally:
        connection_manager.disconnect(
            user_id=current_user.id,
            websocket=websocket,
        )