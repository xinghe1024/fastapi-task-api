import logging


from typing import Annotated

from json import JSONDecodeError

from pydantic import ValidationError

from redis.exceptions import RedisError
from connection_manager import ConnectionManager
from database_models import UserRecord

from fastapi import (
    APIRouter,
    Depends,
    WebSocket,
    WebSocketDisconnect,
    status,
)

from websocket_authentication import (
    get_websocket_current_user,
)

from realtime_dependencies import (
    get_connection_manager,
    get_realtime_event_publisher,
)
from realtime_broker import (
    RedisRealtimeEventPublisher,
)
from realtime_models import (
    ClientBroadcastMessage,
    RealtimeMessageEvent,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ws",
)

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
    connection_manager: Annotated[
        ConnectionManager,
        Depends(get_connection_manager),
    ],
    realtime_event_publisher: Annotated[
        RedisRealtimeEventPublisher,
        Depends(get_realtime_event_publisher),
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

            try:
                await realtime_event_publisher.publish_to_user(
                    user_id=current_user.id,
                    event=outgoing_event,
                )
            except RedisError:
                logger.exception(
                    "实时事件发布失败",
                    extra={
                        "user_id": current_user.id,
                    },
                )

                await websocket.close(
                    code=status.WS_1013_TRY_AGAIN_LATER,
                    reason="Realtime service unavailable",
                )
                return
    except WebSocketDisconnect:
        pass
    finally:
        connection_manager.disconnect(
            user_id=current_user.id,
            websocket=websocket,
        )