import logging

from pydantic import ValidationError

from connection_manager import ConnectionManager
from realtime_models import UserRealtimeEventEnvelope


logger = logging.getLogger(__name__)


class RealtimeEventHandler:
    def __init__(
        self,
        connection_manager: ConnectionManager,
    ) -> None:
        self._connection_manager = connection_manager

    async def handle(
        self,
        serialized_event: str | bytes,
    ) -> bool:
        try:
            envelope = (
                UserRealtimeEventEnvelope.model_validate_json(
                    serialized_event,
                )
            )
        except ValidationError:
            logger.warning(
                "忽略格式不合法的实时事件",
            )
            return False

        await self._connection_manager.broadcast_to_user(
            user_id=envelope.user_id,
            message=envelope.event.model_dump_json(),
        )

        return True