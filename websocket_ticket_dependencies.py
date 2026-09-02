from typing import Annotated

from fastapi import Depends
from redis.asyncio import Redis

from config import Settings, get_settings
from redis_dependencies import get_redis_client
from websocket_tickets import (
    WebSocketTicketStore,
)


def get_websocket_ticket_store(
    redis_client: Annotated[
        Redis,
        Depends(get_redis_client),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> WebSocketTicketStore:
    return WebSocketTicketStore(
        redis_client=redis_client,
        ttl_seconds=(
            settings.websocket_ticket_ttl_seconds
        ),
    )