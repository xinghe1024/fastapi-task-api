from typing import Annotated

from fastapi import (
    Depends,
    Query,
    WebSocketException,
    status,
)
from redis.exceptions import RedisError
from sqlalchemy.orm import Session

from database_models import UserRecord
from dependencies import get_session
from websocket_ticket_dependencies import (
    get_websocket_ticket_store,
)
from websocket_tickets import (
    WebSocketTicketStore,
)


async def consume_websocket_ticket(
    ticket: Annotated[
        str,
        Query(min_length=1),
    ],
    ticket_store: Annotated[
        WebSocketTicketStore,
        Depends(get_websocket_ticket_store),
    ],
) -> int:
    try:
        user_id = await ticket_store.consume(
            ticket,
        )
    except RedisError as error:
        raise WebSocketException(
            code=status.WS_1013_TRY_AGAIN_LATER,
            reason="Authentication service unavailable",
        ) from error

    if user_id is None:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Could not validate credentials",
        )

    return user_id


def get_websocket_current_user(
    user_id: Annotated[
        int,
        Depends(consume_websocket_ticket),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
) -> UserRecord:
    user_record = session.get(
        UserRecord,
        user_id,
    )

    if (
        user_record is None
        or not user_record.is_active
    ):
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Could not validate credentials",
        )

    return user_record