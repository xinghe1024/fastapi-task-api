from typing import Annotated

from fastapi import (
    Depends,
    Query,
    WebSocketException,
    status,
)
from sqlalchemy.orm import Session

from authentication import (
    InvalidCredentialsError,
    resolve_user_from_token,
)
from config import Settings, get_settings
from database_models import UserRecord
from dependencies import get_session


def get_websocket_current_user(
    token: Annotated[
        str,
        Query(min_length=1),
    ],
    session: Annotated[
        Session,
        Depends(get_session),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> UserRecord:
    try:
        return resolve_user_from_token(
            token=token,
            session=session,
            settings=settings,
        )
    except InvalidCredentialsError as error:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Could not validate credentials",
        ) from error