from sqlalchemy import select
from sqlalchemy.orm import Session

from database_models import UserRecord
from passwords import hash_password,verify_password

from typing import Annotated

from fastapi import Depends, HTTPException, status
from jwt.exceptions import InvalidTokenError

from config import Settings, get_settings
from dependencies import get_session
from security import (
    decode_access_token,
    oauth2_scheme,
)

DUMMY_PASSWORD_HASH = hash_password(
    "dummy-password-not-used-for-login",
)

def authenticate_user(
        session: Session,
        username: str,
        plain_password: str,
) -> UserRecord | None:
    statement = select(UserRecord).where(
        UserRecord.username == username
    )
    user_record = session.scalar(statement)

    if user_record is None:
        verify_password(
            plain_password,
            DUMMY_PASSWORD_HASH,
        )
        return user_record

    if not verify_password(
        plain_password,
        user_record.hashed_password,
    ):
        return None

    if not user_record.is_active:
        return None

    return user_record

def create_credentials_exception() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={
            "WWW-Authenticate": "Bearer",
        },
    )


def get_current_user(
    token: Annotated[
        str,
        Depends(oauth2_scheme),
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
        subject = decode_access_token(
            token=token,
            settings=settings,
        )
        user_id = int(subject)
    except (
        InvalidTokenError,
        ValueError,
    ) as error:
        raise create_credentials_exception() from error

    user_record = session.get(
        UserRecord,
        user_id,
    )

    if (
        user_record is None
        or not user_record.is_active
    ):
        raise create_credentials_exception()

    return user_record