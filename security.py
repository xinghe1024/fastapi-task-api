import jwt


from datetime import datetime, timedelta, timezone

from jwt.exceptions import InvalidTokenError
from fastapi.security import OAuth2PasswordBearer

from config import Settings


oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="auth/token",
)


def create_access_token(
    subject: str,
    settings: Settings,
    expires_delta: timedelta | None = None,
) -> str:
    issued_at = datetime.now(timezone.utc)

    if expires_delta is None:
        expires_delta = timedelta(
            minutes=settings.access_token_expire_minutes,
        )

    payload = {
        "sub": subject,
        "iat": issued_at,
        "exp": issued_at + expires_delta,
    }

    return jwt.encode(
        payload,
        settings.jwt_secret_key.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )

def decode_access_token(
    token: str,
    settings: Settings,
) -> str:
    payload = jwt.decode(
        token,
        settings.jwt_secret_key.get_secret_value(),
        algorithms=[
            settings.jwt_algorithm,
        ],
        options={
            "require": [
                "sub",
                "iat",
                "exp",
            ],
        },
    )

    subject = payload["sub"]

    if not isinstance(subject, str) or not subject:
        raise InvalidTokenError(
            "Token subject is invalid",
        )

    return subject