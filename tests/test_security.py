import jwt
from datetime import timedelta

import pytest
from jwt.exceptions import ExpiredSignatureError

from security import (
    create_access_token,
    decode_access_token,
)

from config import Settings
from security import create_access_token

def test_create_access_token_contains_required_claims() -> None:
    test_settings = Settings(
        jwt_secret_key="test-secret-key-that-is-at-least-32-bytes",
        jwt_algorithm="HS256",
        access_token_expire_minutes=30,
    )

    token = create_access_token(
        subject="user-1",
        settings=test_settings,
    )

    payload = jwt.decode(
        token,
        test_settings.jwt_secret_key.get_secret_value(),
        algorithms=[
            test_settings.jwt_algorithm,
        ],
        options={
            "require": [
                "sub",
                "iat",
                "exp",
            ],
        },
    )

    assert payload["sub"] == "user-1"
    assert payload["exp"] > payload["iat"]


def test_decode_rejects_expired_token() -> None:
    test_settings = Settings(
        jwt_secret_key=(
            "test-secret-key-that-is-at-least-32-bytes"
        ),
        jwt_algorithm="HS256",
        access_token_expire_minutes=30,
    )

    expired_token = create_access_token(
        subject="user-1",
        settings=test_settings,
        expires_delta=timedelta(
            seconds=-1,
        ),
    )

    with pytest.raises(ExpiredSignatureError):
        decode_access_token(
            token=expired_token,
            settings=test_settings,
        )