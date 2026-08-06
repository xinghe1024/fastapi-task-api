from passwords import (
    hash_password,
    verify_password,
)

def test_hash_and_verify_password() -> None:
    plain_password = "secure-password"

    hashed_password = hash_password(
        plain_password,
    )

    assert hash_password != hashed_password
    assert verify_password(
        plain_password,
        hashed_password,
    ) is True
    assert verify_password(
        "wrong password",
        hashed_password,
    ) is False