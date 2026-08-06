from typing import Annotated

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

import dependencies


class FakeSession:
    def __init__(self) -> None:
        self.was_rolled_back = False
        self.was_closed = False

    def rollback(self) -> None:
        self.was_rolled_back = True

    def close(self) -> None:
        self.was_closed = True


def test_get_session_rolls_back_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = FakeSession()

    monkeypatch.setattr(
        dependencies,
        "SessionFactory",
        lambda: fake_session,
    )

    test_app = FastAPI()

    @test_app.get("/failure")
    def raise_database_error(
        session: Annotated[
            object,
            Depends(dependencies.get_session),
        ],
    ) -> None:
        assert session is fake_session
        raise RuntimeError("Simulated failure")

    with TestClient(test_app) as test_client:
        with pytest.raises(
            RuntimeError,
            match="Simulated failure",
        ):
            test_client.get("/failure")

    assert fake_session.was_rolled_back is True
    assert fake_session.was_closed is True