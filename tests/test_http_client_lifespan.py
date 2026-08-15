from fastapi.testclient import TestClient

from main import app


def test_http_client_lifespan() -> None:
    with TestClient(app):
        http_client = app.state.http_client

        assert http_client.is_closed is False

    assert http_client.is_closed is True