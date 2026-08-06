import logging
from uuid import UUID

import pytest
from fastapi.testclient import TestClient


def test_response_contains_process_time_header(
    client: TestClient,
) -> None:
    response = client.get("/auth/me")

    assert response.status_code == 401

    process_time = float(
        response.headers["x-process-time"]
    )
    assert process_time >= 0

def test_request_id_correlates_response_and_log(
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(
        logging.INFO,
        logger="middlewares",
    ):
        response = client.get("/auth/me")

    assert response.status_code == 401

    request_id = response.headers[
        "x-request-id"
    ]

    assert UUID(hex=request_id).version == 4

    assert any(
        record.name == "middlewares"
        and request_id in record.getMessage()
        for record in caplog.records
    )