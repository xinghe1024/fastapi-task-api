import pytest
from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    "request_path",
    [
        "/tasks?skip=-1",
        "/tasks?limit=101",
        "/tasks/abc",
    ],
    ids=[
        "negative-skip",
        "limit-above-max",
        "invalid-task-id",
    ],
)
def test_rejects_invalid_parameters(
    task_client: TestClient,
    request_path: str,
) -> None:
    response = task_client.get(request_path)

    assert response.status_code == 422