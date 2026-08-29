from fastapi.testclient import TestClient
import pytest
import logging

from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from redis.exceptions import RedisError
from authentication import get_current_user
from models import TaskResponse
from task_cache import (
    TaskCache,
    TaskCacheLookup,
)
from task_cache_dependencies import get_task_cache


def test_database_starts_emty(
        task_client: TestClient,
) -> None:
    response = task_client.get('/tasks')

    assert response.status_code == 200
    assert response.json() == []

def test_create_task_persists_task(
        task_client: TestClient,
) -> None:
    task_payload = {
        'title': 'Learn testing',
        'description': "practice FastAPI tests",
        'priority': 3,
    }

    response = task_client.post(
        '/tasks',
        json=task_payload,
    )

    assert response.status_code == 201

    created_task = response.json()

    assert response.json() == created_task

    assert isinstance(created_task['id'], int)

    assert created_task['title'] == 'Learn testing'
    assert created_task['description'] == 'practice FastAPI tests'
    assert created_task['priority'] == 3
    assert created_task['completed'] is False

    read_response = task_client.get(
        f"/tasks/{created_task['id']}",
    )

    assert read_response.status_code == 200
    assert read_response.json() == created_task


def test_rejects_title_that_is_too_short(
        task_client: TestClient,
) -> None:
    invalid_payload = {
        'title': "A",
    }

    create_response = task_client.post(
        '/tasks',
        json = invalid_payload,
    )

    assert create_response.status_code == 422
    assert(
        create_response.json()['detail'][0]['loc']
        == ['body','title']
    )

    list_response = task_client.get('/tasks')

    assert list_response.status_code == 200
    assert list_response.json() == []

def test_returns_404_when_task_does_not_exist(
        task_client: TestClient,
) -> None:
    response = task_client.get(
        '/tasks/999',
    )

    assert response.status_code == 404
    assert response.json() == {
        'detail': 'Task not found'
    }

def create_task_for_test(
        task_client: TestClient,
        **task_overrides: object,
) -> dict[str, object]:
    task_payload: dict[str, object] = {
        "title": "Original title",
        "description": "Original description",
        "priority": 2,
    }
    task_payload.update(task_overrides)

    response = task_client.post(
        "/tasks",
        json=task_payload,
    )

    assert response.status_code == 201
    return response.json()

def test_patch_task_persists_changes(
        task_client: TestClient,
) -> None:
    created_task = create_task_for_test(task_client)
    task_id = created_task['id']

    cached_response = task_client.get(
        f"/tasks/{task_id}",
    )
    assert cached_response.status_code == 200
    assert cached_response.json() == created_task


    patch_response = task_client.patch(
        f"/tasks/{task_id}",
        json = {
            'completed': True,
        },
    )

    assert patch_response.status_code == 200
    updated_task = patch_response.json()

    assert updated_task['completed'] is True
    assert updated_task['title'] == 'Original title'
    assert updated_task['description'] == 'Original description'
    assert updated_task['priority'] == 2

    read_response = task_client.get(
        f"/tasks/{task_id}",
    )

    assert read_response.status_code == 200
    assert read_response.json() == updated_task

def test_delete_task_removes_task(
        task_client: TestClient,
) -> None:
    created_task = create_task_for_test(task_client)
    task_id = created_task['id']

    cached_response = task_client.get(
        f"/tasks/{task_id}",
    )

    assert cached_response.status_code == 200
    assert cached_response.json() == created_task

    delete_response = task_client.delete(
        f"/tasks/{task_id}",
    )

    assert delete_response.status_code == 204
    assert delete_response.content == b""

    read_response = task_client.get(
        f"/tasks/{task_id}",
    )

    assert read_response.status_code == 404
    error_body = read_response.json()
    assert error_body['detail'] == 'Task not found'


def test_get_tasks_applies_pagination(
        task_client: TestClient,
) -> None:
    find_task = create_task_for_test(task_client)
    search_task = create_task_for_test(task_client)
    third_task = create_task_for_test(task_client)

    response = task_client.get(
        '/tasks',
        params={
            'skip': 1,
            'limit': 1,
        },
    )

    assert response.status_code == 200
    assert response.json() == [search_task]

@pytest.mark.parametrize(
    "field_name",
    [
        "title",
        "priority",
        "completed",
    ],
)
def test_patch_rejects_null_values(
    task_client: TestClient,
    field_name: str,
) -> None:
    created_task = create_task_for_test(task_client)
    task_id = created_task["id"]

    patch_response = task_client.patch(
        f"/tasks/{task_id}",
        json={
            field_name: None,
        },
    )

    assert patch_response.status_code == 422

    read_response = task_client.get(
        f"/tasks/{task_id}",
    )

    assert read_response.status_code == 200
    assert read_response.json() == created_task

def test_create_task_rejects_owner_id(
    task_client: TestClient,
) -> None:
    response = task_client.post(
        "/tasks",
        json={
            "title": "Forged owner",
            "owner_id": 999,
        },
    )

    assert response.status_code == 422

def test_create_task_runs_audit_background_task(
    task_client: TestClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(
        logging.INFO,
        logger="audit_log",
    ):
        response = task_client.post(
            "/tasks",
            json={
                "title": "Audited task",
            },
        )

    assert response.status_code == 201

    created_task = response.json()

    assert any(
        record.name == "audit_log"
        and (
            f"task_id={created_task['id']}"
            in record.getMessage()
        )
        for record in caplog.records
    )


def test_create_task_due_date(
        task_client: TestClient,
) -> None:

    created_task = create_task_for_test(
        task_client,
        due_date="2026-08-15",
    )
    assert created_task["due_date"] == "2026-08-15"

    read_response = task_client.get(
        f"/tasks/{created_task['id']}",
    )
    assert read_response.status_code == 200
    assert read_response.json() == created_task


def test_patch_task_due_date(
        task_client: TestClient,
) -> None:
    created_task = create_task_for_test(
        task_client,
        due_date="2026-08-15",
    )
    assert created_task["due_date"] == "2026-08-15"

    patch_response = task_client.patch(
        f"/tasks/{created_task['id']}",
        json={
            "due_date": None,
        },
    )
    assert patch_response.status_code == 200
    patch_task = patch_response.json()
    assert patch_task["due_date"] is None

    read_response = task_client.get(
        f"/tasks/{patch_task['id']}",
    )

    assert read_response.status_code == 200
    assert read_response.json() == patch_task


def test_create_task_rejects_nonexistent_due_date(
        task_client: TestClient,
) -> None:
    response = task_client.post(
        "/tasks",
        json={
            "title": "Nonexistent task",
            "due_date": "2026-02-30",
        },
    )
    assert response.status_code == 422
    error_detail = response.json()["detail"][0]
    assert error_detail["loc"] == [
        "body",
        "due_date",
    ]
    assert (
            error_detail["type"]
            == "date_from_datetime_parsing"
    )

    read_response = task_client.get(
        "/tasks",
    )

    assert read_response.status_code == 200
    assert read_response.json() == []

def test_get_task_returns_cached_task(
    task_client: TestClient,
) -> None:
    cached_task = TaskResponse(
        id=999,
        title="Cached task",
        description=None,
        priority=2,
        completed=False,
        due_date=None,
    )

    task_cache = MagicMock(spec=TaskCache)
    task_cache.lookup_task = AsyncMock(
        return_value=TaskCacheLookup(
            cache_hit=True,
            task=cached_task,
        ),
    )
    task_cache.set_task = AsyncMock()

    def override_get_task_cache() -> TaskCache:
        return task_cache

    task_client.app.dependency_overrides[
        get_task_cache
    ] = override_get_task_cache

    try:
        response = task_client.get(
            "/tasks/999",
        )
    finally:
        task_client.app.dependency_overrides.pop(
            get_task_cache,
            None,
        )

    assert response.status_code == 200
    assert response.json() == (
        cached_task.model_dump(mode="json")
    )

    current_user = (
        task_client.app.dependency_overrides[
            get_current_user
        ]()
    )

    task_cache.lookup_task.assert_awaited_once_with(
        current_user.id,
        999,
    )
    task_cache.set_task.assert_not_awaited()


def test_get_task_falls_back_to_database_when_cache_unavailable(
    task_client: TestClient,
) -> None:
    created_task = create_task_for_test(
        task_client,
    )
    task_id = int(created_task["id"])

    task_cache = MagicMock(spec=TaskCache)
    task_cache.lookup_task = AsyncMock(
        side_effect=RedisError(
            "Redis read failed",
        ),
    )
    task_cache.set_task = AsyncMock(
        side_effect=RedisError(
            "Redis write failed",
        ),
    )

    def override_unavailable_task_cache(
    ) -> TaskCache:
        return task_cache

    task_client.app.dependency_overrides[
        get_task_cache
    ] = override_unavailable_task_cache

    try:
        response = task_client.get(
            f"/tasks/{task_id}",
        )
    finally:
        task_client.app.dependency_overrides.pop(
            get_task_cache,
            None,
        )

    assert response.status_code == 200
    assert response.json() == created_task

    current_user = (
        task_client.app.dependency_overrides[
            get_current_user
        ]()
    )

    task_cache.lookup_task.assert_awaited_once_with(
        current_user.id,
        task_id,
    )
    task_cache.set_task.assert_awaited_once()


def test_patch_task_succeeds_when_cache_invalidation_fails(
    task_client: TestClient,
) -> None:
    created_task = create_task_for_test(
        task_client,
    )
    task_id = int(created_task["id"])

    task_cache = MagicMock(spec=TaskCache)
    task_cache.delete_task = AsyncMock(
        side_effect=RedisError(
            "Redis delete failed",
        ),
    )

    def override_unavailable_task_cache(
    ) -> TaskCache:
        return task_cache

    task_client.app.dependency_overrides[
        get_task_cache
    ] = override_unavailable_task_cache

    try:
        patch_response = task_client.patch(
            f"/tasks/{task_id}",
            json={
                "completed": True,
            },
        )
    finally:
        task_client.app.dependency_overrides.pop(
            get_task_cache,
            None,
        )

    assert patch_response.status_code == 200
    updated_task = patch_response.json()
    assert updated_task["completed"] is True

    current_user = (
        task_client.app.dependency_overrides[
            get_current_user
        ]()
    )

    task_cache.delete_task.assert_awaited_once_with(
        current_user.id,
        task_id,
    )

    list_response = task_client.get("/tasks")

    assert list_response.status_code == 200
    assert list_response.json() == [
        updated_task,
    ]

def test_negative_cache_skips_database_and_is_invalidated_on_create(
    task_client: TestClient,
) -> None:
    first_response = task_client.get(
        "/tasks/1",
    )

    assert first_response.status_code == 404

    with patch(
        "task_cache_dependencies.find_owned_task",
    ) as find_owned_task_mock:
        second_response = task_client.get(
            "/tasks/1",
        )

    assert second_response.status_code == 404
    find_owned_task_mock.assert_not_called()

    created_task = create_task_for_test(
        task_client,
    )

    assert created_task["id"] == 1

    read_response = task_client.get(
        "/tasks/1",
    )

    assert read_response.status_code == 200
    assert read_response.json() == created_task