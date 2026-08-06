from fastapi.testclient import TestClient


def create_auth_headers(
    client: TestClient,
    username: str,
) -> dict[str, str]:
    password = "secure-test-password"

    register_response = client.post(
        "/auth/register",
        json={
            "username": username,
            "password": password,
        },
    )
    assert register_response.status_code == 201

    login_response = client.post(
        "/auth/token",
        data={
            "username": username,
            "password": password,
        },
    )
    assert login_response.status_code == 200

    access_token = login_response.json()[
        "access_token"
    ]

    return {
        "Authorization": f"Bearer {access_token}",
    }


def test_users_only_list_their_own_tasks(
    client: TestClient,
) -> None:
    alice_headers = create_auth_headers(
        client,
        "alice",
    )
    bob_headers = create_auth_headers(
        client,
        "bob",
    )

    alice_task = client.post(
        "/tasks",
        headers=alice_headers,
        json={"title": "Alice task"},
    ).json()

    bob_task = client.post(
        "/tasks",
        headers=bob_headers,
        json={"title": "Bob task"},
    ).json()

    alice_response = client.get(
        "/tasks",
        headers=alice_headers,
    )
    bob_response = client.get(
        "/tasks",
        headers=bob_headers,
    )

    assert alice_response.json() == [alice_task]
    assert bob_response.json() == [bob_task]

def test_user_cannot_access_another_users_task(
    client: TestClient,
) -> None:
    alice_headers = create_auth_headers(
        client,
        "alice",
    )
    bob_headers = create_auth_headers(
        client,
        "bob",
    )

    alice_task = client.post(
        "/tasks",
        headers=alice_headers,
        json={"title": "Alice private task"},
    ).json()

    task_path = f"/tasks/{alice_task['id']}"

    read_response = client.get(
        task_path,
        headers=bob_headers,
    )
    update_response = client.patch(
        task_path,
        headers=bob_headers,
        json={"completed": True},
    )
    delete_response = client.delete(
        task_path,
        headers=bob_headers,
    )

    assert read_response.status_code == 404
    assert update_response.status_code == 404
    assert delete_response.status_code == 404

    owner_response = client.get(
        task_path,
        headers=alice_headers,
    )

    assert owner_response.status_code == 200
    assert owner_response.json() == alice_task