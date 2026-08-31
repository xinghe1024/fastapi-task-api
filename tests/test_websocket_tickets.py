import asyncio

from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from redis.asyncio import Redis

from websocket_tickets import (
    WebSocketTicketStore,
)


def test_ticket_store_issues_short_lived_ticket(
) -> None:
    redis_client = MagicMock(
        spec=Redis,
    )
    redis_client.set = AsyncMock(
        return_value=True,
    )

    ticket_store = WebSocketTicketStore(
        redis_client=redis_client,
        ttl_seconds=30,
        ticket_generator=lambda: "test-ticket",
    )

    ticket = asyncio.run(
        ticket_store.issue(user_id=7),
    )

    assert ticket == "test-ticket"
    redis_client.set.assert_awaited_once_with(
        "websocket-ticket:test-ticket",
        "7",
        nx=True,
        ex=30,
    )


def test_ticket_store_consumes_ticket_once(
) -> None:
    redis_client = MagicMock(
        spec=Redis,
    )
    redis_client.getdel = AsyncMock(
        return_value="7",
    )

    ticket_store = WebSocketTicketStore(
        redis_client=redis_client,
        ttl_seconds=30,
    )

    user_id = asyncio.run(
        ticket_store.consume("test-ticket"),
    )

    assert user_id == 7
    redis_client.getdel.assert_awaited_once_with(
        "websocket-ticket:test-ticket",
    )


def test_ticket_store_returns_none_for_missing_ticket(
) -> None:
    redis_client = MagicMock(
        spec=Redis,
    )
    redis_client.getdel = AsyncMock(
        return_value=None,
    )

    ticket_store = WebSocketTicketStore(
        redis_client=redis_client,
        ttl_seconds=30,
    )

    user_id = asyncio.run(
        ticket_store.consume("missing-ticket"),
    )

    assert user_id is None