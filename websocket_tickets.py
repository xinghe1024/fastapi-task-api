from collections.abc import Callable
from uuid import uuid4

from redis.asyncio import Redis


def generate_websocket_ticket() -> str:
    return uuid4().hex


class WebSocketTicketStore:
    def __init__(
        self,
        redis_client: Redis,
        ttl_seconds: int,
        ticket_generator: Callable[
            [],
            str,
        ] = generate_websocket_ticket,
    ) -> None:
        if ttl_seconds < 1:
            raise ValueError(
                "ttl_seconds must be at least 1",
            )

        self._redis_client = redis_client
        self._ttl_seconds = ttl_seconds
        self._ticket_generator = ticket_generator

    @staticmethod
    def _build_key(ticket: str) -> str:
        return f"websocket-ticket:{ticket}"

    async def issue(
        self,
        user_id: int,
    ) -> str:
        ticket = self._ticket_generator()
        ticket_key = self._build_key(ticket)

        stored = await self._redis_client.set(
            ticket_key,
            str(user_id),
            nx=True,
            ex=self._ttl_seconds,
        )

        if not stored:
            raise RuntimeError(
                "Could not issue WebSocket ticket",
            )

        return ticket

    async def consume(
        self,
        ticket: str,
    ) -> int | None:
        ticket_key = self._build_key(ticket)

        stored_user_id = (
            await self._redis_client.getdel(
                ticket_key,
            )
        )

        if stored_user_id is None:
            return None

        return int(stored_user_id)