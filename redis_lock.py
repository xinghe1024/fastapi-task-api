from collections.abc import Callable
from uuid import uuid4

from redis.asyncio import Redis


RELEASE_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end

return 0
"""


def generate_lock_token() -> str:
    return uuid4().hex


class RedisLock:
    def __init__(
        self,
        redis_client: Redis,
        ttl_seconds: int,
        token_generator: Callable[
            [],
            str,
        ] = generate_lock_token,
    ) -> None:
        if ttl_seconds < 1:
            raise ValueError(
                "ttl_seconds must be at least 1",
            )

        self._redis_client = redis_client
        self._ttl_seconds = ttl_seconds
        self._token_generator = token_generator

    async def acquire(
        self,
        lock_key: str,
    ) -> str | None:
        lock_token = self._token_generator()

        acquired = await self._redis_client.set(
            lock_key,
            lock_token,
            nx=True,
            ex=self._ttl_seconds,
        )

        if not acquired:
            return None

        return lock_token

    async def release(
        self,
        lock_key: str,
        lock_token: str,
    ) -> bool:
        deleted_count = await self._redis_client.eval(
            RELEASE_LOCK_SCRIPT,
            1,
            lock_key,
            lock_token,
        )

        return bool(deleted_count)