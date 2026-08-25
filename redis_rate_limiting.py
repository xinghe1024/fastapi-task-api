from redis.asyncio import Redis


FIXED_WINDOW_SCRIPT = """
local request_count = redis.call(
    "INCR",
    KEYS[1]
)

if request_count == 1 then
    redis.call(
        "EXPIRE",
        KEYS[1],
        tonumber(ARGV[1])
    )
end

local retry_after_seconds = redis.call(
    "TTL",
    KEYS[1]
)

return {
    request_count,
    retry_after_seconds
}
"""


class RedisFixedWindowRateLimiter:
    def __init__(
        self,
        redis_client: Redis,
        request_limit: int,
        window_seconds: int,
        key_prefix: str,
    ) -> None:
        if request_limit < 1:
            raise ValueError(
                "request_limit must be at least 1",
            )

        if window_seconds < 1:
            raise ValueError(
                "window_seconds must be at least 1",
            )

        self._redis_client = redis_client
        self._request_limit = request_limit
        self._window_seconds = window_seconds
        self._key_prefix = key_prefix

    async def check(
        self,
        client_key: str,
    ) -> int | None:
        redis_key = (
            f"{self._key_prefix}:{client_key}"
        )

        script_result = await self._redis_client.eval(
            FIXED_WINDOW_SCRIPT,
            1,
            redis_key,
            self._window_seconds,
        )

        request_count = int(script_result[0])
        retry_after_seconds = int(script_result[1])

        if request_count <= self._request_limit:
            return None

        return max(1, retry_after_seconds)