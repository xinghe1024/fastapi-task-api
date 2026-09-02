from redis.asyncio import Redis
from starlette.requests import HTTPConnection


def get_redis_client(
    connection: HTTPConnection,
) -> Redis:
    return connection.app.state.redis_client