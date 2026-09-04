from starlette.requests import HTTPConnection

from connection_manager import ConnectionManager
from realtime_broker import (
    RedisRealtimeEventPublisher,
)


def get_connection_manager(
    connection: HTTPConnection,
) -> ConnectionManager:
    return connection.app.state.connection_manager


def get_realtime_event_publisher(
    connection: HTTPConnection,
) -> RedisRealtimeEventPublisher:
    return (
        connection
        .app
        .state
        .realtime_event_publisher
    )