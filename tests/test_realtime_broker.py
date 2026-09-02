import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from redis.asyncio import Redis

from realtime_broker import (
    REALTIME_EVENTS_CHANNEL,
    RedisRealtimeEventPublisher,
)
from realtime_models import RealtimeMessageEvent


def test_realtime_publisher_serializes_user_event() -> None:
    redis_client = MagicMock(spec=Redis)
    redis_client.publish = AsyncMock(return_value=3)

    publisher = RedisRealtimeEventPublisher(
        redis_client=redis_client,
    )

    subscriber_count = asyncio.run(
        publisher.publish_to_user(
            user_id=7,
            event=RealtimeMessageEvent(
                content="Task updated",
            ),
        )
    )

    assert subscriber_count == 3

    redis_client.publish.assert_awaited_once()

    channel, serialized_event = (
        redis_client.publish.await_args.args
    )

    assert channel == REALTIME_EVENTS_CHANNEL
    assert json.loads(serialized_event) == {
        "user_id": 7,
        "event": {
            "type": "message",
            "content": "Task updated",
        },
    }