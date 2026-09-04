import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from redis.asyncio import Redis
from realtime_broker import RedisRealtimeEventSubscriber
from realtime_event_handler import RealtimeEventHandler
from realtime_models import RealtimeMessageEvent

from realtime_broker import (
    REALTIME_EVENTS_CHANNEL,
    RedisRealtimeEventPublisher,
)



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


def test_realtime_subscriber_dispatches_business_message() -> None:
    serialized_event = (
        '{"user_id":7,'
        '"event":{'
        '"type":"message",'
        '"content":"Task updated"'
        '}}'
    )

    async def generate_messages():
        yield {
            "type": "subscribe",
            "channel": REALTIME_EVENTS_CHANNEL,
            "data": 1,
        }
        yield {
            "type": "message",
            "channel": REALTIME_EVENTS_CHANNEL,
            "data": serialized_event,
        }

    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.listen.return_value = generate_messages()
    pubsub.__aenter__ = AsyncMock(
        return_value=pubsub,
    )
    pubsub.__aexit__ = AsyncMock(
        return_value=False,
    )

    redis_client = MagicMock(spec=Redis)
    redis_client.pubsub.return_value = pubsub

    event_handler = MagicMock(
        spec=RealtimeEventHandler,
    )
    event_handler.handle = AsyncMock()

    subscriber = RedisRealtimeEventSubscriber(
        redis_client=redis_client,
        event_handler=event_handler,
    )

    async def run_subscriber() -> None:
        subscriber_task = asyncio.create_task(
            subscriber.listen(),
        )

        await subscriber.wait_until_subscribed()
        await subscriber_task

    asyncio.run(run_subscriber())

    redis_client.pubsub.assert_called_once_with()

    pubsub.subscribe.assert_awaited_once_with(
        REALTIME_EVENTS_CHANNEL,
    )

    event_handler.handle.assert_awaited_once_with(
        serialized_event,
    )

    pubsub.__aexit__.assert_awaited_once()