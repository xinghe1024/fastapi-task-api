import asyncio
import os
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from realtime_broker import (
    RedisRealtimeEventPublisher,
    RedisRealtimeEventSubscriber,
)
from realtime_event_handler import RealtimeEventHandler
from realtime_models import (
    RealtimeMessageEvent,
    UserRealtimeEventEnvelope,
)


TEST_REDIS_URL = "redis://127.0.0.1:6380/0"

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)


def test_real_redis_delivers_user_event() -> None:
    async def run_test() -> None:
        channel = f"test:realtime:{uuid4().hex}"
        received_messages: asyncio.Queue[str | bytes] = (
            asyncio.Queue()
        )

        # 只模拟末端处理器，不模拟 Redis 的发布和订阅
        event_handler = MagicMock(spec=RealtimeEventHandler)
        event_handler.handle = AsyncMock(
            side_effect=received_messages.put_nowait,
        )

        async with (
            Redis.from_url(
                TEST_REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
            ) as subscriber_client,
            Redis.from_url(
                TEST_REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
            ) as publisher_client,
        ):
            subscriber = RedisRealtimeEventSubscriber(
                redis_client=subscriber_client,
                event_handler=event_handler,
                channel=channel,
            )
            publisher = RedisRealtimeEventPublisher(
                redis_client=publisher_client,
                channel=channel,
            )

            event = RealtimeMessageEvent(
                content="Real Redis message",
            )

            async with asyncio.TaskGroup() as task_group:
                subscriber_task = task_group.create_task(
                    subscriber.listen(),
                )

                try:
                    await asyncio.wait_for(
                        subscriber.wait_until_subscribed(),
                        timeout=3.0,
                    )

                    subscriber_count = await asyncio.wait_for(
                        publisher.publish_to_user(
                            user_id=7,
                            event=event,
                        ),
                        timeout=3.0,
                    )

                    serialized_event = await asyncio.wait_for(
                        received_messages.get(),
                        timeout=3.0,
                    )

                    envelope = (
                        UserRealtimeEventEnvelope
                        .model_validate_json(serialized_event)
                    )

                    assert subscriber_count == 1
                    assert envelope == UserRealtimeEventEnvelope(
                        user_id=7,
                        event=event,
                    )
                    event_handler.handle.assert_awaited_once()
                finally:
                    subscriber_task.cancel()

            assert subscriber.is_subscribed is False

    asyncio.run(run_test())