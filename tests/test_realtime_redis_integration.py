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


TEST_REDIS_URL = os.getenv(
    "TEST_REDIS_URL",
    "redis://127.0.0.1:6380/0",
)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)


async def wait_for_replacement_subscription(
    redis_client: Redis,
    subscriber_name: str,
    previous_connection_id: str,
    subscriber: RedisRealtimeEventSubscriber,
    previous_generation: int,
) -> str:
    while True:
        connections = await redis_client.client_list(
            _type="pubsub",
        )

        for connection in connections:
            if (
                connection.get("name") == subscriber_name
                and connection["id"] != previous_connection_id
                and subscriber.is_subscribed
                and (
                    subscriber.subscription_generation
                    > previous_generation
                )
            ):
                return connection["id"]

        # 等待新连接出现，并且应用收到新的订阅确认
        await asyncio.sleep(0.02)


async def wait_for_no_subscribers(
    redis_client: Redis,
    channel: str,
) -> None:
    while True:
        channel_counts = await redis_client.pubsub_numsub(
            channel,
        )

        if channel_counts == [(channel, 0)]:
            return

        await asyncio.sleep(0.02)


@pytest.mark.parametrize(
    "subscriber_total, disconnect_before_publish",
    [
        (1, False),
        (2, False),
        (1, True),
    ],
)
def test_real_redis_delivers_user_event(
    subscriber_total: int,
    disconnect_before_publish: bool,
) -> None:
    async def run_test() -> None:
        test_run_id = uuid4().hex
        channel = f"test:realtime:{test_run_id}"
        subscriber_name = f"test-subscriber:{test_run_id}"
        receivers = []

        reconnect_waiting = asyncio.Event()
        allow_reconnect = asyncio.Event()

        async def controlled_reconnect_sleep(
            _delay_seconds: float,
        ) -> None:
            # 通知测试：订阅器已经进入重连等待阶段
            reconnect_waiting.set()

            # 暂停当前订阅器，直到测试明确允许重连
            await allow_reconnect.wait()

        async with (
            Redis.from_url(
                TEST_REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
                client_name=subscriber_name,
            ) as subscriber_client,
            Redis.from_url(
                TEST_REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
            ) as publisher_client,
        ):
            for _ in range(subscriber_total):
                received_messages: asyncio.Queue[str | bytes] = (
                    asyncio.Queue()
                )

                event_handler = MagicMock(
                    spec=RealtimeEventHandler,
                )
                event_handler.handle = AsyncMock(
                    side_effect=received_messages.put_nowait,
                )

                subscriber = RedisRealtimeEventSubscriber(
                    redis_client=subscriber_client,
                    event_handler=event_handler,
                    channel=channel,
                    sleep=(
                        controlled_reconnect_sleep
                        if disconnect_before_publish
                        else asyncio.sleep
                    ),
                )

                # 每组保存自己的订阅器、接收箱和处理器
                receivers.append(
                    (subscriber, received_messages, event_handler)
                )

            publisher = RedisRealtimeEventPublisher(
                redis_client=publisher_client,
                channel=channel,
            )
            event = RealtimeMessageEvent(
                content="Real Redis message",
            )

            async with asyncio.TaskGroup() as task_group:
                subscriber_tasks = [
                    task_group.create_task(subscriber.listen())
                    for subscriber, _, _ in receivers
                ]

                try:
                    # 所有订阅器都确认就绪后，才发布消息
                    for subscriber, _, _ in receivers:
                        await asyncio.wait_for(
                            subscriber.wait_until_subscribed(),
                            timeout=3.0,
                        )

                    connections = await asyncio.wait_for(
                        publisher_client.client_list(
                            _type="pubsub",
                        ),
                        timeout=3.0,
                    )

                    subscription_connections = [
                        connection
                        for connection in connections
                        if connection.get("name") == subscriber_name
                    ]

                    assert len(subscription_connections) == (
                        subscriber_total
                    )

                    connection_ids = {
                        connection["id"]
                        for connection in subscription_connections
                    }

                    assert len(connection_ids) == subscriber_total

                    if disconnect_before_publish:
                        # 本轮只对单订阅器场景注入断线
                        assert subscriber_total == 1

                        previous_connection_id = (
                            subscription_connections[0]["id"]
                        )

                        subscriber = receivers[0][0]
                        previous_generation = subscriber.subscription_generation

                        disconnected_count = await asyncio.wait_for(
                            publisher_client.client_kill_filter(
                                _id=previous_connection_id,
                            ),
                            timeout=3.0,
                        )
                        assert disconnected_count == 1

                        await asyncio.wait_for(
                            reconnect_waiting.wait(),
                            timeout=3.0,
                        )
                        assert subscriber.is_subscribed is False

                        await asyncio.wait_for(
                            wait_for_no_subscribers(
                                redis_client=publisher_client,
                                channel=channel,
                            ),
                            timeout=3.0,
                        )

                        missed_event = RealtimeMessageEvent(
                            content="Message A during disconnection",
                        )
                        missed_subscriber_count = await asyncio.wait_for(
                            publisher.publish_to_user(
                                user_id=7,
                                event=missed_event,
                            ),
                            timeout=3.0,
                        )
                        assert missed_subscriber_count == 0

                        # 消息 A 发布完成，现在允许订阅器重连
                        allow_reconnect.set()

                        replacement_connection_id = await asyncio.wait_for(
                            wait_for_replacement_subscription(
                                redis_client=publisher_client,
                                subscriber_name=subscriber_name,
                                previous_connection_id=previous_connection_id,
                                subscriber=subscriber,
                                previous_generation=previous_generation,
                            ),
                            timeout=5.0,
                        )

                        assert replacement_connection_id != previous_connection_id

                    subscriber_count = await asyncio.wait_for(
                        publisher.publish_to_user(
                            user_id=7,
                            event=event,
                        ),
                        timeout=3.0,
                    )


                    assert subscriber_count == subscriber_total

                    # 分别验证每一个接收箱，不能只检查总数
                    for _, received_messages, event_handler in receivers:
                        serialized_event = await asyncio.wait_for(
                            received_messages.get(),
                            timeout=3.0,
                        )

                        envelope = (
                            UserRealtimeEventEnvelope
                            .model_validate_json(serialized_event)
                        )

                        assert envelope == UserRealtimeEventEnvelope(
                            user_id=7,
                            event=event,
                        )
                        event_handler.handle.assert_awaited_once()
                finally:
                    for subscriber_task in subscriber_tasks:
                        subscriber_task.cancel()

            for subscriber, _, _ in receivers:
                assert subscriber.is_subscribed is False

    asyncio.run(run_test())