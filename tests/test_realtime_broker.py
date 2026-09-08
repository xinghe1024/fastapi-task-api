import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, call

from redis.asyncio import Redis
from realtime_broker import RedisRealtimeEventSubscriber
from realtime_event_handler import RealtimeEventHandler
from realtime_models import RealtimeMessageEvent

from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError as RedisConnectionError,
    DataError,
    RedisError,
    ResponseError,
)
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
        await asyncio.Future()

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
        message_handled = asyncio.Event()

        async def mark_message_handled(
                _: str | bytes,
        ) -> None:
            message_handled.set()

        event_handler.handle.side_effect = (
            mark_message_handled
        )

        subscriber_task = asyncio.create_task(
            subscriber.listen(),
        )

        try:
            await asyncio.wait_for(
                subscriber.wait_until_subscribed(),
                timeout=1.0,
            )

            await asyncio.wait_for(
                message_handled.wait(),
                timeout=1.0,
            )
        finally:
            subscriber_task.cancel()

            try:
                await subscriber_task
            except asyncio.CancelledError:
                pass

    asyncio.run(run_subscriber())

    redis_client.pubsub.assert_called_once_with()

    pubsub.subscribe.assert_awaited_once_with(
        REALTIME_EVENTS_CHANNEL,
    )

    event_handler.handle.assert_awaited_once_with(
        serialized_event,
    )

    pubsub.__aexit__.assert_awaited_once()


def test_realtime_subscriber_fails_fast_before_first_subscription(
) -> None:
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock(
        side_effect=RedisConnectionError(
            "Redis is unavailable",
        ),
    )
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

    with pytest.raises(
        RedisConnectionError,
        match="Redis is unavailable",
    ):
        asyncio.run(subscriber.listen())

    assert subscriber.is_subscribed is False


def test_realtime_subscriber_reconnects_after_runtime_error(
) -> None:
    serialized_event = (
        '{"user_id":7,'
        '"event":{'
        '"type":"message",'
        '"content":"reconnected"'
        '}}'
    )

    async def run_test() -> None:
        message_handled = asyncio.Event()

        async def disconnected_messages():
            yield {
                "type": "subscribe",
                "channel": REALTIME_EVENTS_CHANNEL,
                "data": 1,
            }

            raise RedisConnectionError("Connection lost")

        async def reconnected_messages():
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

            # 模拟重新订阅后继续等待消息
            await asyncio.Future()

        first_pubsub = MagicMock()
        first_pubsub.subscribe = AsyncMock()
        first_pubsub.listen.return_value = (
            disconnected_messages()
        )
        first_pubsub.__aenter__ = AsyncMock(
            return_value=first_pubsub,
        )
        first_pubsub.__aexit__ = AsyncMock(
            return_value=False,
        )

        second_pubsub = MagicMock()
        second_pubsub.subscribe = AsyncMock()
        second_pubsub.listen.return_value = (
            reconnected_messages()
        )
        second_pubsub.__aenter__ = AsyncMock(
            return_value=second_pubsub,
        )
        second_pubsub.__aexit__ = AsyncMock(
            return_value=False,
        )

        redis_client = MagicMock(spec=Redis)
        redis_client.pubsub.side_effect = [
            first_pubsub,
            second_pubsub,
        ]

        async def handle_message(
            _: str | bytes,
        ) -> None:
            message_handled.set()

        event_handler = MagicMock(
            spec=RealtimeEventHandler,
        )
        event_handler.handle = AsyncMock(
            side_effect=handle_message,
        )

        sleep = AsyncMock()
        jitter = MagicMock(return_value=0.25)

        subscriber = RedisRealtimeEventSubscriber(
            redis_client=redis_client,
            event_handler=event_handler,
            reconnect_delay_seconds=1.0,
            sleep=sleep,
            jitter=jitter,
        )

        subscriber_task = asyncio.create_task(
            subscriber.listen(),
        )

        await asyncio.wait_for(
            message_handled.wait(),
            timeout=1.0,
        )

        subscriber_task.cancel()

        try:
            await subscriber_task
        except asyncio.CancelledError:
            pass

        assert redis_client.pubsub.call_count == 2

        jitter.assert_called_once_with(0.0, 1.0)
        sleep.assert_awaited_once_with(0.25)

        event_handler.handle.assert_awaited_once_with(
            serialized_event,
        )

        assert subscriber.is_subscribed is False

    asyncio.run(run_test())


def test_realtime_subscriber_waits_for_subscription_confirmation(
) -> None:
    async def run_test() -> None:
        listening_started = asyncio.Event()
        allow_confirmation = asyncio.Event()

        async def generate_messages():
            # 告诉测试：已经开始读取，但尚未返回订阅确认
            listening_started.set()
            await allow_confirmation.wait()

            yield {
                "type": "subscribe",
                "channel": REALTIME_EVENTS_CHANNEL,
                "data": 1,
            }

            # 保持监听，直到测试主动取消
            await asyncio.Future()

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

        subscriber_task = asyncio.create_task(
            subscriber.listen(),
        )

        try:
            await asyncio.wait_for(
                listening_started.wait(),
                timeout=1.0,
            )

            assert subscriber.is_subscribed is False

            # 允许模拟的 Redis 返回订阅确认
            allow_confirmation.set()

            await asyncio.wait_for(
                subscriber.wait_until_subscribed(),
                timeout=1.0,
            )

            assert subscriber.is_subscribed is True
            event_handler.handle.assert_not_awaited()
        finally:
            subscriber_task.cancel()

            try:
                await subscriber_task
            except asyncio.CancelledError:
                pass

        assert subscriber.is_subscribed is False
        pubsub.__aexit__.assert_awaited_once()

    asyncio.run(run_test())


@pytest.mark.parametrize(
    "exception_type",
    [
        DataError,
        ResponseError,
        AuthenticationError,
        AuthorizationError,
    ],
)
def test_realtime_subscriber_does_not_retry_unrecoverable_error(
    exception_type: type[RedisError],
) -> None:
    async def run_test() -> None:
        expected_error = exception_type(
            "Unrecoverable subscription error",
        )

        async def generate_messages():
            yield {
                "type": "subscribe",
                "channel": REALTIME_EVENTS_CHANNEL,
                "data": 1,
            }

            # 确保测试进入“曾经订阅成功”的状态
            assert subscriber.is_subscribed is True

            raise expected_error

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

        sleep = AsyncMock(
            side_effect=AssertionError(
                "不可恢复错误不应进入重连等待",
            ),
        )

        subscriber = RedisRealtimeEventSubscriber(
            redis_client=redis_client,
            event_handler=event_handler,
            sleep=sleep,
        )

        with pytest.raises(exception_type) as exception_info:
            await subscriber.listen()

        assert exception_info.value is expected_error
        sleep.assert_not_awaited()
        redis_client.pubsub.assert_called_once_with()
        pubsub.__aexit__.assert_awaited_once()
        event_handler.handle.assert_not_awaited()
        assert subscriber.is_subscribed is False

    asyncio.run(run_test())


@pytest.mark.parametrize(
    "subscription_results, expected_maximum_delays",
    [
        (
            [
                None,
                RedisConnectionError("Still offline"),
                RedisConnectionError("Still offline"),
                RedisConnectionError("Still offline"),
            ],
            [1.0, 2.0, 4.0, 4.0],
        ),
        (
            [
                None,
                RedisConnectionError("Still offline"),
                None,
                RedisConnectionError("Still offline"),
            ],
            [1.0, 2.0, 1.0, 2.0],
        ),
    ],
)
def test_realtime_subscriber_increases_reconnect_delay(
    subscription_results: list[RedisConnectionError | None],
    expected_maximum_delays: list[float],
) -> None:
    async def run_test() -> None:
        async def confirmed_then_disconnected():
            yield {
                "type": "subscribe",
                "channel": REALTIME_EVENTS_CHANNEL,
                "data": 1,
            }

            raise RedisConnectionError("Connection lost")

        pubsub = MagicMock()
        pubsub.__aenter__ = AsyncMock(return_value=pubsub)
        pubsub.__aexit__ = AsyncMock(return_value=False)


        # 每组参数决定四轮订阅分别成功还是失败
        pubsub.subscribe = AsyncMock(
            side_effect=subscription_results,
        )

        # 每次开始监听，都创建一个新的消息生成器
        pubsub.listen.side_effect = (
            confirmed_then_disconnected
        )

        redis_client = MagicMock(spec=Redis)
        redis_client.pubsub.return_value = pubsub

        event_handler = MagicMock(spec=RealtimeEventHandler)
        event_handler.handle = AsyncMock()

        jitter = MagicMock(
            side_effect=[0.2, 0.6, 0.3, 0.8],
        )

        # 不真正等待；第四次进入等待时，模拟任务取消
        sleep = AsyncMock(
            side_effect=[
                None,
                None,
                None,
                asyncio.CancelledError(),
            ],
        )

        subscriber = RedisRealtimeEventSubscriber(
            redis_client=redis_client,
            event_handler=event_handler,
            reconnect_delay_seconds=1.0,
            maximum_reconnect_delay_seconds=4.0,
            sleep=sleep,
            jitter=jitter,
        )

        with pytest.raises(asyncio.CancelledError):
            await subscriber.listen()

        assert jitter.call_args_list == [
            call(0.0, maximum_delay)
            for maximum_delay in expected_maximum_delays
        ]

        assert sleep.await_args_list == [
            call(0.2),
            call(0.6),
            call(0.3),
            call(0.8),
        ]

        assert redis_client.pubsub.call_count == 4
        assert pubsub.subscribe.await_count == 4
        assert pubsub.__aexit__.await_count == 4

        event_handler.handle.assert_not_awaited()
        assert subscriber.is_subscribed is False

    asyncio.run(run_test())