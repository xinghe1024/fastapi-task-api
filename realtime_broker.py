import asyncio
import logging


from collections.abc import Awaitable, Callable
from random import uniform
from reconnect_backoff import ReconnectBackoff


from redis.asyncio import Redis
from realtime_event_handler import RealtimeEventHandler
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from realtime_models import (
    RealtimeMessageEvent,
    UserRealtimeEventEnvelope,
)

REALTIME_EVENTS_CHANNEL = "realtime:user-events"

logger = logging.getLogger(__name__)

SleepFunction = Callable[
    [float],
    Awaitable[None],
]

class RedisRealtimeEventPublisher:
    def __init__(
        self,
        redis_client: Redis,
        channel: str = REALTIME_EVENTS_CHANNEL,
    ) -> None:
        if not channel:
            raise ValueError("channel cannot be empty")

        self._redis_client = redis_client
        self._channel = channel

    async def publish_to_user(
        self,
        user_id: int,
        event: RealtimeMessageEvent,
    ) -> int:
        envelope = UserRealtimeEventEnvelope(
            user_id=user_id,
            event=event,
        )

        subscriber_count = await self._redis_client.publish(
            self._channel,
            envelope.model_dump_json(),
        )

        return int(subscriber_count)


class RedisRealtimeEventSubscriber:
    def __init__(
            self,
            redis_client: Redis,
            event_handler: RealtimeEventHandler,
            channel: str = REALTIME_EVENTS_CHANNEL,
            reconnect_delay_seconds: float = 1.0,
            sleep: SleepFunction = asyncio.sleep,
            maximum_reconnect_delay_seconds: float = 30.0,
            jitter: Callable[[float, float], float] = uniform,
    ) -> None:
        if not channel:
            raise ValueError("channel cannot be empty")

        self._redis_client = redis_client
        self._event_handler = event_handler
        self._channel = channel
        self._sleep = sleep
        self._jitter = jitter

        self._backoff = ReconnectBackoff(
            initial_delay=reconnect_delay_seconds,
            maximum_delay=maximum_reconnect_delay_seconds,
        )

        self._subscribed_event = asyncio.Event()
        self._has_subscribed_once = False
        self._subscription_generation = 0

    @property
    def is_subscribed(self) -> bool:
        return self._subscribed_event.is_set()

    @property
    def subscription_generation(self) -> int:
        return self._subscription_generation

    async def listen(self) -> None:
        while True:
            try:
                await self._listen_once()
            except (AuthenticationError, AuthorizationError):
                # 凭证或权限错误需要修正配置
                raise
            except (RedisConnectionError, RedisTimeoutError):
                if not self._has_subscribed_once:
                    raise

                logger.exception(
                    "Redis实时订阅断开，准备重新连接",
                )
            finally:
                self._subscribed_event.clear()

            maximum_delay = self._backoff.next_delay()
            delay = self._jitter(0.0, maximum_delay)

            await self._sleep(delay)

    async def _listen_once(self) -> None:
        async with self._redis_client.pubsub() as pubsub:
            await pubsub.subscribe(self._channel)

            async for message in pubsub.listen():
                message_type = message.get("type")
                channel = message.get("channel")

                if (
                        message_type in ("subscribe", b"subscribe")
                        and channel in (
                        self._channel,
                        self._channel.encode("utf-8"),
                )
                ):
                    # 收到目标频道的订阅确认后，才标记就绪
                    self._backoff.reset()
                    self._has_subscribed_once = True
                    self._subscription_generation += 1
                    self._subscribed_event.set()
                    continue

                if message_type not in ("message", b"message"):
                    continue

                serialized_event = message.get("data")

                if not isinstance(
                        serialized_event,
                        (str, bytes),
                ):
                    continue

                await self._event_handler.handle(
                    serialized_event,
                )

    async def wait_until_subscribed(self) -> None:
        await self._subscribed_event.wait()


