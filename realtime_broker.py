from asyncio import Event

from redis.asyncio import Redis
from realtime_event_handler import RealtimeEventHandler

from realtime_models import (
    RealtimeMessageEvent,
    UserRealtimeEventEnvelope,
)


REALTIME_EVENTS_CHANNEL = "realtime:user-events"


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
    ) -> None:
        if not channel:
            raise ValueError("channel cannot be empty")

        self._subscribed_event = Event()
        self._redis_client = redis_client
        self._event_handler = event_handler
        self._channel = channel

    async def listen(self) -> None:
        async with self._redis_client.pubsub() as pubsub:
            await pubsub.subscribe(self._channel)

            self._subscribed_event.set()

            async for message in pubsub.listen():
                if message.get("type") != "message":
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


