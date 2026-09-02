from redis.asyncio import Redis

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