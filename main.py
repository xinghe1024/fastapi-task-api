import httpx

from asyncio import TaskGroup
from connection_manager import ConnectionManager
from config import get_settings
from circuit_breaker import CircuitBreaker
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from database import engine

from fastapi import FastAPI

from routers.tasks import router as task_router
from routers.auth import router as auth_router
from routers.health import router as health_router
from routers.realtime import router as realtime_router
from redis.asyncio import Redis
from routers.external import router as external_router
from rate_limiting import FixedWindowRateLimiter
from realtime_event_handler import RealtimeEventHandler

from middlewares import register_middlewares


from exception_handlers import (
    register_exception_handlers,
)
from realtime_broker import (
    RedisRealtimeEventPublisher,
    RedisRealtimeEventSubscriber,
)


@asynccontextmanager
async def lifespan(
    app: FastAPI,
) -> AsyncGenerator[None]:
    settings = get_settings()

    redis_client = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
    )

    connection_manager = ConnectionManager()
    app.state.connection_manager = connection_manager

    realtime_event_handler = RealtimeEventHandler(
        connection_manager=connection_manager,
    )

    realtime_subscriber = RedisRealtimeEventSubscriber(
        redis_client=redis_client,
        event_handler=realtime_event_handler,
    )

    app.state.redis_client = redis_client

    realtime_event_publisher = (
        RedisRealtimeEventPublisher(
            redis_client=redis_client,
        )
    )

    app.state.realtime_event_publisher = (
        realtime_event_publisher
    )

    app.state.fallback_login_rate_limiter = (
        FixedWindowRateLimiter(
            request_limit=settings.login_rate_limit,
            window_seconds=(
                settings.login_rate_window_seconds
            ),
        )
    )

    app.state.external_service_circuit_breaker = (
        CircuitBreaker(
            failure_threshold=(
                settings.external_service_failure_threshold
            ),
            recovery_timeout=(
                settings.external_service_recovery_timeout
            ),
        )
    )

    timeout = httpx.Timeout(
        connect=3.0,
        read=5.0,
        write=5.0,
        pool=2.0,
    )

    try:
        async with TaskGroup() as task_group:
            subscriber_task = task_group.create_task(
                realtime_subscriber.listen(),
                name="redis-realtime-subscriber",
            )

            await realtime_subscriber.wait_until_subscribed()

            try:
                async with httpx.AsyncClient(
                        timeout=timeout,
                ) as http_client:
                    app.state.http_client = http_client
                    yield
            finally:
                subscriber_task.cancel()
    finally:
        await redis_client.aclose()
        engine.dispose()


app = FastAPI(lifespan=lifespan)

register_exception_handlers(app)
register_middlewares(
    app,
    get_settings().cors_allowed_origins,
)
app.include_router(task_router)
app.include_router(auth_router)
app.include_router(health_router)
app.include_router(external_router)
app.include_router(realtime_router)
