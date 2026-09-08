import asyncio
import pytest
import main

from unittest.mock import AsyncMock, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from main import app

class StubRealtimeEventSubscriber:
    def __init__(
        self,
        **_: object,
    ) -> None:
        pass

    async def listen(self) -> None:
        # 模拟一个持续运行的订阅任务
        await asyncio.Future()

    async def wait_until_subscribed(self) -> None:
        # 当前测试不验证 Redis，只模拟订阅已经就绪
        return None


def test_http_client_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "main.RedisRealtimeEventSubscriber",
        StubRealtimeEventSubscriber,
    )

    with TestClient(app):
        http_client = app.state.http_client

        assert http_client.is_closed is False

    assert http_client.is_closed is True


def test_lifespan_cleans_up_after_subscription_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run_test() -> None:
        listener_started = asyncio.Event()
        listener_stopped = asyncio.Event()

        async def listen_until_cancelled() -> None:
            listener_started.set()

            try:
                await asyncio.Future()
            finally:
                listener_stopped.set()

        async def never_confirm_subscription() -> None:
            # 先确认监听任务已经开始，再持续等待订阅确认
            await listener_started.wait()
            await asyncio.Future()

        subscriber = MagicMock()
        subscriber.listen = AsyncMock(
            side_effect=listen_until_cancelled,
        )
        subscriber.wait_until_subscribed = AsyncMock(
            side_effect=never_confirm_subscription,
        )

        redis_client = MagicMock()
        redis_client.aclose = AsyncMock()

        engine = MagicMock()

        monkeypatch.setattr(
            main,
            "RedisRealtimeEventSubscriber",
            MagicMock(return_value=subscriber),
        )
        monkeypatch.setattr(
            main.Redis,
            "from_url",
            MagicMock(return_value=redis_client),
        )
        monkeypatch.setattr(main, "engine", engine)
        monkeypatch.setattr(
            main,
            "REALTIME_SUBSCRIPTION_TIMEOUT_SECONDS",
            0.01,
        )

        with pytest.raises(ExceptionGroup) as exception_info:
            async with main.lifespan(FastAPI()):
                pytest.fail(
                    "未确认订阅时，不应该完成启动",
                )

        errors = exception_info.value.exceptions

        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)

        assert listener_started.is_set()
        assert listener_stopped.is_set()

        subscriber.listen.assert_awaited_once()
        subscriber.wait_until_subscribed.assert_awaited_once()
        redis_client.aclose.assert_awaited_once()
        engine.dispose.assert_called_once()

    asyncio.run(run_test())