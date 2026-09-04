import asyncio

import pytest
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