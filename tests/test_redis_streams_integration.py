import asyncio
import os
from uuid import uuid4

import pytest
from redis.asyncio import Redis


TEST_REDIS_URL = os.getenv(
    "TEST_REDIS_URL",
    "redis://127.0.0.1:6380/0",
)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)


def test_stream_reads_stored_messages_from_last_id() -> None:
    async def run_test() -> None:
        stream_key = f"test:streams:{uuid4().hex}"
        first_payload = {"content": "Message A"}
        second_payload = {"content": "Message B"}

        async with Redis.from_url(
            TEST_REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                # 先写入两条消息，此时还没有执行读取
                first_id = await redis_client.xadd(
                    name=stream_key,
                    fields=first_payload,
                    id="*",
                )
                second_id = await redis_client.xadd(
                    name=stream_key,
                    fields=second_payload,
                    id="*",
                )

                first_result = await redis_client.xread(
                    streams={stream_key: "0-0"},
                    count=1,
                )
                assert first_result == [
                    [stream_key, [(first_id, first_payload)]],
                ]

                # A 已确认读到，继续读取 A 之后的消息
                second_result = await redis_client.xread(
                    streams={stream_key: first_id},
                    count=1,
                )
                assert second_result == [
                    [stream_key, [(second_id, second_payload)]],
                ]
            finally:
                # 只删除本次测试创建的随机 Stream
                await redis_client.delete(stream_key)

    asyncio.run(run_test())


def test_consumer_group_acknowledges_without_deleting_message() -> None:
    async def run_test() -> None:
        stream_key = f"test:streams:{uuid4().hex}"
        group_name = "task-workers"
        consumer_name = "worker-a"
        payload = {"content": "Message A"}

        async with Redis.from_url(
            TEST_REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                message_id = await redis_client.xadd(
                    name=stream_key,
                    fields=payload,
                    id="*",
                )

                # 从起点创建组，允许领取已经写入的消息
                await redis_client.xgroup_create(
                    name=stream_key,
                    groupname=group_name,
                    id="0-0",
                )

                pending_before_read = await redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_before_read["pending"] == 0

                messages = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=1,
                )
                assert messages == [
                    [stream_key, [(message_id, payload)]],
                ]

                pending_after_read = await redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_read["pending"] == 1
                assert pending_after_read["min"] == message_id
                assert pending_after_read["max"] == message_id
                assert pending_after_read["consumers"] == [
                    {"name": consumer_name, "pending": 1},
                ]

                # A 已经领取，不会再次作为新消息投递
                new_messages = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=1,
                )
                assert new_messages == []

                # 使用同一个消费者名字，重新读取自己的待确认消息
                pending_messages = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: "0-0"},
                    count=1,
                )
                assert pending_messages == [
                    [stream_key, [(message_id, payload)]],
                ]

                # 再次读取同一条消息，不会多出一条待确认记录
                pending_after_reread = await redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_reread["pending"] == 1

                # 本例只测试确认机制，不执行真实业务
                acknowledged_count = await redis_client.xack(
                    stream_key,
                    group_name,
                    message_id,
                )
                assert acknowledged_count == 1

                pending_after_ack = await redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_ack["pending"] == 0

                remaining_pending_messages = (
                    await redis_client.xreadgroup(
                        groupname=group_name,
                        consumername=consumer_name,
                        streams={stream_key: "0-0"},
                        count=1,
                    )
                )
                assert remaining_pending_messages == [
                    [stream_key, []],
                ]

                # 确认完成后，消息正文仍然可以被普通读取
                stored_messages = await redis_client.xread(
                    streams={stream_key: "0-0"},
                    count=1,
                )
                assert stored_messages == [
                    [stream_key, [(message_id, payload)]],
                ]
            finally:
                # 只清理本次测试创建的 Stream 及其消费组
                await redis_client.delete(stream_key)

    asyncio.run(run_test())


def test_other_consumer_claims_pending_message() -> None:
    async def run_test() -> None:
        stream_key = f"test:streams:{uuid4().hex}"
        group_name = "task-workers"
        original_consumer = "worker-a"
        recovery_consumer = "worker-b"
        payload = {"content": "Message A"}

        async with Redis.from_url(
            TEST_REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                message_id = await redis_client.xadd(
                    name=stream_key,
                    fields=payload,
                    id="*",
                )
                await redis_client.xgroup_create(
                    name=stream_key,
                    groupname=group_name,
                    id="0-0",
                )

                # worker-a 领取消息，但故意不确认
                original_messages = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=original_consumer,
                    streams={stream_key: ">"},
                    count=1,
                )
                assert original_messages == [
                    [stream_key, [(message_id, payload)]],
                ]

                # 接管前，worker-b 无法通过自己的待确认列表读取 A
                pending_before_claim = await redis_client.xreadgroup(
                    groupname=group_name,
                    consumername=recovery_consumer,
                    streams={stream_key: "0-0"},
                    count=1,
                )
                assert pending_before_claim == [
                    [stream_key, []],
                ]

                # 仅在独立测试中使用 0，避免真实等待
                claim_result = await redis_client.xautoclaim(
                    name=stream_key,
                    groupname=group_name,
                    consumername=recovery_consumer,
                    min_idle_time=0,
                    start_id="0-0",
                    count=1,
                )
                (
                    next_cursor,
                    claimed_messages,
                    deleted_message_ids,
                ) = claim_result

                assert next_cursor == "0-0"
                assert claimed_messages == [
                    (message_id, payload),
                ]
                assert deleted_message_ids == []

                # 接管改变归属，但消息仍然处于待确认状态
                pending_after_claim = await redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_claim["pending"] == 1
                assert pending_after_claim["consumers"] == [
                    {"name": recovery_consumer, "pending": 1},
                ]

                # 本例只验证机制，不执行真实业务
                acknowledged_count = await redis_client.xack(
                    stream_key,
                    group_name,
                    message_id,
                )
                assert acknowledged_count == 1

                pending_after_ack = await redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_ack["pending"] == 0
            finally:
                # 只清理本次测试创建的 Stream
                await redis_client.delete(stream_key)

    asyncio.run(run_test())