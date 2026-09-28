import pytest
import os
import json

from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
from sqlalchemy import URL, Engine, create_engine, text
from pathlib import Path
from typing import Literal
from threading import Barrier
from unittest.mock import Mock

from redis.exceptions import ConnectionError as RedisConnectionError
from redis import Redis as SyncRedis


class EventPayloadConflictError(ValueError):
    """同一业务事件编号对应了不同的业务内容。"""


class RetryableMessageError(RuntimeError):
    """明确允许有限重试的消息处理故障。"""


def should_retry_message(
    failed_attempts: int,
    max_attempts: int = 3,
) -> bool:
    """判断可重试故障发生后，是否还有剩余尝试次数。"""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    if failed_attempts < 1:
        raise ValueError("failed_attempts must be at least 1")

    return failed_attempts < max_attempts


def decide_message_failure(
    error: Exception,
    failed_attempts: int,
    max_attempts: int = 3,
) -> Literal["retry", "dead_letter"]:
    """根据故障类型和失败次数，决定后续处理方向。"""
    if isinstance(error, EventPayloadConflictError):
        return "dead_letter"

    if isinstance(error, RetryableMessageError):
        if should_retry_message(
            failed_attempts=failed_attempts,
            max_attempts=max_attempts,
        ):
            return "retry"

        return "dead_letter"

    # 未知错误继续向上抛出，不擅自重试或隔离消息。
    raise error


WRITE_DEAD_LETTER_ONCE_SCRIPT = """
local existing_id = redis.call("HGET", KEYS[2], ARGV[1])
if existing_id then
    return existing_id
end

local dead_letter_id = redis.call(
    "XADD", KEYS[1], "*",
    "source_stream", ARGV[2],
    "source_group", ARGV[3],
    "source_message_id", ARGV[4],
    "payload", ARGV[5],
    "reason", ARGV[6]
)

redis.call("HSET", KEYS[2], ARGV[1], dead_letter_id)
return dead_letter_id
"""


def apply_completion_once(
    engine: Engine,
    event_id: str,
    increment_by: int = 1,
) -> bool:
    with engine.begin() as connection:
        inserted_record = connection.execute(
            text(
                "INSERT INTO processed_events (event_id, increment_by) "
                "VALUES (:event_id, :increment_by) "
                "ON CONFLICT (event_id) DO NOTHING"
            ),
            {
                "event_id": event_id,
                "increment_by": increment_by,
            },
        )

        if inserted_record.rowcount == 0:
            # 编号已存在，还必须核对业务内容
            stored_increment = connection.execute(
                text(
                    "SELECT increment_by FROM processed_events "
                    "WHERE event_id = :event_id"
                ),
                {"event_id": event_id},
            ).scalar_one()

            if stored_increment != increment_by:
                raise EventPayloadConflictError(
                    "Event payload conflicts with processed event"
                )

            # 编号和业务内容都相同，才是正常重复
            return False

        updated_counter = connection.execute(
            text(
                "UPDATE completion_counter "
                "SET completed_count = completed_count + :increment_by "
                "WHERE id = 1"
            ),
            {"increment_by": increment_by},
        )

        if updated_counter.rowcount != 1:
            raise RuntimeError("Completion counter is missing")

    return True


def initialize_counter_database(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE processed_events ("
                "event_id TEXT NOT NULL PRIMARY KEY, "
                "increment_by INTEGER NOT NULL)"
            ),
        )
        connection.execute(
            text(
                "CREATE TABLE completion_counter ("
                "id INTEGER PRIMARY KEY, "
                "completed_count INTEGER NOT NULL)"
            ),
        )
        connection.execute(
            text(
                "INSERT INTO completion_counter "
                "(id, completed_count) VALUES (1, 0)"
            ),
        )


def process_completion_and_acknowledge(
    engine: Engine,
    redis_client: SyncRedis,
    stream_key: str,
    group_name: str,
    message_id: str,
    event_id: str,
    increment_by: int = 1,
) -> tuple[bool, int]:
    # 必须把消息对应的增加量传给业务处理函数
    applied = apply_completion_once(
        engine,
        event_id,
        increment_by=increment_by,
    )

    # 业务成功或确认是正常重复后，才确认这条消息
    acknowledged_count = redis_client.xack(
        stream_key,
        group_name,
        message_id,
    )

    return applied, acknowledged_count


def write_dead_letter(
    redis_client: SyncRedis,
    dead_letter_stream: str,
    source_stream: str,
    source_group: str,
    source_message_id: str,
    payload: dict[str, str],
    reason: str,
) -> str:
    # 只负责保存问题消息，不确认原消息，不修改业务数据
    return redis_client.xadd(
        dead_letter_stream,
        {
            "source_stream": source_stream,
            "source_group": source_group,
            "source_message_id": source_message_id,
            "payload": json.dumps(payload, ensure_ascii=False),
            "reason": reason,
        },
    )


def write_dead_letter_once(
    redis_client: SyncRedis,
    dead_letter_stream: str,
    source_stream: str,
    source_group: str,
    source_message_id: str,
    payload: dict[str, str],
    reason: str,
) -> str:
    deduplication_key = f"{dead_letter_stream}:dedup"

    # 用 JSON 数组保留三个身份组成部分的明确边界
    message_identity = json.dumps(
        [source_stream, source_group, source_message_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return redis_client.eval(
        WRITE_DEAD_LETTER_ONCE_SCRIPT,
        2,
        dead_letter_stream,
        deduplication_key,
        message_identity,
        source_stream,
        source_group,
        source_message_id,
        json.dumps(payload, ensure_ascii=False),
        reason,
    )


def archive_message_and_acknowledge(
    redis_client: SyncRedis,
    dead_letter_stream: str,
    source_stream: str,
    source_group: str,
    source_message_id: str,
    payload: dict[str, str],
    reason: str,
) -> tuple[str, int]:
    # 先保存排查依据；失败时异常会直接向上传递
    dead_letter_id = write_dead_letter_once(
        redis_client,
        dead_letter_stream=dead_letter_stream,
        source_stream=source_stream,
        source_group=source_group,
        source_message_id=source_message_id,
        payload=payload,
        reason=reason,
    )

    # 只有保存成功，才会执行到这里
    acknowledged_count = redis_client.xack(
        source_stream,
        source_group,
        source_message_id,
    )

    return dead_letter_id, acknowledged_count


def handle_message_failure(
    redis_client: SyncRedis,
    dead_letter_stream: str,
    source_stream: str,
    source_group: str,
    source_message_id: str,
    payload: dict[str, str],
    error: Exception,
    failed_attempts: int,
    max_attempts: int = 3,
) -> Literal["retry", "dead_letter"]:
    """执行失败处理决定，不负责计数或重新执行业务。"""
    action = decide_message_failure(
        error=error,
        failed_attempts=failed_attempts,
        max_attempts=max_attempts,
    )

    if action == "retry":
        # 不确认消息，让它继续保留在待确认列表中。
        return "retry"

    archive_message_and_acknowledge(
        redis_client=redis_client,
        dead_letter_stream=dead_letter_stream,
        source_stream=source_stream,
        source_group=source_group,
        source_message_id=source_message_id,
        payload=payload,
        reason=type(error).__name__,
    )

    return "dead_letter"


def process_completion_with_failure_policy(
    engine: Engine,
    redis_client: SyncRedis,
    dead_letter_stream: str,
    source_stream: str,
    source_group: str,
    source_message_id: str,
    payload: dict[str, str],
    failed_attempts: int,
    max_attempts: int = 3,
) -> Literal["applied", "duplicate", "retry", "dead_letter"]:
    """执行业务，并将已识别的业务故障交给失败处理入口。"""
    event_id = payload["event_id"]
    increment_by = int(payload["increment_by"])

    try:
        applied = apply_completion_once(
            engine,
            event_id,
            increment_by=increment_by,
        )
    except (EventPayloadConflictError, RetryableMessageError) as error:
        return handle_message_failure(
            redis_client=redis_client,
            dead_letter_stream=dead_letter_stream,
            source_stream=source_stream,
            source_group=source_group,
            source_message_id=source_message_id,
            payload=payload,
            error=error,
            failed_attempts=failed_attempts,
            max_attempts=max_attempts,
        )

    # 确认失败属于确认阶段故障，不送入业务失败分类。
    redis_client.xack(
        source_stream,
        source_group,
        source_message_id,
    )

    return "applied" if applied else "duplicate"


def test_duplicate_message_increments_counter_only_once() -> None:
    engine = create_engine("sqlite:///:memory:")

    try:
        # 准备独立的测试表与初始计数
        initialize_counter_database(engine)

        # 模拟同一条消息被处理两次
        first_applied = apply_completion_once(engine, "event-a")
        second_applied = apply_completion_once(engine, "event-a")

        assert first_applied is True
        assert second_applied is False

        # 重新查询数据库，验证实际保存的结果
        with engine.connect() as connection:
            completed_count = connection.execute(
                text(
                    "SELECT completed_count "
                    "FROM completion_counter WHERE id = 1"
                ),
            ).scalar_one()

            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_events"),
            ).scalar_one()

        assert completed_count == 1
        assert processed_count == 1
    finally:
        engine.dispose()


def test_failed_message_can_be_retried_after_rollback() -> None:
    engine = create_engine("sqlite:///:memory:")
    event_id = "event-a"

    try:
        initialize_counter_database(engine)

        # 故意移除业务目标，让后续更新失败
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM completion_counter WHERE id = 1"),
            )

        # 必须出现预期的异常类型和错误信息
        with pytest.raises(
            RuntimeError,
            match="^Completion counter is missing$",
        ):
            apply_completion_once(engine, event_id)

        # 抛出异常还不够，必须确认处理记录没有残留
        with engine.connect() as connection:
            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_events"),
            ).scalar_one()

        assert processed_count == 0

        # 修复业务条件，重新建立计数记录
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO completion_counter "
                    "(id, completed_count) VALUES (1, 0)"
                ),
            )

        # 使用相同消息编号重试，不能换成另一条消息
        retry_applied = apply_completion_once(engine, event_id)

        assert retry_applied is True

        with engine.connect() as connection:
            completed_count = connection.execute(
                text(
                    "SELECT completed_count "
                    "FROM completion_counter WHERE id = 1"
                ),
            ).scalar_one()

            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_events"),
            ).scalar_one()

        assert completed_count == 1
        assert processed_count == 1
    finally:
        engine.dispose()


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_claimed_message_is_acknowledged_without_repeating_business() -> None:
    engine = create_engine("sqlite:///:memory:")
    stream_key = f"test:idempotency:{uuid4().hex}"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "operation": "increment",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    try:
        initialize_counter_database(engine)

        with SyncRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                message_id = redis_client.xadd(stream_key, payload)
                redis_client.xgroup_create(
                    stream_key,
                    group_name,
                    id="0-0",
                )

                # worker-a 领取并完成业务，但故意不确认
                original_messages = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-a",
                    streams={stream_key: ">"},
                    count=1,
                )
                assert original_messages == [
                    [stream_key, [(message_id, payload)]],
                ]

                first_applied = apply_completion_once(
                    engine,
                    payload["event_id"],
                )
                assert first_applied is True

                pending_before_claim = redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_before_claim["pending"] == 1

                # worker-b 接管；测试使用 0 避免真实等待
                _, claimed_messages, _ = redis_client.xautoclaim(
                    name=stream_key,
                    groupname=group_name,
                    consumername="worker-b",
                    min_idle_time=0,
                    start_id="0-0",
                    count=1,
                )
                assert claimed_messages == [(message_id, payload)]

                # 使用实际接管到的消息编号进行幂等处理
                claimed_message_id, claimed_payload = claimed_messages[0]
                recovery_applied, acknowledged_count = (
                    process_completion_and_acknowledge(
                        engine,
                        redis_client,
                        stream_key,
                        group_name,
                        claimed_message_id,
                        event_id=claimed_payload["event_id"],
                    )
                )

                assert recovery_applied is False
                assert acknowledged_count == 1

                # 数据库业务效果仍然只有一次
                with engine.connect() as connection:
                    completed_count = connection.execute(
                        text(
                            "SELECT completed_count "
                            "FROM completion_counter WHERE id = 1"
                        ),
                    ).scalar_one()

                    processed_count = connection.execute(
                        text("SELECT COUNT(*) FROM processed_events"),
                    ).scalar_one()

                assert completed_count == 1
                assert processed_count == 1

                pending_after_ack = redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_ack["pending"] == 0
            finally:
                # 只清理本次测试创建的随机 Stream
                redis_client.delete(stream_key)
    finally:
        engine.dispose()


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_failed_claimed_message_remains_pending() -> None:
    engine = create_engine("sqlite:///:memory:")
    stream_key = f"test:idempotency:{uuid4().hex}"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "operation": "increment",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    try:
        initialize_counter_database(engine)

        # 删除业务目标，让接管后的业务处理失败
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM completion_counter WHERE id = 1"),
            )

        with SyncRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                message_id = redis_client.xadd(stream_key, payload)
                redis_client.xgroup_create(
                    stream_key,
                    group_name,
                    id="0-0",
                )

                # worker-a 只领取，不处理、不确认
                original_messages = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-a",
                    streams={stream_key: ">"},
                    count=1,
                )
                assert original_messages == [
                    [stream_key, [(message_id, payload)]],
                ]

                # worker-b 接管同一条消息
                _, claimed_messages, _ = redis_client.xautoclaim(
                    name=stream_key,
                    groupname=group_name,
                    consumername="worker-b",
                    min_idle_time=0,
                    start_id="0-0",
                    count=1,
                )
                assert claimed_messages == [(message_id, payload)]
                claimed_message_id, claimed_payload = claimed_messages[0]

                # 检查公共处理流程确实抛出业务异常
                with pytest.raises(
                    RuntimeError,
                    match="^Completion counter is missing$",
                ):
                    process_completion_and_acknowledge(
                        engine,
                        redis_client,
                        stream_key,
                        group_name,
                        claimed_message_id,
                        event_id=claimed_payload["event_id"],
                    )

                # 数据库不能残留错误的已处理记录
                with engine.connect() as connection:
                    processed_count = connection.execute(
                        text("SELECT COUNT(*) FROM processed_events"),
                    ).scalar_one()

                assert processed_count == 0

                # Redis 消息不能因为业务失败而被确认
                pending_after_failure = redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after_failure["pending"] == 1
                assert pending_after_failure["consumers"] == [
                    {"name": "worker-b", "pending": 1},
                ]

                # worker-b 仍能从自己的待确认列表读到它
                retryable_messages = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-b",
                    streams={stream_key: "0-0"},
                    count=1,
                )
                assert retryable_messages == [
                    [stream_key, [(message_id, payload)]],
                ]
            finally:
                # 仅清理本次测试数据，不代表业务确认
                redis_client.delete(stream_key)
    finally:
        engine.dispose()


def test_concurrent_duplicate_message_increments_only_once(
    tmp_path: Path,
) -> None:
    database_url = URL.create(
        "sqlite",
        database=str(tmp_path / "concurrent_messages.db"),
    )

    # 两个独立连接池，连接同一个测试数据库
    worker_engines = [
        create_engine(
            database_url,
            connect_args={"timeout": 5.0},
        )
        for _ in range(2)
    ]

    start_barrier = Barrier(2, timeout=5.0)
    event_id = "event-a"

    def process_in_worker(worker_engine: Engine) -> bool:
        # 两个线程到齐后，再开始处理相同消息
        start_barrier.wait()
        return apply_completion_once(worker_engine, event_id)

    try:
        # 两个 Engine 指向同一数据库，因此只初始化一次
        initialize_counter_database(worker_engines[0])

        with ThreadPoolExecutor(max_workers=2) as executor:
            # 先提交全部任务，不在这里等待单个任务完成
            futures = [
                executor.submit(process_in_worker, worker_engine)
                for worker_engine in worker_engines
            ]

            # 收集结果，同时让线程中的异常能够使测试失败
            results = [
                future.result(timeout=15.0)
                for future in futures
            ]

        # 不规定谁先成功，但必须一个执行、一个跳过
        assert sorted(results) == [False, True]

        with worker_engines[0].connect() as connection:
            completed_count = connection.execute(
                text(
                    "SELECT completed_count "
                    "FROM completion_counter WHERE id = 1"
                ),
            ).scalar_one()

            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_events"),
            ).scalar_one()

        assert completed_count == 1
        assert processed_count == 1
    finally:
        for worker_engine in worker_engines:
            worker_engine.dispose()


def test_distinct_messages_are_processed_independently() -> None:
    engine = create_engine("sqlite:///:memory:")

    try:
        initialize_counter_database(engine)

        # 两条独立业务消息，都应该首次处理成功
        first_applied = apply_completion_once(engine, "event-a")
        second_applied = apply_completion_once(engine, "event-b")

        # 分别重复投递，两条消息都不应该再次生效
        first_repeated = apply_completion_once(engine, "event-a")
        second_repeated = apply_completion_once(engine, "event-b")

        assert first_applied is True
        assert second_applied is True
        assert first_repeated is False
        assert second_repeated is False

        with engine.connect() as connection:
            completed_count = connection.execute(
                text(
                    "SELECT completed_count "
                    "FROM completion_counter WHERE id = 1"
                ),
            ).scalar_one()

            processed_event_ids = connection.execute(
                text(
                    "SELECT event_id FROM processed_events "
                    "ORDER BY event_id"
                ),
            ).scalars().all()

        assert completed_count == 2
        assert processed_event_ids == ["event-a", "event-b"]
    finally:
        engine.dispose()


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_two_messages_for_same_event_apply_once_and_ack_both() -> None:
    engine = create_engine("sqlite:///:memory:")
    stream_key = f"test:idempotency:{uuid4().hex}"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "operation": "increment",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    try:
        initialize_counter_database(engine)

        with SyncRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                # 同一业务事件被发送成两条不同消息
                first_message_id = redis_client.xadd(stream_key, payload)
                second_message_id = redis_client.xadd(stream_key, payload)

                assert first_message_id != second_message_id

                redis_client.xgroup_create(
                    stream_key,
                    group_name,
                    id="0-0",
                )

                messages = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-a",
                    streams={stream_key: ">"},
                    count=2,
                )
                assert messages == [
                    [
                        stream_key,
                        [
                            (first_message_id, payload),
                            (second_message_id, payload),
                        ],
                    ],
                ]

                # 业务事件虽然只有一个，待确认消息却有两条
                pending_before = redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_before["pending"] == 2

                results = []

                # 从实际读取结果中分别取出消息编号和正文
                for message_id, message_payload in messages[0][1]:
                    result = process_completion_and_acknowledge(
                        engine,
                        redis_client,
                        stream_key,
                        group_name,
                        message_id=message_id,
                        event_id=message_payload["event_id"],
                    )
                    results.append(result)

                # 业务只生效一次，两条消息分别确认成功
                assert results == [(True, 1), (False, 1)]

                with engine.connect() as connection:
                    completed_count = connection.execute(
                        text(
                            "SELECT completed_count "
                            "FROM completion_counter WHERE id = 1"
                        ),
                    ).scalar_one()

                    processed_event_ids = connection.execute(
                        text(
                            "SELECT event_id FROM processed_events "
                            "ORDER BY event_id"
                        ),
                    ).scalars().all()

                assert completed_count == 1
                assert processed_event_ids == ["event-a"]

                pending_after = redis_client.xpending(
                    stream_key,
                    group_name,
                )
                assert pending_after["pending"] == 0
            finally:
                # 只删除本次测试创建的随机 Stream
                redis_client.delete(stream_key)
    finally:
        engine.dispose()


def test_same_event_with_different_increment_is_rejected() -> None:
    engine = create_engine("sqlite:///:memory:")
    event_id = "event-a"

    try:
        initialize_counter_database(engine)

        # 使用非默认值，验证增加量确实参与业务处理
        first_applied = apply_completion_once(
            engine,
            event_id,
            increment_by=3,
        )
        assert first_applied is True

        # 同一个事件编号，却要求执行不同的业务内容
        with pytest.raises(
            EventPayloadConflictError,
            match="^Event payload conflicts with processed event$",
        ):
            apply_completion_once(
                engine,
                event_id,
                increment_by=10,
            )

        # 原内容再次投递，仍应被识别为正常重复
        repeated_applied = apply_completion_once(
            engine,
            event_id,
            increment_by=3,
        )
        assert repeated_applied is False

        with engine.connect() as connection:
            completed_count = connection.execute(
                text(
                    "SELECT completed_count "
                    "FROM completion_counter WHERE id = 1"
                ),
            ).scalar_one()

            stored_increment = connection.execute(
                text(
                    "SELECT increment_by FROM processed_events "
                    "WHERE event_id = :event_id"
                ),
                {"event_id": event_id},
            ).scalar_one()

            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_events"),
            ).scalar_one()

        assert completed_count == 3
        assert stored_increment == 3
        assert processed_count == 1
    finally:
        engine.dispose()


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_conflicting_stream_message_remains_pending() -> None:
    engine = create_engine("sqlite:///:memory:")
    stream_key = f"test:idempotency:{uuid4().hex}"
    group_name = "completion-workers"

    first_payload = {
        "event_id": "event-a",
        "increment_by": "3",
    }
    conflicting_payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    try:
        initialize_counter_database(engine)

        with SyncRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            try:
                first_id = redis_client.xadd(
                    stream_key, first_payload,
                )
                conflicting_id = redis_client.xadd(
                    stream_key, conflicting_payload,
                )
                assert first_id != conflicting_id

                redis_client.xgroup_create(
                    stream_key, group_name, id="0-0",
                )
                messages = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-a",
                    streams={stream_key: ">"},
                    count=2,
                )
                assert messages == [
                    [
                        stream_key,
                        [
                            (first_id, first_payload),
                            (conflicting_id, conflicting_payload),
                        ],
                    ],
                ]
                assert redis_client.xpending(
                    stream_key, group_name,
                )["pending"] == 2

                # 从实际领取的消息中提取业务参数
                message_id, payload = messages[0][1][0]
                result = process_completion_and_acknowledge(
                    engine,
                    redis_client,
                    stream_key,
                    group_name,
                    message_id=message_id,
                    event_id=payload["event_id"],
                    increment_by=int(payload["increment_by"]),
                )
                assert result == (True, 1)

                # 第二条消息内容冲突，不能确认
                message_id, payload = messages[0][1][1]
                with pytest.raises(
                    EventPayloadConflictError,
                    match="^Event payload conflicts with processed event$",
                ):
                    process_completion_and_acknowledge(
                        engine,
                        redis_client,
                        stream_key,
                        group_name,
                        message_id=message_id,
                        event_id=payload["event_id"],
                        increment_by=int(payload["increment_by"]),
                    )

                pending = redis_client.xpending(
                    stream_key, group_name,
                )
                assert pending["pending"] == 1

                # 不只检查数量，还要确认留下的是冲突消息
                remaining_messages = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-a",
                    streams={stream_key: "0-0"},
                    count=10,
                )
                assert remaining_messages == [
                    [
                        stream_key,
                        [(conflicting_id, conflicting_payload)],
                    ],
                ]

                with engine.connect() as connection:
                    completed_count = connection.execute(
                        text(
                            "SELECT completed_count "
                            "FROM completion_counter WHERE id = 1"
                        ),
                    ).scalar_one()
                    stored_events = connection.execute(
                        text(
                            "SELECT event_id, increment_by "
                            "FROM processed_events"
                        ),
                    ).all()

                assert completed_count == 3
                assert stored_events == [("event-a", 3)]
            finally:
                # 只清理本次测试创建的随机 Stream
                redis_client.delete(stream_key)
    finally:
        engine.dispose()


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_dead_letter_preserves_message_without_acknowledging() -> None:
    test_prefix = f"test:dead-letter:{uuid4().hex}"
    source_stream = f"{test_prefix}:source"
    dead_letter_stream = f"{test_prefix}:failed"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        # 先检查连接，避免连接失败后清理操作再次报错
        redis_client.ping()

        try:
            source_id = redis_client.xadd(source_stream, payload)
            redis_client.xgroup_create(
                source_stream, group_name, id="0-0",
            )

            received = redis_client.xreadgroup(
                groupname=group_name,
                consumername="worker-a",
                streams={source_stream: ">"},
                count=1,
            )
            assert received == [
                [source_stream, [(source_id, payload)]],
            ]
            received_id, received_payload = received[0][1][0]

            dead_letter_id = write_dead_letter(
                redis_client,
                dead_letter_stream=dead_letter_stream,
                source_stream=source_stream,
                source_group=group_name,
                source_message_id=received_id,
                payload=received_payload,
                reason="event_payload_conflict",
            )

            # 重新从 Redis 读取，不能只检查函数返回了编号
            records = redis_client.xrange(dead_letter_stream)
            assert len(records) == 1

            stored_id, stored_record = records[0]
            assert stored_id == dead_letter_id
            assert stored_record["source_stream"] == source_stream
            assert stored_record["source_group"] == group_name
            assert stored_record["source_message_id"] == source_id
            assert stored_record["reason"] == "event_payload_conflict"
            assert json.loads(stored_record["payload"]) == payload

            # 保存死信与确认原消息是两件不同的事
            pending = redis_client.xpending(
                source_stream, group_name,
            )
            assert pending["pending"] == 1
        finally:
            # 只删除本次测试创建的两个随机 Stream
            redis_client.delete(source_stream, dead_letter_stream)


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
@pytest.mark.parametrize(
    "write_fails",
    [False, True],
    ids=["saved", "write-failed"],
)
def test_archive_acknowledges_only_after_saving(
    monkeypatch: pytest.MonkeyPatch,
    write_fails: bool,
) -> None:
    test_prefix = f"test:archive:{uuid4().hex}"
    source_stream = f"{test_prefix}:source"
    dead_letter_stream = f"{test_prefix}:failed"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        redis_client.ping()

        try:
            source_id = redis_client.xadd(source_stream, payload)
            redis_client.xgroup_create(
                source_stream, group_name, id="0-0",
            )
            received = redis_client.xreadgroup(
                groupname=group_name,
                consumername="worker-a",
                streams={source_stream: ">"},
                count=1,
            )
            assert received == [
                [source_stream, [(source_id, payload)]],
            ]
            received_id, received_payload = received[0][1][0]

            with monkeypatch.context() as patch:
                if write_fails:
                    # 原消息已经准备好，只模拟后续死信写入失败
                    patch.setattr(
                        redis_client,
                        "eval",
                        Mock(
                            side_effect=RedisConnectionError(
                                "Dead-letter write failed"
                            ),
                        ),
                    )

                expected_outcome = (
                    pytest.raises(
                        RedisConnectionError,
                        match="^Dead-letter write failed$",
                    )
                    if write_fails
                    else nullcontext()
                )

                result = None
                with expected_outcome:
                    result = archive_message_and_acknowledge(
                        redis_client,
                        dead_letter_stream=dead_letter_stream,
                        source_stream=source_stream,
                        source_group=group_name,
                        source_message_id=received_id,
                        payload=received_payload,
                        reason="event_payload_conflict",
                    )

            # 此处临时替换已经恢复，查询真实 Redis 状态
            records = redis_client.xrange(dead_letter_stream)
            pending = redis_client.xpending(
                source_stream, group_name,
            )

            if write_fails:
                assert result is None
                assert records == []
                assert pending["pending"] == 1
            else:
                assert result is not None
                dead_letter_id, acknowledged_count = result

                assert acknowledged_count == 1
                assert pending["pending"] == 0
                assert len(records) == 1
                assert records[0][0] == dead_letter_id
                assert records[0][1]["source_message_id"] == source_id
                assert json.loads(records[0][1]["payload"]) == payload
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                f"{dead_letter_stream}:dedup",
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_dead_letter_retry_reuses_existing_record() -> None:
    test_prefix = f"test:dead-letter-once:{uuid4().hex}"
    source_stream = f"{test_prefix}:source"
    dead_letter_stream = f"{test_prefix}:failed"
    deduplication_key = f"{dead_letter_stream}:dedup"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        redis_client.ping()

        try:
            source_id = redis_client.xadd(source_stream, payload)

            first_id = write_dead_letter_once(
                redis_client,
                dead_letter_stream,
                source_stream,
                group_name,
                source_id,
                payload,
                "event_payload_conflict",
            )
            second_id = write_dead_letter_once(
                redis_client,
                dead_letter_stream,
                source_stream,
                group_name,
                source_id,
                payload,
                "event_payload_conflict",
            )

            # 重复调用应复用同一个死信编号
            assert second_id == first_id

            # 不能只检查编号，还要检查真正保存的记录
            records = redis_client.xrange(dead_letter_stream)
            assert len(records) == 1

            stored_id, stored_record = records[0]
            assert stored_id == first_id
            assert stored_record["source_stream"] == source_stream
            assert stored_record["source_group"] == group_name
            assert stored_record["source_message_id"] == source_id
            assert stored_record["reason"] == "event_payload_conflict"
            assert json.loads(stored_record["payload"]) == payload

            # 索引中也只应登记这一条死信编号
            indexed_ids = redis_client.hvals(deduplication_key)
            assert indexed_ids == [first_id]
        finally:
            # 本轮多创建了索引，清理时不能遗漏
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                deduplication_key,
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_retry_after_save_before_ack_uses_one_dead_letter() -> None:
    test_prefix = f"test:archive-retry:{uuid4().hex}"
    source_stream = f"{test_prefix}:source"
    dead_letter_stream = f"{test_prefix}:failed"
    deduplication_key = f"{dead_letter_stream}:dedup"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        redis_client.ping()

        try:
            source_id = redis_client.xadd(source_stream, payload)
            redis_client.xgroup_create(
                source_stream,
                group_name,
                id="0-0",
            )
            received = redis_client.xreadgroup(
                groupname=group_name,
                consumername="worker-a",
                streams={source_stream: ">"},
                count=1,
            )
            assert received == [
                [source_stream, [(source_id, payload)]],
            ]
            received_id, received_payload = received[0][1][0]

            # 模拟死信已保存，但原消息尚未确认时进程中断
            saved_id = write_dead_letter_once(
                redis_client,
                dead_letter_stream,
                source_stream,
                group_name,
                received_id,
                received_payload,
                "event_payload_conflict",
            )
            assert redis_client.xpending(
                source_stream, group_name,
            )["pending"] == 1

            # 测试中立即允许另一个 worker 接管
            claimed = redis_client.xautoclaim(
                source_stream,
                group_name,
                "worker-b",
                min_idle_time=0,
                start_id="0-0",
                count=1,
            )
            claimed_messages = claimed[1]
            assert claimed_messages == [(source_id, payload)]
            claimed_id, claimed_payload = claimed_messages[0]

            retry_id, acknowledged_count = (
                archive_message_and_acknowledge(
                    redis_client,
                    dead_letter_stream=dead_letter_stream,
                    source_stream=source_stream,
                    source_group=group_name,
                    source_message_id=claimed_id,
                    payload=claimed_payload,
                    reason="event_payload_conflict",
                )
            )

            assert retry_id == saved_id
            assert acknowledged_count == 1
            assert redis_client.xpending(
                source_stream, group_name,
            )["pending"] == 0

            dead_letters = redis_client.xrange(
                dead_letter_stream,
            )
            assert len(dead_letters) == 1
            assert dead_letters[0][0] == saved_id
            assert dead_letters[0][1]["source_message_id"] == source_id
            assert json.loads(
                dead_letters[0][1]["payload"]
            ) == payload
            assert redis_client.hvals(
                deduplication_key
            ) == [saved_id]
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                deduplication_key,
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_lost_reply_after_saving_dead_letter_is_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_prefix = f"test:lost-reply:{uuid4().hex}"
    source_stream = f"{test_prefix}:source"
    dead_letter_stream = f"{test_prefix}:failed"
    deduplication_key = f"{dead_letter_stream}:dedup"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        redis_client.ping()

        try:
            source_id = redis_client.xadd(source_stream, payload)
            redis_client.xgroup_create(
                source_stream,
                group_name,
                id="0-0",
            )
            messages = redis_client.xreadgroup(
                groupname=group_name,
                consumername="worker-a",
                streams={source_stream: ">"},
                count=1,
            )
            assert messages == [
                [source_stream, [(source_id, payload)]],
            ]
            message_id, received_payload = messages[0][1][0]

            # 保留真实方法，让 Redis 先执行保存脚本
            real_eval = redis_client.eval

            def execute_then_lose_reply(
                *args: object,
                **kwargs: object,
            ) -> None:
                real_eval(*args, **kwargs)
                raise RedisConnectionError(
                    "Reply lost after write"
                )

            with monkeypatch.context() as patch:
                patch.setattr(
                    redis_client,
                    "eval",
                    execute_then_lose_reply,
                )
                with pytest.raises(
                    RedisConnectionError,
                    match="^Reply lost after write$",
                ):
                    archive_message_and_acknowledge(
                        redis_client,
                        dead_letter_stream=dead_letter_stream,
                        source_stream=source_stream,
                        source_group=group_name,
                        source_message_id=message_id,
                        payload=received_payload,
                        reason="event_payload_conflict",
                    )

            # 临时替换已恢复，查询 Redis 中的真实状态
            dead_letters = redis_client.xrange(
                dead_letter_stream
            )
            assert len(dead_letters) == 1

            saved_id = dead_letters[0][0]
            assert (
                dead_letters[0][1]["source_message_id"]
                == source_id
            )
            assert json.loads(
                dead_letters[0][1]["payload"]
            ) == payload
            assert redis_client.hvals(
                deduplication_key
            ) == [saved_id]
            assert redis_client.xpending(
                source_stream, group_name,
            )["pending"] == 1

            # 再次处理同一条原消息
            retry_id, acknowledged_count = (
                archive_message_and_acknowledge(
                    redis_client,
                    dead_letter_stream=dead_letter_stream,
                    source_stream=source_stream,
                    source_group=group_name,
                    source_message_id=message_id,
                    payload=received_payload,
                    reason="event_payload_conflict",
                )
            )

            assert retry_id == saved_id
            assert acknowledged_count == 1
            assert len(
                redis_client.xrange(dead_letter_stream)
            ) == 1
            assert redis_client.xpending(
                source_stream, group_name,
            )["pending"] == 0
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                deduplication_key,
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_lost_xack_reply_does_not_duplicate_dead_letter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_prefix = f"test:lost-xack-reply:{uuid4().hex}"
    source_stream = f"{test_prefix}:source"
    dead_letter_stream = f"{test_prefix}:failed"
    deduplication_key = f"{dead_letter_stream}:dedup"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        redis_client.ping()

        try:
            source_id = redis_client.xadd(source_stream, payload)
            redis_client.xgroup_create(
                source_stream,
                group_name,
                id="0-0",
            )
            messages = redis_client.xreadgroup(
                groupname=group_name,
                consumername="worker-a",
                streams={source_stream: ">"},
                count=1,
            )
            assert messages == [
                [source_stream, [(source_id, payload)]],
            ]
            message_id, received_payload = messages[0][1][0]

            real_xack = redis_client.xack

            def acknowledge_then_lose_reply(
                stream_name: str,
                consumer_group: str,
                pending_id: str,
            ) -> None:
                real_xack(
                    stream_name,
                    consumer_group,
                    pending_id,
                )
                raise RedisConnectionError(
                    "XACK reply lost"
                )

            with monkeypatch.context() as patch:
                patch.setattr(
                    redis_client,
                    "xack",
                    acknowledge_then_lose_reply,
                )
                with pytest.raises(
                    RedisConnectionError,
                    match="^XACK reply lost$",
                ):
                    archive_message_and_acknowledge(
                        redis_client,
                        dead_letter_stream=dead_letter_stream,
                        source_stream=source_stream,
                        source_group=group_name,
                        source_message_id=message_id,
                        payload=received_payload,
                        reason="event_payload_conflict",
                    )

            # 替身已恢复，查询 Redis 中的真实状态
            dead_letters = redis_client.xrange(
                dead_letter_stream
            )
            assert len(dead_letters) == 1

            saved_id = dead_letters[0][0]
            assert redis_client.hvals(
                deduplication_key
            ) == [saved_id]
            assert redis_client.xpending(
                source_stream,
                group_name,
            )["pending"] == 0

            # XACK 移除 pending，不删除原 Stream 正文
            assert redis_client.xrange(source_stream) == [
                (source_id, payload),
            ]

            # 手动再次调用，用来观察重复调用的结果
            retry_id, acknowledged_count = (
                archive_message_and_acknowledge(
                    redis_client,
                    dead_letter_stream=dead_letter_stream,
                    source_stream=source_stream,
                    source_group=group_name,
                    source_message_id=message_id,
                    payload=received_payload,
                    reason="event_payload_conflict",
                )
            )

            assert retry_id == saved_id
            assert acknowledged_count == 0
            assert len(
                redis_client.xrange(dead_letter_stream)
            ) == 1
            assert redis_client.xpending(
                source_stream,
                group_name,
            )["pending"] == 0
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                deduplication_key,
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_pending_is_claimed_only_after_idle_threshold() -> None:
    source_stream = f"test:claim-idle:{uuid4().hex}"
    group_name = "completion-workers"
    payload = {"event_id": "event-a"}
    claim_threshold_ms = 60_000
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    ) as redis_client:
        redis_client.ping()

        try:
            message_id = redis_client.xadd(
                source_stream, payload,
            )
            redis_client.xgroup_create(
                source_stream,
                group_name,
                id="0-0",
            )
            received = redis_client.xreadgroup(
                groupname=group_name,
                consumername="worker-a",
                streams={source_stream: ">"},
                count=1,
            )
            assert received == [
                [source_stream, [(message_id, payload)]],
            ]

            # 消息刚领取，不满足一分钟的接管门槛
            early_claim = redis_client.xautoclaim(
                source_stream,
                group_name,
                "worker-b",
                min_idle_time=claim_threshold_ms,
                start_id="0-0",
                count=1,
            )
            assert early_claim[1] == []

            pending_before = redis_client.xpending_range(
                source_stream,
                group_name,
                "-",
                "+",
                1,
            )[0]
            assert pending_before["consumer"] == "worker-a"

            # 仅测试用：人为模拟已空闲 61 秒
            aged_message = redis_client.xclaim(
                source_stream,
                group_name,
                "worker-a",
                min_idle_time=0,
                message_ids=[message_id],
                idle=61_000,
            )
            assert aged_message == [(message_id, payload)]

            # 现在超过门槛，worker-b 可以接管
            claimed = redis_client.xautoclaim(
                source_stream,
                group_name,
                "worker-b",
                min_idle_time=claim_threshold_ms,
                start_id="0-0",
                count=1,
            )
            assert claimed[1] == [(message_id, payload)]

            pending_after = redis_client.xpending_range(
                source_stream,
                group_name,
                "-",
                "+",
                1,
            )[0]
            assert pending_after["consumer"] == "worker-b"

            assert redis_client.xack(
                source_stream,
                group_name,
                message_id,
            ) == 1
            assert redis_client.xpending(
                source_stream,
                group_name,
            )["pending"] == 0
        finally:
            redis_client.delete(source_stream)


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
@pytest.mark.parametrize(
    "first_finisher",
    ["worker-a", "worker-b"],
)
def test_reclaimed_message_has_one_business_effect(
    first_finisher: str,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    source_stream = f"test:claim-overlap:{uuid4().hex}"
    group_name = "completion-workers"
    payload = {
        "event_id": "event-a",
        "increment_by": "3",
    }
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )

    try:
        initialize_counter_database(engine)

        with SyncRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
        ) as redis_client:
            redis_client.ping()

            try:
                source_id = redis_client.xadd(
                    source_stream, payload,
                )
                redis_client.xgroup_create(
                    source_stream, group_name, id="0-0",
                )
                received_a = redis_client.xreadgroup(
                    groupname=group_name,
                    consumername="worker-a",
                    streams={source_stream: ">"},
                    count=1,
                )
                assert received_a == [
                    [source_stream, [(source_id, payload)]],
                ]

                # 模拟 A 仍持有消息，但空闲时间已超过门槛
                redis_client.xclaim(
                    source_stream,
                    group_name,
                    "worker-a",
                    min_idle_time=0,
                    message_ids=[source_id],
                    idle=61_000,
                )
                claimed_b = redis_client.xautoclaim(
                    source_stream,
                    group_name,
                    "worker-b",
                    min_idle_time=60_000,
                    start_id="0-0",
                    count=1,
                )
                assert claimed_b[1] == [(source_id, payload)]

                pending_entry = redis_client.xpending_range(
                    source_stream, group_name, "-", "+", 1,
                )[0]
                assert pending_entry["consumer"] == "worker-b"

                # 两边分别保留自己实际取得的消息
                deliveries = {
                    "worker-a": received_a[0][1][0],
                    "worker-b": claimed_b[1][0],
                }
                second_finisher = (
                    "worker-b"
                    if first_finisher == "worker-a"
                    else "worker-a"
                )

                results = []
                for worker_name in (
                    first_finisher,
                    second_finisher,
                ):
                    message_id, message_payload = (
                        deliveries[worker_name]
                    )
                    result = process_completion_and_acknowledge(
                        engine,
                        redis_client,
                        source_stream,
                        group_name,
                        message_id=message_id,
                        event_id=message_payload["event_id"],
                        increment_by=int(
                            message_payload["increment_by"]
                        ),
                    )
                    results.append(result)

                # 先完成的执行业务，后完成的识别为正常重复
                assert results == [(True, 1), (False, 0)]

                with engine.connect() as connection:
                    completed_count = connection.execute(
                        text(
                            "SELECT completed_count "
                            "FROM completion_counter WHERE id = 1"
                        ),
                    ).scalar_one()
                    stored_events = connection.execute(
                        text(
                            "SELECT event_id, increment_by "
                            "FROM processed_events"
                        ),
                    ).all()

                assert completed_count == 3
                assert stored_events == [("event-a", 3)]
                assert redis_client.xpending(
                    source_stream, group_name,
                )["pending"] == 0
            finally:
                redis_client.delete(source_stream)
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("failed_attempts", "expected_retry"),
    [
        (1, True),
        (2, True),
        (3, False),
        (4, False),
    ],
)
def test_message_retry_respects_attempt_limit(
    failed_attempts: int,
    expected_retry: bool,
) -> None:
    should_retry = should_retry_message(
        failed_attempts=failed_attempts,
        max_attempts=3,
    )

    assert should_retry is expected_retry


@pytest.mark.parametrize(
    ("failed_attempts", "max_attempts", "expected_parameter"),
    [
        (0, 3, "failed_attempts"),
        (-1, 3, "failed_attempts"),
        (1, 0, "max_attempts"),
        (1, -1, "max_attempts"),
    ],
)
def test_message_retry_rejects_invalid_counts(
    failed_attempts: int,
    max_attempts: int,
    expected_parameter: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=expected_parameter,
    ):
        should_retry_message(
            failed_attempts=failed_attempts,
            max_attempts=max_attempts,
        )


@pytest.mark.parametrize(
    ("error", "failed_attempts", "expected_action"),
    [
        (
            RetryableMessageError("Temporary failure"),
            1,
            "retry",
        ),
        (
            RetryableMessageError("Temporary failure"),
            3,
            "dead_letter",
        ),
        (
            EventPayloadConflictError("Payload conflict"),
            1,
            "dead_letter",
        ),
    ],
)
def test_message_failure_selects_action(
    error: Exception,
    failed_attempts: int,
    expected_action: str,
) -> None:
    action = decide_message_failure(
        error=error,
        failed_attempts=failed_attempts,
        max_attempts=3,
    )

    assert action == expected_action


def test_message_failure_propagates_unknown_error() -> None:
    original_error = TypeError("Unexpected programming error")

    with pytest.raises(TypeError) as captured_error:
        decide_message_failure(
            error=original_error,
            failed_attempts=1,
            max_attempts=3,
        )

    assert captured_error.value is original_error


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
@pytest.mark.parametrize(
    (
        "error",
        "failed_attempts",
        "expected_action",
        "expected_reason",
    ),
    [
        (
            RetryableMessageError("Temporary failure"),
            1,
            "retry",
            None,
        ),
        (
            RetryableMessageError("Temporary failure"),
            3,
            "dead_letter",
            "RetryableMessageError",
        ),
        (
            EventPayloadConflictError("Payload conflict"),
            1,
            "dead_letter",
            "EventPayloadConflictError",
        ),
    ],
)
def test_failure_handler_applies_retry_policy(
    error: Exception,
    failed_attempts: int,
    expected_action: str,
    expected_reason: str | None,
) -> None:
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )
    key_prefix = f"test:message-retry:{uuid4().hex}"
    source_stream = f"{key_prefix}:source"
    dead_letter_stream = f"{key_prefix}:dead"
    source_group = "retry-test-group"
    payload = {
        "event_id": "event-a",
        "increment_by": "3",
    }

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        try:
            message_id = redis_client.xadd(
                source_stream,
                payload,
            )
            redis_client.xgroup_create(
                source_stream,
                source_group,
                id="0-0",
            )

            # 先领取消息，使其进入待确认列表。
            received = redis_client.xreadgroup(
                source_group,
                "worker-a",
                {source_stream: ">"},
                count=1,
            )
            assert received[0][1][0][0] == message_id

            action = handle_message_failure(
                redis_client=redis_client,
                dead_letter_stream=dead_letter_stream,
                source_stream=source_stream,
                source_group=source_group,
                source_message_id=message_id,
                payload=payload,
                error=error,
                failed_attempts=failed_attempts,
                max_attempts=3,
            )

            assert action == expected_action

            pending_messages = redis_client.xpending_range(
                source_stream,
                source_group,
                "-",
                "+",
                10,
            )

            if expected_action == "retry":
                assert [
                           message["message_id"]
                           for message in pending_messages
                       ] == [message_id]

                assert redis_client.xlen(dead_letter_stream) == 0
                assert redis_client.exists(
                    f"{dead_letter_stream}:dedup",
                ) == 0
            else:
                assert pending_messages == []

                dead_letters = redis_client.xrange(
                    dead_letter_stream,
                )
                assert len(dead_letters) == 1
                dead_letter_id, dead_letter = dead_letters[0]

                assert dead_letter["source_stream"] == source_stream
                assert dead_letter["source_group"] == source_group
                assert dead_letter["source_message_id"] == message_id
                assert json.loads(dead_letter["payload"]) == payload
                assert dead_letter["reason"] == expected_reason

                message_identity = json.dumps(
                    [source_stream, source_group, message_id],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                assert redis_client.hgetall(
                    f"{dead_letter_stream}:dedup",
                ) == {message_identity: dead_letter_id}

                # 确认只移除了 pending，原消息正文仍然存在。
                assert redis_client.xrange(source_stream) == [
                    (message_id, payload),
                ]
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                f"{dead_letter_stream}:dedup",
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_failure_handler_preserves_pending_when_archive_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )
    key_prefix = f"test:archive-failure:{uuid4().hex}"
    source_stream = f"{key_prefix}:source"
    dead_letter_stream = f"{key_prefix}:dead"
    source_group = "archive-failure-group"
    payload = {
        "event_id": "event-a",
        "increment_by": "3",
    }

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        try:
            message_id = redis_client.xadd(
                source_stream,
                payload,
            )
            redis_client.xgroup_create(
                source_stream,
                source_group,
                id="0-0",
            )
            received = redis_client.xreadgroup(
                source_group,
                "worker-a",
                {source_stream: ">"},
                count=1,
            )
            assert received[0][1][0][0] == message_id

            # 只让死信保存脚本失败，其他 Redis 操作保持真实。
            save_error = RedisConnectionError(
                "Dead-letter write failed",
            )
            failed_eval = Mock(side_effect=save_error)
            acknowledge_spy = Mock(wraps=redis_client.xack)

            monkeypatch.setattr(
                redis_client,
                "eval",
                failed_eval,
            )
            monkeypatch.setattr(
                redis_client,
                "xack",
                acknowledge_spy,
            )

            with pytest.raises(
                RedisConnectionError,
                match="Dead-letter write failed",
            ) as captured_error:
                handle_message_failure(
                    redis_client=redis_client,
                    dead_letter_stream=dead_letter_stream,
                    source_stream=source_stream,
                    source_group=source_group,
                    source_message_id=message_id,
                    payload=payload,
                    error=RetryableMessageError("Temporary failure"),
                    failed_attempts=3,
                    max_attempts=3,
                )

            # 保存确实被尝试，异常没有被吞掉，确认没有被调用。
            assert captured_error.value is save_error
            failed_eval.assert_called_once()
            acknowledge_spy.assert_not_called()

            pending_messages = redis_client.xpending_range(
                source_stream,
                source_group,
                "-",
                "+",
                10,
            )
            assert [
                message["message_id"]
                for message in pending_messages
            ] == [message_id]

            assert redis_client.xlen(dead_letter_stream) == 0
            assert redis_client.exists(
                f"{dead_letter_stream}:dedup",
            ) == 0
            assert redis_client.xrange(source_stream) == [
                (message_id, payload),
            ]
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                f"{dead_letter_stream}:dedup",
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_failure_handler_preserves_pending_for_unknown_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )
    key_prefix = f"test:unknown-failure:{uuid4().hex}"
    source_stream = f"{key_prefix}:source"
    dead_letter_stream = f"{key_prefix}:dead"
    source_group = "archive-failure-group"
    payload = {
        "event_id": "event-a",
        "increment_by": "3",
    }

    with SyncRedis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        try:
            message_id = redis_client.xadd(
                source_stream,
                payload,
            )
            redis_client.xgroup_create(
                source_stream,
                source_group,
                id="0-0",
            )
            received = redis_client.xreadgroup(
                source_group,
                "worker-a",
                {source_stream: ">"},
                count=1,
            )
            assert received[0][1][0][0] == message_id

            original_error = TypeError(
                "Unexpected programming error",
            )
            save_spy = Mock(wraps=redis_client.eval)
            acknowledge_spy = Mock(wraps=redis_client.xack)

            monkeypatch.setattr(
                redis_client,
                "eval",
                save_spy,
            )
            monkeypatch.setattr(
                redis_client,
                "xack",
                acknowledge_spy,
            )

            with pytest.raises(
                    TypeError,
                    match="Unexpected programming error",
            ) as captured_error:
                handle_message_failure(
                    redis_client=redis_client,
                    dead_letter_stream=dead_letter_stream,
                    source_stream=source_stream,
                    source_group=source_group,
                    source_message_id=message_id,
                    payload=payload,
                    error=original_error,
                    failed_attempts=3,
                    max_attempts=3,
                )

            assert captured_error.value is original_error
            save_spy.assert_not_called()
            acknowledge_spy.assert_not_called()

            pending_messages = redis_client.xpending_range(
                source_stream,
                source_group,
                "-",
                "+",
                10,
            )
            assert [
                message["message_id"]
                for message in pending_messages
            ] == [message_id]

            assert redis_client.xlen(dead_letter_stream) == 0
            assert redis_client.exists(
                f"{dead_letter_stream}:dedup",
            ) == 0
            assert redis_client.xrange(source_stream) == [
                (message_id, payload),
            ]
        finally:
            redis_client.delete(
                source_stream,
                dead_letter_stream,
                f"{dead_letter_stream}:dedup",
            )


@pytest.mark.skipif(
    os.getenv("RUN_REDIS_INTEGRATION") != "1",
    reason="需要明确开启真实 Redis 集成测试",
)
def test_real_conflict_is_archived_without_changing_database() -> None:
    engine = create_engine("sqlite:///:memory:")
    redis_url = os.getenv(
        "TEST_REDIS_URL",
        "redis://127.0.0.1:6380/0",
    )
    key_prefix = f"test:real-conflict:{uuid4().hex}"
    source_stream = f"{key_prefix}:source"
    dead_letter_stream = f"{key_prefix}:dead"
    source_group = "conflict-workers"
    conflicting_payload = {
        "event_id": "event-a",
        "increment_by": "10",
    }

    try:
        initialize_counter_database(engine)

        # 先真实提交原事件：增加 3。
        assert apply_completion_once(
            engine,
            "event-a",
            increment_by=3,
        ) is True

        with SyncRedis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        ) as redis_client:
            try:
                message_id = redis_client.xadd(
                    source_stream,
                    conflicting_payload,
                )
                redis_client.xgroup_create(
                    source_stream,
                    source_group,
                    id="0-0",
                )
                received = redis_client.xreadgroup(
                    source_group,
                    "worker-a",
                    {source_stream: ">"},
                    count=1,
                )
                received_id, received_payload = received[0][1][0]
                assert received_id == message_id

                # 传入实际领取的内容，由数据库业务逻辑发现冲突。
                action = process_completion_with_failure_policy(
                    engine=engine,
                    redis_client=redis_client,
                    dead_letter_stream=dead_letter_stream,
                    source_stream=source_stream,
                    source_group=source_group,
                    source_message_id=received_id,
                    payload=received_payload,
                    failed_attempts=1,
                    max_attempts=3,
                )
                assert action == "dead_letter"

                with engine.connect() as connection:
                    completed_count = connection.execute(
                        text(
                            "SELECT completed_count "
                            "FROM completion_counter WHERE id = 1"
                        ),
                    ).scalar_one()
                    stored_events = connection.execute(
                        text(
                            "SELECT event_id, increment_by "
                            "FROM processed_events"
                        ),
                    ).all()

                assert completed_count == 3
                assert stored_events == [("event-a", 3)]

                dead_letters = redis_client.xrange(dead_letter_stream)
                assert len(dead_letters) == 1
                dead_letter_id, dead_letter = dead_letters[0]

                assert dead_letter["source_stream"] == source_stream
                assert dead_letter["source_group"] == source_group
                assert dead_letter["source_message_id"] == message_id
                assert json.loads(dead_letter["payload"]) == conflicting_payload
                assert dead_letter["reason"] == "EventPayloadConflictError"

                message_identity = json.dumps(
                    [source_stream, source_group, message_id],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                assert redis_client.hgetall(
                    f"{dead_letter_stream}:dedup",
                ) == {message_identity: dead_letter_id}

                assert redis_client.xpending(
                    source_stream,
                    source_group,
                )["pending"] == 0

                assert redis_client.xrange(source_stream) == [
                    (message_id, conflicting_payload),
                ]
            finally:
                redis_client.delete(
                    source_stream,
                    dead_letter_stream,
                    f"{dead_letter_stream}:dedup",
                )
    finally:
        engine.dispose()