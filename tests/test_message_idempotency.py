import pytest
import os
import json

from uuid import uuid4
from sqlalchemy import URL, Engine, create_engine, text
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from redis import Redis as SyncRedis


class EventPayloadConflictError(ValueError):
    """同一业务事件编号对应了不同的业务内容。"""


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