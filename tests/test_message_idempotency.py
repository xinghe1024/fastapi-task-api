import pytest
import os

from uuid import uuid4
from sqlalchemy import Engine, create_engine, text

from redis import Redis as SyncRedis


def apply_completion_once(engine: Engine, message_id: str) -> bool:
    # 处理记录与业务修改共用一个事务
    with engine.begin() as connection:
        inserted_record = connection.execute(
            text(
                "INSERT INTO processed_messages (message_id) "
                "VALUES (:message_id) "
                "ON CONFLICT (message_id) DO NOTHING"
            ),
            {"message_id": message_id},
        )

        # 已处理过这条消息，不再增加业务计数
        if inserted_record.rowcount == 0:
            return False

        updated_counter = connection.execute(
            text(
                "UPDATE completion_counter "
                "SET completed_count = completed_count + 1 "
                "WHERE id = 1"
            ),
        )

        # 业务目标不存在时，让整个事务回滚
        if updated_counter.rowcount != 1:
            raise RuntimeError("Completion counter is missing")

    # 正常离开事务块之后，提交已经完成
    return True


def initialize_counter_database(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE processed_messages ("
                "message_id TEXT NOT NULL PRIMARY KEY)"
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


def test_duplicate_message_increments_counter_only_once() -> None:
    engine = create_engine("sqlite:///:memory:")

    try:
        # 准备独立的测试表与初始计数
        initialize_counter_database(engine)

        # 模拟同一条消息被处理两次
        first_applied = apply_completion_once(engine, "message-a")
        second_applied = apply_completion_once(engine, "message-a")

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
                text("SELECT COUNT(*) FROM processed_messages"),
            ).scalar_one()

        assert completed_count == 1
        assert processed_count == 1
    finally:
        engine.dispose()


def test_failed_message_can_be_retried_after_rollback() -> None:
    engine = create_engine("sqlite:///:memory:")
    message_id = "message-a"

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
            apply_completion_once(engine, message_id)

        # 抛出异常还不够，必须确认处理记录没有残留
        with engine.connect() as connection:
            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_messages"),
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
        retry_applied = apply_completion_once(engine, message_id)

        assert retry_applied is True

        with engine.connect() as connection:
            completed_count = connection.execute(
                text(
                    "SELECT completed_count "
                    "FROM completion_counter WHERE id = 1"
                ),
            ).scalar_one()

            processed_count = connection.execute(
                text("SELECT COUNT(*) FROM processed_messages"),
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
    payload = {"operation": "increment"}
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

                first_applied = apply_completion_once(engine, message_id)
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
                claimed_message_id, _ = claimed_messages[0]
                recovery_applied = apply_completion_once(
                    engine,
                    claimed_message_id,
                )
                assert recovery_applied is False

                # 已确认业务此前成功提交，因此跳过修改后补做确认
                acknowledged_count = redis_client.xack(
                    stream_key,
                    group_name,
                    claimed_message_id,
                )
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
                        text("SELECT COUNT(*) FROM processed_messages"),
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