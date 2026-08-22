import pytest

from circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)

class FakeClock:
    def __init__(self) -> None:
        self.current_time = 0.0

    def __call__(self) -> float:
        return self.current_time

    def advance(self, seconds: float) -> None:
        self.current_time += seconds


def test_circuit_breaker_starts_closed() -> None:
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
    )

    circuit_breaker.before_call()

    assert circuit_breaker.state is CircuitState.CLOSED
    assert circuit_breaker.failure_count == 0


def test_circuit_breaker_opens_after_failure_threshold() -> None:
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
    )

    circuit_breaker.record_failure()

    assert circuit_breaker.state is CircuitState.CLOSED

    circuit_breaker.record_failure()

    assert circuit_breaker.state is CircuitState.OPEN
    assert circuit_breaker.failure_count == 2

    with pytest.raises(CircuitBreakerOpenError):
        circuit_breaker.before_call()


def test_success_resets_consecutive_failure_count() -> None:
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
    )

    circuit_breaker.record_failure()
    circuit_breaker.record_success()
    circuit_breaker.record_failure()

    assert circuit_breaker.failure_count == 1
    assert circuit_breaker.state is CircuitState.CLOSED

def test_open_circuit_rejects_request_before_recovery_timeout(
) -> None:
    fake_clock = FakeClock()
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=10.0,
        clock=fake_clock,
    )

    circuit_breaker.record_failure()
    circuit_breaker.record_failure()
    fake_clock.advance(9.0)

    with pytest.raises(CircuitBreakerOpenError):
        circuit_breaker.before_call()

    assert circuit_breaker.state is CircuitState.OPEN


def test_open_circuit_enters_half_open_after_recovery_timeout(
) -> None:
    fake_clock = FakeClock()
    circuit_breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=10.0,
        clock=fake_clock,
    )

    circuit_breaker.record_failure()
    circuit_breaker.record_failure()
    fake_clock.advance(10.0)

    circuit_breaker.before_call()

    assert circuit_breaker.state is CircuitState.HALF_OPEN


def test_half_open_circuit_closes_after_success() -> None:
    fake_clock = FakeClock()
    circuit_breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=10.0,
        clock=fake_clock,
    )

    circuit_breaker.record_failure()
    fake_clock.advance(10.0)
    circuit_breaker.before_call()

    circuit_breaker.record_success()

    assert circuit_breaker.state is CircuitState.CLOSED
    assert circuit_breaker.failure_count == 0


def test_half_open_circuit_reopens_after_failure() -> None:
    fake_clock = FakeClock()
    circuit_breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=10.0,
        clock=fake_clock,
    )

    circuit_breaker.record_failure()
    fake_clock.advance(10.0)
    circuit_breaker.before_call()

    circuit_breaker.record_failure()
    fake_clock.advance(9.0)

    with pytest.raises(CircuitBreakerOpenError):
        circuit_breaker.before_call()

    assert circuit_breaker.state is CircuitState.OPEN

def test_half_open_circuit_allows_only_one_probe_request(
) -> None:
    fake_clock = FakeClock()
    circuit_breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=10.0,
        clock=fake_clock,
    )

    circuit_breaker.record_failure()
    fake_clock.advance(10.0)

    circuit_breaker.before_call()

    assert circuit_breaker.state is CircuitState.HALF_OPEN

    with pytest.raises(
        CircuitBreakerOpenError,
        match="probe is already in progress",
    ):
        circuit_breaker.before_call()