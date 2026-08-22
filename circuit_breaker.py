from collections.abc import Callable
from enum import StrEnum
from time import monotonic


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(RuntimeError):
    """熔断器处于打开状态。"""


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int,
        recovery_timeout: float = 30.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError(
                "failure_threshold must be at least 1",
            )

        if recovery_timeout <= 0:
            raise ValueError(
                "recovery_timeout must be greater than 0",
            )

        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._clock = clock

        self._failure_count = 0
        self._opened_at: float | None = None
        self._state = CircuitState.CLOSED

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    def before_call(self) -> None:
        if self._state is CircuitState.CLOSED:
            return

        if self._state is CircuitState.HALF_OPEN:
            raise CircuitBreakerOpenError(
                "Circuit breaker probe is already in progress",
            )

        if self._opened_at is None:
            raise RuntimeError(
                "Open circuit breaker has no opening time",
            )

        elapsed_time = self._clock() - self._opened_at

        if elapsed_time < self._recovery_timeout:
            raise CircuitBreakerOpenError(
                "Circuit breaker is open",
            )

        self._state = CircuitState.HALF_OPEN

    def record_success(self) -> None:
        self._failure_count = 0
        self._opened_at = None
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._open()
            return

        self._failure_count += 1

        if self._failure_count >= self._failure_threshold:
            self._open()

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()