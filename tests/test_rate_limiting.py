import pytest
from rate_limiting import FixedWindowRateLimiter

class FakeClock:
    def __init__(self) -> None:
        self.current_time = 100.0

    def __call__(self) -> float:
        return self.current_time

    def advance(
        self,
        seconds: float,
    ) -> None:
        self.current_time += seconds

@pytest.fixture
def limiter_and_clock():
    fake_clock = FakeClock()
    limiter = FixedWindowRateLimiter(
        request_limit=3,
        window_seconds=60,
        clock=fake_clock,
    )

    return limiter, fake_clock

def test_rate_limiter_allows_requests_up_to_limit(limiter_and_clock) -> None:
    limiter,_ = limiter_and_clock
    client_key = "test_user"

    assert limiter.check(client_key) is None
    assert limiter.check(client_key) is None
    assert limiter.check(client_key) is None

def test_rate_limiter_returns_retry_after_when_exceeded(limiter_and_clock) -> None:
    limiter, clock = limiter_and_clock
    client_key = "test_user"

    for _ in range(3):
        limiter.check(client_key)

    assert limiter.check(client_key) == 60

    clock.advance(10.5)

    assert limiter.check(client_key) == 50

def test_rate_limiter_resets_after_window_expires(limiter_and_clock) -> None:
    limiter, clock = limiter_and_clock
    client_key = "test_user"

    for _ in range(3):
        limiter.check(client_key)

    clock.advance(60)

    assert limiter.check(client_key) is None