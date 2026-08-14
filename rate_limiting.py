from collections.abc import Callable
from dataclasses import dataclass
from math import ceil
from threading import Lock
from time import monotonic

@dataclass
class FixedWindow:
    started_at: float
    request_count: int


class FixedWindowRateLimiter:
    def __init__(
        self,
        request_limit: int,
        window_seconds: int,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._request_limit = request_limit
        self._window_seconds = window_seconds
        self._clock = clock
        self._windows: dict[str, FixedWindow] = {}
        self._lock = Lock()

    def check(
        self,
        client_key: str,
    ) -> int | None:
        current_time = self._clock()

        with self._lock:
            window = self._windows.get(client_key)

            if (
                    window is None
                    or current_time - window.started_at
                    >= self._window_seconds
            ):
                self._windows[client_key] = FixedWindow(
                    started_at=current_time,
                    request_count=1,
                )
                return None

            if window.request_count < self._request_limit:
                window.request_count += 1
                return None

            retry_after_seconds = ceil(
                self._window_seconds
                - (current_time - window.started_at)
            )
            return max(1, retry_after_seconds)

