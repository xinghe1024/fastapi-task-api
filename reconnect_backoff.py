class ReconnectBackoff:
    def __init__(
        self,
        initial_delay: float = 1.0,
        maximum_delay: float = 30.0,
    ) -> None:
        if initial_delay <= 0:
            raise ValueError("initial_delay must be positive")

        if maximum_delay < initial_delay:
            raise ValueError(
                "maximum_delay must be at least initial_delay"
            )

        self._initial_delay = initial_delay
        self._maximum_delay = maximum_delay
        self._next_delay = initial_delay

    def next_delay(self) -> float:
        current_delay = self._next_delay

        self._next_delay = min(
            current_delay * 2,
            self._maximum_delay,
        )

        return current_delay

    def reset(self) -> None:
        self._next_delay = self._initial_delay