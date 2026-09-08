from reconnect_backoff import ReconnectBackoff


def test_reconnect_backoff_grows_until_maximum() -> None:
    backoff = ReconnectBackoff(
        initial_delay=1.0,
        maximum_delay=8.0,
    )

    delays = [
        backoff.next_delay()
        for _ in range(6)
    ]

    assert delays == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_reconnect_backoff_reset_restores_initial_delay() -> None:
    backoff = ReconnectBackoff(
        initial_delay=1.0,
        maximum_delay=8.0,
    )

    assert backoff.next_delay() == 1.0
    assert backoff.next_delay() == 2.0

    backoff.reset()

    assert backoff.next_delay() == 1.0
    assert backoff.next_delay() == 2.0