import httpx

from circuit_breaker import CircuitBreaker
from retrying import (
    RetryBudgetExceededError,
    retry_http_operation,
)


async def fetch_external_status(
    http_client: httpx.AsyncClient,
    url: str,
    *,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
    total_timeout: float = 15.0,
    circuit_breaker: CircuitBreaker | None = None,
) -> int:
    async def request_status() -> int:
        response = await http_client.get(url)
        response.raise_for_status()

        return response.status_code

    async def execute_request_with_retry() -> int:
        return await retry_http_operation(
            operation=request_status,
            max_attempts=max_attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            total_timeout=total_timeout,
        )

    if circuit_breaker is None:
        return await execute_request_with_retry()

    circuit_breaker.before_call()

    try:
        status_code = await execute_request_with_retry()
    except (
        httpx.HTTPError,
        RetryBudgetExceededError,
    ):
        circuit_breaker.record_failure()
        raise

    circuit_breaker.record_success()

    return status_code