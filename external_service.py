import httpx

from retrying import retry_http_operation


async def fetch_external_status(
    http_client: httpx.AsyncClient,
    url: str,
    *,
    max_attempts: int = 3,
    base_delay: float = 0.5,
) -> int:
    async def request_status() -> int:
        response = await http_client.get(url)
        response.raise_for_status()

        return response.status_code

    return await retry_http_operation(
        operation=request_status,
        max_attempts=max_attempts,
        base_delay=base_delay,
    )