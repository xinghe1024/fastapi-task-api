import httpx


async def fetch_external_status(
    http_client: httpx.AsyncClient,
    url: str,
) -> int:
    response = await http_client.get(url)
    response.raise_for_status()

    return response.status_code