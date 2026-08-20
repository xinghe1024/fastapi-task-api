from typing import Annotated, Literal

import httpx
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)
from pydantic import BaseModel

from config import Settings, get_settings
from external_service import fetch_external_status
from http_client_dependencies import get_http_client


class ExternalServiceResponse(BaseModel):
    status: Literal["available"]


router = APIRouter(
    prefix="/external",
    tags=["external"],
)


@router.get(
    "/status",
    response_model=ExternalServiceResponse,
)
async def check_external_service(
    http_client: Annotated[
        httpx.AsyncClient,
        Depends(get_http_client),
    ],
    settings: Annotated[
        Settings,
        Depends(get_settings),
    ],
) -> ExternalServiceResponse:
    try:
        await fetch_external_status(
            http_client=http_client,
            url=settings.external_service_url,
            max_attempts=(
                settings.external_service_max_attempts
            ),
            base_delay=(
                settings.external_service_base_delay
            ),
        )
    except httpx.TimeoutException as external_error:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Upstream service timed out",
        ) from external_error
    except httpx.HTTPError as external_error:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream service unavailable",
        ) from external_error

    return ExternalServiceResponse(
        status="available",
    )