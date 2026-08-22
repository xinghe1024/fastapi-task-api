from typing import Annotated, Literal

import httpx
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)
from pydantic import BaseModel

from circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
)
from circuit_breaker_dependencies import (
    get_external_service_circuit_breaker,
)
from config import Settings, get_settings
from external_service import fetch_external_status
from http_client_dependencies import get_http_client
from retrying import RetryBudgetExceededError


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
    circuit_breaker: Annotated[
        CircuitBreaker,
        Depends(get_external_service_circuit_breaker),
    ],
) -> ExternalServiceResponse:
    try:
        await fetch_external_status(
            http_client=http_client,
            url=settings.external_service_url,
            max_delay=(
                settings.external_service_max_delay
            ),
            total_timeout=(
                settings.external_service_total_timeout
            ),
            max_attempts=(
                settings.external_service_max_attempts
            ),
            base_delay=(
                settings.external_service_base_delay
            ),
            circuit_breaker=circuit_breaker,
        )
    except (
        httpx.TimeoutException,
        RetryBudgetExceededError,
    ) as external_error:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Upstream service timed out",
        ) from external_error
    except httpx.HTTPError as external_error:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream service unavailable",
        ) from external_error
    except CircuitBreakerOpenError as external_error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Upstream service temporarily unavailable",
        ) from external_error

    return ExternalServiceResponse(
        status="available",
    )