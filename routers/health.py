from typing import Annotated, Literal

from pydantic import BaseModel
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)

from redis.asyncio import Redis
from redis.exceptions import RedisError

from redis_dependencies import get_redis_client
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from dependencies import get_session


class HealthResponse(BaseModel):
    status: Literal["ok"]


router = APIRouter(
    prefix="/health",
    tags=["health"],
)

@router.get(
    "/live",
    response_model=HealthResponse,
)
def check_liveness() -> HealthResponse:
    return HealthResponse(status="ok")

@router.get(
    "/ready",
    response_model=HealthResponse,
)
def check_readiness(
        session: Annotated[
            Session,
            Depends(get_session),
        ],
) -> HealthResponse:
    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError as database_error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from database_error

    return HealthResponse(status="ok")

@router.get(
    "/redis",
    response_model=HealthResponse,
)
async def check_redis_health(
    redis_client: Annotated[
        Redis,
        Depends(get_redis_client),
    ],
) -> HealthResponse:
    try:
        await redis_client.ping()
    except RedisError as redis_error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis unavailable",
        ) from redis_error

    return HealthResponse(status="ok")