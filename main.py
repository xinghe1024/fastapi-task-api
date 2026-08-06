from fastapi import FastAPI
from routers.tasks import router as task_router
from routers.auth import router as auth_router
from routers.health import router as health_router

from config import get_settings
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from database import engine
from middlewares import register_middlewares
from exception_handlers import (
    register_exception_handlers,
)


@asynccontextmanager
async def lifespan(
        _app: FastAPI,
) -> AsyncGenerator[None]:
    try:
        yield
    finally:
        engine.dispose()


app = FastAPI(lifespan=lifespan)

register_exception_handlers(app)
register_middlewares(
    app,
    get_settings().cors_allowed_origins,
)
app.include_router(task_router)
app.include_router(auth_router)
app.include_router(health_router)
