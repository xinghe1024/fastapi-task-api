import logging
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import (
    RequestResponseEndpoint,
)

logger = logging.getLogger(__name__)

async def add_request_observability(
    request: Request,
    call_next: RequestResponseEndpoint,
) -> Response:
    request_id = uuid4().hex
    request.state.request_id = request_id
    start_time = perf_counter()

    try:
        response = await call_next(request)
    except Exception:
        process_time = perf_counter() - start_time

        logger.exception(
            (
                "Request failed: request_id=%s "
                "method=%s path=%s "
                "process_time=%.6f"
            ),
            request_id,
            request.method,
            request.url.path,
            process_time,
        )
        raise

    process_time = perf_counter() - start_time

    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = (
        f"{process_time:.6f}"
    )

    logger.info(
        (
            "Request completed: request_id=%s "
            "method=%s path=%s "
            "status_code=%s process_time=%.6f"
        ),
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        process_time,
    )

    return response

def register_middlewares(
    app: FastAPI,
    allowed_origins: list[str],
) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=[
            "GET",
            "POST",
            "PATCH",
            "DELETE",
        ],
        allow_headers=[
            "Authorization",
            "Content-Type",
        ],
        expose_headers=[
            "X-Process-Time",
            "X-Request-ID",
        ],
    )

    app.middleware("http")(
        add_request_observability
    )