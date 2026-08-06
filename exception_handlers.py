from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse


async def handle_unexpected_exception(
    request: Request,
    _error: Exception,
) -> JSONResponse:
    request_id = getattr(
        request.state,
        "request_id",
        "unavailable",
    )

    return JSONResponse(
        status_code=(
            status.HTTP_500_INTERNAL_SERVER_ERROR
        ),
        content={
            "detail": "Internal server error",
        },
        headers={
            "X-Request-ID": request_id,
        },
    )


def register_exception_handlers(
    app: FastAPI,
) -> None:
    app.add_exception_handler(
        Exception,
        handle_unexpected_exception,
    )