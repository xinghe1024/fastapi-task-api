from fastapi import Request

from circuit_breaker import CircuitBreaker


def get_external_service_circuit_breaker(
    request: Request,
) -> CircuitBreaker:
    return request.app.state.external_service_circuit_breaker