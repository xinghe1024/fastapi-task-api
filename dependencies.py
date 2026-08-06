from collections.abc import Generator
from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database import SessionFactory
from database_models import TaskRecord


def get_session() -> Generator[Session, None, None]:
    session = SessionFactory()

    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def get_pagination(
    skip: Annotated[
        int,
        Query(ge=0),
    ] = 0,
    limit: Annotated[
        int,
        Query(ge=1, le=100),
    ] = 20,
) -> dict[str, int]:
    return {
        "skip": skip,
        "limit": limit,
    }