from typing import Annotated
from sqlalchemy.exc import IntegrityError
from fastapi.security import OAuth2PasswordRequestForm

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
)

from sqlalchemy import select
from sqlalchemy.orm import Session

from user_models import (
    TokenResponse,
    UserCreate,
    UserResponse,
)

from authentication import (
    authenticate_user,
    get_current_user,
)

from authentication import authenticate_user
from config import Settings, get_settings
from database_models import UserRecord
from dependencies import get_session
from passwords import hash_password
from user_models import UserCreate, UserResponse

from security import create_access_token, oauth2_scheme


router = APIRouter(
    prefix="/auth",
    tags=["auth"],
)

@router.get("/me")
def read_current_user(
    current_user: Annotated[
        UserRecord,
        Depends(get_current_user),
    ],
) -> UserResponse:
    return UserResponse.model_validate(
        current_user,
    )


@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
)
def register_user(
        user_create: UserCreate,
        session: Annotated[
            Session,
            Depends(get_session),
        ],
) -> UserResponse:
    statement = select(UserRecord).where(
        UserRecord.username == user_create.username,
    )
    existing_user = session.scalar(statement)

    if existing_user is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already registered",
        )

    user_record = UserRecord(
        username = user_create.username,
        hashed_password = hash_password(
            user_create.password,
        ),
        is_active = True,
    )

    session.add(user_record)

    try:
        session.commit()
    except IntegrityError as error:
        session.rollback()

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already registered",
        ) from error

    session.refresh(user_record)

    return UserResponse.model_validate(
        user_record,
    )

@router.post(
    '/token'
)
def login_for_access_token(
        form_data: Annotated[
            OAuth2PasswordRequestForm,
            Depends(),
        ],
        session: Annotated[
            Session,
            Depends(get_session),
        ],
        settings: Annotated[
            Settings,
            Depends(get_settings),
        ],
) -> TokenResponse:
    user_record = authenticate_user(
        session = session,
        username = form_data.username,
        plain_password = form_data.password,
    )

    if user_record is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={
                "WWW-Authenticate": "Bearer",
            },
        )

    access_token = create_access_token(
        subject = str(user_record.id),
        settings = settings,
    )

    return TokenResponse(
        access_token = access_token,
        token_type = "bearer",
    )