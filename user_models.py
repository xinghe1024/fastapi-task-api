from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

class UserCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )

    username: Annotated[
        str,
        Field(min_length=3,max_length =50),
    ]
    password: Annotated[
        str,
        Field(min_length=8,max_length= 128),
    ]

class UserResponse(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
    )

    id: int
    username: str
    is_active: bool

class TokenResponse(BaseModel):
    access_token: str
    token_type: str