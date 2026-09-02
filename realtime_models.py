from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class ClientBroadcastMessage(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )

    content: Annotated[
        str,
        Field(min_length=1, max_length=500),
    ]


class RealtimeMessageEvent(BaseModel):
    type: Literal["message"] = "message"
    content: str


class UserRealtimeEventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: Annotated[
        int,
        Field(ge=1),
    ]
    event: RealtimeMessageEvent