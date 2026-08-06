from typing import Annotated, Any
from datetime import date

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)


class TaskBase(BaseModel):
    title: Annotated[
        str,
        Field(min_length=2, max_length=100),
    ]
    description: Annotated[
        str | None,
        Field(max_length=500),
    ] = None
    priority: Annotated[
        int,
        Field(ge=1, le=5),
    ] = 1
    due_date: date | None = None

class TaskCreate(TaskBase):
    model_config = ConfigDict(extra="forbid")


class TaskUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Annotated[
        str | None,
        Field(min_length=2, max_length=100),
    ] = None
    description: Annotated[
        str | None,
        Field(max_length=500),
    ] = None
    priority: Annotated[
        int | None,
        Field(ge=1, le=5),
    ] = None
    completed: bool | None = None
    due_date: date | None = None

    @field_validator(
        "title",
        "priority",
        "completed",
        mode="before",
    )
    @classmethod
    def reject_null(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("Field cannot be null")

        return value


class TaskResponse(TaskBase):
    model_config = ConfigDict(
        from_attributes=True,
    )

    id: int
    completed: bool

