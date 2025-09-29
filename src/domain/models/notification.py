from typing import Any
from pydantic import BaseModel, Field


class NotificationData(BaseModel):
    session_id: str = Field(
        ...,
        description="identificador de la sesión",
        serialization_alias="sessionId",
    )
    type: str = Field(
        ...,
        description="tipo de notificación",
        serialization_alias="type",
    )
    data: dict[str, Any] = Field(
        ...,
        description="datos del mensaje",
        serialization_alias="data",
    )


class Notification(BaseModel):
    id: str = Field(
        ..., serialization_alias="id", description="identificador único del mensaje"
    )
    message: NotificationData = Field(
        ..., serialization_alias="message", description="mensaje"
    )
