"""Routes подсистемы диалогов.

После HW7 здесь — только тонкий HTTP-слой: валидация, проверка прав,
маппинг в DTO. Вся бизнес-логика — в DialogRepository, конкретная реализация
выбирается env-флагом DIALOGS_BACKEND (postgres/tarantool) через factory.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.dialogs.factory import get_dialog_repository
from app.dialogs.repository import DialogRepository

router = APIRouter()


class SendMessageRequest(BaseModel):
    text: str = Field(min_length=1, max_length=10_000)


class SendMessageResponse(BaseModel):
    status: str


class DialogMessage(BaseModel):
    from_user_id: UUID
    to_user_id: UUID
    text: str
    created_at: datetime


@router.post(
    "/dialog/{user_id}/send",
    response_model=SendMessageResponse,
    status_code=status.HTTP_200_OK,
)
async def send_message(
    user_id: UUID,
    body: SendMessageRequest,
    current_user: UUID = Depends(get_current_user),
    repo: DialogRepository = Depends(get_dialog_repository),
) -> SendMessageResponse:
    if user_id == current_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot send a message to yourself",
        )
    await repo.send(current_user, user_id, body.text)
    return SendMessageResponse(status="ok")


@router.get(
    "/dialog/{user_id}/list",
    response_model=list[DialogMessage],
    status_code=status.HTTP_200_OK,
)
async def list_messages(
    user_id: UUID,
    current_user: UUID = Depends(get_current_user),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    repo: DialogRepository = Depends(get_dialog_repository),
) -> list[DialogMessage]:
    rows = await repo.list(current_user, user_id, limit, offset)
    return [
        DialogMessage(
            from_user_id=r.from_user_id,
            to_user_id=r.to_user_id,
            text=r.text,
            created_at=r.created_at,
        )
        for r in rows
    ]
