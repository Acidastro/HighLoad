"""Routes подсистемы диалогов (homework 5).

Эндпоинты:
  POST /dialog/{user_id}/send — отправить сообщение пользователю user_id.
  GET  /dialog/{user_id}/list — получить переписку с user_id (итерация 5).

Логика роутинга по шардам полностью инкапсулирована в DialogsCluster:
здесь только бизнес-проверки, маппинг входа/выхода и SQL.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.dialogs.cluster import DialogsCluster
from app.dialogs.sharding import compute_chat_id

router = APIRouter()


class SendMessageRequest(BaseModel):
    text: str = Field(min_length=1, max_length=10_000)


class SendMessageResponse(BaseModel):
    status: str


class DialogMessage(BaseModel):
    """Один элемент выдачи /dialog/list. Поле `from`/`to` зарезервированы как
    keywords-друзья, поэтому используем `from_user_id`/`to_user_id`.
    """

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
) -> SendMessageResponse:
    if user_id == current_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot send a message to yourself",
        )

    chat_id = compute_chat_id(current_user, user_id)
    # message_id ОДИН на сообщение, даже если в dual-write пишем в два шарда.
    # Это и есть ключ идемпотентности для backfill: ON CONFLICT (message_id)
    # DO NOTHING спасает от дублей, если backfill повторно перенесёт строку,
    # которую только что вставил живой /send.
    message_id = uuid4()
    pools = DialogsCluster.write_pools_for_chat(chat_id)
    for pool in pools:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO messages
                    (message_id, chat_id, sender_id, recipient_id, body)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (message_id) DO NOTHING
                """,
                message_id,
                chat_id,
                current_user,
                user_id,
                body.text,
            )
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
) -> list[DialogMessage]:
    # Один и тот же chat_id и для записи (см. /send), и для чтения —
    # значит, /list всегда читает с одного шарда. Никакого scatter-gather.
    chat_id = compute_chat_id(current_user, user_id)
    async with DialogsCluster.connection_for_chat(chat_id) as conn:
        rows = await conn.fetch(
            """
            SELECT sender_id, recipient_id, body, created_at
            FROM messages
            WHERE chat_id = $1
            ORDER BY created_at DESC, id DESC
            LIMIT $2 OFFSET $3
            """,
            chat_id,
            limit,
            offset,
        )
    return [
        DialogMessage(
            from_user_id=row["sender_id"],
            to_user_id=row["recipient_id"],
            text=row["body"],
            created_at=row["created_at"],
        )
        for row in rows
    ]
