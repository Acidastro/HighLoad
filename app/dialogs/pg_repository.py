"""DialogRepository поверх шардированного Postgres из ДЗ-5.

Переносит SQL-код, который раньше жил в routes/dialogs.py, за фасад
DialogRepository. Поведение, контракт, ON CONFLICT, dual-write — всё то же,
что было в HW5. Из API ничего не выпадает.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from app.dialogs.cluster import DialogsCluster
from app.dialogs.repository import DialogMessageDTO
from app.dialogs.sharding import compute_chat_id


class PgDialogRepository:
    """Шардированный Postgres как хранилище диалогов."""

    async def send(self, from_id: UUID, to_id: UUID, text: str) -> None:
        chat_id = compute_chat_id(from_id, to_id)
        # message_id один на сообщение — ключ идемпотентности для dual-write
        # и backfill из ДЗ-5 (см. routes/dialogs.py историю).
        message_id = uuid4()
        for pool in DialogsCluster.write_pools_for_chat(chat_id):
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
                    from_id,
                    to_id,
                    text,
                )

    async def list(
        self,
        user_a: UUID,
        user_b: UUID,
        limit: int,
        offset: int,
    ) -> list[DialogMessageDTO]:
        chat_id = compute_chat_id(user_a, user_b)
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
            DialogMessageDTO(
                from_user_id=row["sender_id"],
                to_user_id=row["recipient_id"],
                text=row["body"],
                created_at=row["created_at"],
            )
            for row in rows
        ]
