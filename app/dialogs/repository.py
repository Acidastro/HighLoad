"""Интерфейс хранилища диалогов и общие DTO (homework 7).

Цель — отвязать роуты от конкретного бэкенда (Postgres/Citus или Tarantool).
В роуте используем только Protocol; конкретную реализацию даёт фабрика по
env-флагу DIALOGS_BACKEND. Это позволяет переключать "до/после" без правок
HTTP-слоя и снимать честный A/B бенчмарк.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel


class DialogMessageDTO(BaseModel):
    """Общий DTO между бэкендами. PG-репо строит его из asyncpg.Record,
    Tarantool-репо — из dict, который вернула Lua-UDF.
    """

    from_user_id: UUID
    to_user_id: UUID
    text: str
    created_at: datetime


class DialogRepository(Protocol):
    """Контракт, на который завязаны роуты диалогов.

    Имена и сигнатуры — минимально-достаточные. Никаких chat_id, shard_id,
    transactions — это детали реализаций.
    """

    async def send(self, from_id: UUID, to_id: UUID, text: str) -> None: ...

    async def list(
        self,
        user_a: UUID,
        user_b: UUID,
        limit: int,
        offset: int,
    ) -> list[DialogMessageDTO]: ...
