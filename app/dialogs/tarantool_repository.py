"""DialogRepository поверх Tarantool (homework 7).

Общение ТОЛЬКО через UDF (dialog_send / dialog_list) — никаких прямых
обращений к space через драйвер. Это требование ДЗ и проверка того, что
схема Tarantool — частная деталь реализации.

UDF возвращают timestamp в миллисекундах (clock.realtime() * 1000),
конвертируем в datetime для общего DTO.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from app.dialogs.repository import DialogMessageDTO
from app.dialogs.tarantool_client import TarantoolPool


class TarantoolDialogRepository:
    def __init__(self, pool: TarantoolPool) -> None:
        self._pool = pool

    async def send(self, from_id: UUID, to_id: UUID, text: str) -> None:
        # UUID шлём как строку — Tarantool format ожидает string.
        await self._pool.call(
            "dialog_send",
            [str(from_id), str(to_id), text],
        )

    async def list(
        self,
        user_a: UUID,
        user_b: UUID,
        limit: int,
        offset: int,
    ) -> list[DialogMessageDTO]:
        # asynctnt всегда возвращает list (iproto заворачивает результат
        # функции в массив значений). У нас функция возвращает ровно одно
        # значение — массив сообщений; достаём через [0].
        response = await self._pool.call(
            "dialog_list",
            [str(user_a), str(user_b), limit, offset],
        )
        rows: list[dict[str, Any]] = response[0] if response else []
        return [_row_to_dto(row) for row in rows]


def _row_to_dto(row: dict[str, Any]) -> DialogMessageDTO:
    return DialogMessageDTO(
        from_user_id=UUID(row["from_user_id"]),
        to_user_id=UUID(row["to_user_id"]),
        text=row["text"],
        created_at=datetime.fromtimestamp(row["created_at"] / 1000, tz=timezone.utc),
    )
