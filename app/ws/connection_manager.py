"""ConnectionManager — реестр живых WebSocket-сессий внутри инстанса.

Зачем (урок 3): WS-сервис stateful — соединение привязано к процессу.
Один user_id может держать несколько коннектов (5 вкладок), поэтому
храним set[WebSocket]. Lock защищает от гонки add/remove vs broadcast.

При connect/disconnect вызываются callback'и (on_user_subscribe /
on_user_unsubscribe) — они нужны RabbitWsBridge для bind/unbind
конкретного routing_key feed.user.<uid> на per-instance очереди.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from fastapi import WebSocket

logger = logging.getLogger(__name__)

SubscribeCallback = Callable[[UUID], Awaitable[None]]


class ConnectionManager:
    """Process-local реестр активных WebSocket-коннектов."""

    def __init__(
        self,
        on_first_connect: SubscribeCallback | None = None,
        on_last_disconnect: SubscribeCallback | None = None,
    ) -> None:
        self._conns: dict[UUID, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._on_first_connect = on_first_connect
        self._on_last_disconnect = on_last_disconnect

    async def connect(self, user_id: UUID, ws: WebSocket) -> None:
        """Регистрирует новый коннект. Если это первый коннект юзера — зовёт callback (bind)."""
        is_first = False
        async with self._lock:
            if not self._conns[user_id]:
                is_first = True
            self._conns[user_id].add(ws)
        if is_first and self._on_first_connect is not None:
            try:
                await self._on_first_connect(user_id)
            except Exception:  # noqa: BLE001
                logger.exception("on_first_connect failed for %s", user_id)

    async def disconnect(self, user_id: UUID, ws: WebSocket) -> None:
        is_last = False
        async with self._lock:
            sockets = self._conns.get(user_id)
            if sockets is None:
                return
            sockets.discard(ws)
            if not sockets:
                self._conns.pop(user_id, None)
                is_last = True
        if is_last and self._on_last_disconnect is not None:
            try:
                await self._on_last_disconnect(user_id)
            except Exception:  # noqa: BLE001
                logger.exception("on_last_disconnect failed for %s", user_id)

    async def push(self, user_id: UUID, payload: dict[str, Any]) -> int:
        """Отправляет JSON-payload всем сокетам user_id. Возвращает число отправок.

        Битые сокеты (send упал) удаляются из реестра.
        """
        async with self._lock:
            sockets = list(self._conns.get(user_id, ()))
        if not sockets:
            return 0
        sent = 0
        broken: list[WebSocket] = []
        for ws in sockets:
            try:
                await ws.send_json(payload)
                sent += 1
            except Exception:  # noqa: BLE001
                broken.append(ws)
        if broken:
            for ws in broken:
                await self.disconnect(user_id, ws)
        return sent

    def active_users(self) -> list[UUID]:
        return list(self._conns.keys())

    def total_connections(self) -> int:
        return sum(len(s) for s in self._conns.values())
