"""RabbitMQ ⇄ WebSocket bridge (homework 6, урок 8).

Один экземпляр на процесс:
- Создаёт persistent channel (отдельный от publisher channel из RabbitClient).
- Объявляет ОДНУ exclusive auto-delete queue для всего инстанса.
- На connect юзера → queue.bind(exchange, routing_key=feed.user.<uid>).
- На disconnect (последнего коннекта) → queue.unbind(...).
- Постоянный consumer читает queue и переадресует payload в ConnectionManager.

Идея: RabbitMQ маршрутизирует копию сообщения только тем инстансам,
у которых есть подходящий binding. Каждый инстанс «слышит» только своих
подписчиков → линейная масштабируемость (урок 9).
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any
from uuid import UUID

from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustExchange,
    AbstractRobustQueue,
)

from app.config import settings
from app.rabbit_client import user_routing_key
from app.ws.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class RabbitWsBridge:
    """Привязывает per-instance очередь к exchange и шлёт события в ConnectionManager."""

    def __init__(self, manager: ConnectionManager) -> None:
        self._manager = manager
        self._channel: AbstractRobustChannel | None = None
        self._queue: AbstractRobustQueue | None = None
        self._exchange: AbstractRobustExchange | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._instance_id = uuid.uuid4().hex[:8]

    async def start(self, connection: AbstractRobustConnection) -> None:
        """Создаём channel, exclusive queue, подписываемся."""
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=settings.rabbitmq_prefetch)
        exchange = await channel.declare_exchange(
            settings.rabbitmq_exchange,
            type="topic",
            durable=True,
        )
        # Имя пустое → брокер сгенерирует уникальное; exclusive+auto_delete
        queue = await channel.declare_queue(
            name=f"ws.{self._instance_id}",
            exclusive=True,
            auto_delete=True,
        )
        self._channel = channel
        self._exchange = exchange
        self._queue = queue
        self._consumer_task = asyncio.create_task(
            self._consume_loop(),
            name=f"rabbit-ws-bridge-{self._instance_id}",
        )
        logger.info("RabbitWsBridge started: instance_id=%s", self._instance_id)

    async def stop(self) -> None:
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._channel is not None:
            await self._channel.close()
        self._channel = None
        self._queue = None
        self._exchange = None
        self._consumer_task = None

    async def bind_user(self, user_id: UUID) -> None:
        """Подписать per-instance очередь на сообщения для конкретного user_id."""
        if self._queue is None:
            return
        await self._queue.bind(
            settings.rabbitmq_exchange,
            routing_key=user_routing_key(user_id),
        )

    async def unbind_user(self, user_id: UUID) -> None:
        if self._queue is None:
            return
        try:
            await self._queue.unbind(
                settings.rabbitmq_exchange,
                routing_key=user_routing_key(user_id),
            )
        except Exception:  # noqa: BLE001
            logger.exception("unbind_user failed: %s", user_id)

    async def _consume_loop(self) -> None:
        if self._queue is None:
            return
        async with self._queue.iterator() as it:
            async for message in it:
                async with message.process(requeue=False):
                    try:
                        payload: dict[str, Any] = json.loads(message.body)
                    except (ValueError, json.JSONDecodeError):
                        logger.warning("Битый payload в WS bridge: %r", message.body)
                        continue
                    rk = message.routing_key or ""
                    # routing_key формата feed.user.<uuid>
                    parts = rk.split(".")
                    if len(parts) != 3:
                        logger.warning("Неожиданный routing_key: %s", rk)
                        continue
                    try:
                        user_id = UUID(parts[2])
                    except ValueError:
                        logger.warning("Невалидный user_id в routing_key: %s", rk)
                        continue
                    await self._manager.push(user_id, payload)
