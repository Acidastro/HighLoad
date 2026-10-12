"""RabbitMQ client (homework 6 — realtime feed).

Singleton-обёртка над aio_pika.connect_robust:
- Один robust-connection на процесс (auto-reconnect под капотом).
- Один publisher channel в режиме confirms — для безопасных публикаций.
- Помощник publish_event() для FastAPI handlers и воркеров.

Топология (см. docs/tdd/homework_6_websocket_feed.md, урок 5):
- exchange:  posts.events  (topic, durable)
- queue:     feed.materialize (durable, читает feed-worker)
- queue:     ws.<instance>  (exclusive, auto_delete — создаёт WS-инстанс)

Routing keys:
- feed.materialize    — задача на материализацию ленты
- feed.user.<S>       — целевое событие для подписчика S (доставка через WS)
"""
from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustExchange,
)

from app.config import settings


class _UUIDEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, UUID):
            return str(o)
        return super().default(o)


def _dumps(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, cls=_UUIDEncoder, separators=(",", ":")).encode()


class RabbitClient:
    """Process-wide singleton для RabbitMQ.

    Используется и приложением (FastAPI lifespan), и воркером.
    """

    _connection: AbstractRobustConnection | None = None
    _publisher_channel: AbstractRobustChannel | None = None
    _publisher_exchange: AbstractRobustExchange | None = None
    _lock = asyncio.Lock()

    @classmethod
    async def connect(cls) -> None:
        if cls._connection is not None:
            return
        async with cls._lock:
            if cls._connection is not None:
                return
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            cls._connection = connection
            channel = await connection.channel(publisher_confirms=True)
            await channel.set_qos(prefetch_count=settings.rabbitmq_prefetch)
            exchange = await channel.declare_exchange(
                settings.rabbitmq_exchange,
                ExchangeType.TOPIC,
                durable=True,
            )
            cls._publisher_channel = channel
            cls._publisher_exchange = exchange

    @classmethod
    async def disconnect(cls) -> None:
        if cls._connection is None:
            return
        try:
            await cls._connection.close()
        finally:
            cls._connection = None
            cls._publisher_channel = None
            cls._publisher_exchange = None

    @classmethod
    def connection(cls) -> AbstractRobustConnection:
        if cls._connection is None:
            raise RuntimeError("RabbitClient is not connected")
        return cls._connection

    @classmethod
    def exchange(cls) -> AbstractRobustExchange:
        if cls._publisher_exchange is None:
            raise RuntimeError("RabbitClient is not connected")
        return cls._publisher_exchange

    @classmethod
    async def publish(
        cls,
        routing_key: str,
        payload: dict[str, Any],
        *,
        persistent: bool = True,
    ) -> None:
        """Публикация в общий topic exchange posts.events.

        persistent=True для durable-сообщений (feed.materialize),
        persistent=False для эфемерных WS-нотификаций (feed.user.<S>) —
        потеря допустима, клиент при reconnect перезапросит ленту.
        """
        exchange = cls.exchange()
        message = Message(
            body=_dumps(payload),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
        )
        await exchange.publish(message, routing_key=routing_key)


def user_routing_key(user_id: UUID | str) -> str:
    return f"{settings.rabbitmq_user_routing_prefix}.{user_id}"
