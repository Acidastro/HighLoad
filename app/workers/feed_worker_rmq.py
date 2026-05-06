"""Feed materializer worker (homework 6, урок 7) — RabbitMQ-версия.

Слушает durable очередь `feed.materialize` (привязанную к topic exchange
`posts.events` с ключом `feed.materialize`).

На каждое сообщение `event_type=created`:
  1. Берём список фолловеров автора.
  2. Если автор — celebrity (followers >= threshold) — skip push (см. урок 10).
  3. Иначе:
       a) Материализуем пост в feed:{follower} sorted set + post:{id} hash.
       b) Публикуем targeted event с routing_key=feed.user.{follower} —
          его поймает per-instance очередь WS-инстанса с подходящим binding,
          и пушнет клиенту.

На `event_type=deleted` — убираем пост из всех лент.

Запуск: python -m app.workers.feed_worker_rmq
Несколько копий = competing consumers (одно сообщение — один воркер).
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import datetime
from typing import Any
from uuid import UUID

from app.config import settings
from app.database import Database
from app.feed_cache import (
    fan_out_post,
    post_key,
    remove_post_from_all_feeds,
    store_post_hash,
)
from app.rabbit_client import RabbitClient, user_routing_key
from app.redis_client import RedisClient
from app.services.celebrity import is_celebrity

logger = logging.getLogger(__name__)
_shutdown = asyncio.Event()


def _install_signal_handlers() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown.set)


async def _publish_ws_event(follower: UUID, payload: dict[str, Any]) -> None:
    """Целевая публикация для WS-доставки. NOT_PERSISTENT — потеря допустима."""
    await RabbitClient.publish(
        user_routing_key(follower),
        payload,
        persistent=False,
    )


async def _handle_created(redis: Any, body: dict[str, Any]) -> None:
    post_id = UUID(body["post_id"])
    author_id = UUID(body["author_id"])
    created_at = datetime.fromisoformat(body["created_at"])
    text: str = body.get("text") or ""

    # Тело поста — на случай если producer не успел положить (race) или TTL вышел
    if text:
        await store_post_hash(redis, post_id, text, author_id, created_at)

    if await is_celebrity(redis, author_id):
        # Lady Gaga effect: не материализуем push-ленту,
        # подписчики получат пост через pull при чтении ленты.
        logger.info("Skip fan-out for celebrity author=%s post=%s", author_id, post_id)
        return

    # 1) push в Redis-ленту (использует имеющийся helper HW4)
    await fan_out_post(redis, post_id, author_id, created_at)

    # 2) targeted WS-нотификации каждому фолловеру
    from app.feed_cache import get_followers  # локально, чтобы избежать циклов

    followers = await get_followers(author_id)
    if not followers:
        return

    # Подгружаем тело из Redis hash (если ничего не было передано в body)
    if not text:
        cached = await redis.hget(post_key(post_id), "text")
        text = cached or ""

    payload = {
        "type": "post.new",
        "post_id": str(post_id),
        "author_id": str(author_id),
        "text_preview": text[:200],
        "created_at": created_at.isoformat(),
    }
    # Параллельные публикации; gather без return_exceptions = упадём, ack не сделаем
    await asyncio.gather(
        *[_publish_ws_event(follower, payload) for follower in followers],
    )


async def _handle_deleted(redis: Any, body: dict[str, Any]) -> None:
    post_id = UUID(body["post_id"])
    author_id = UUID(body["author_id"])
    await remove_post_from_all_feeds(redis, post_id, author_id)


async def _handle_message(redis: Any, raw: bytes) -> None:
    body = json.loads(raw)
    event_type = body.get("event_type")
    if event_type == "created":
        await _handle_created(redis, body)
    elif event_type == "deleted":
        await _handle_deleted(redis, body)
    else:
        logger.warning("Неизвестный event_type: %r", event_type)


async def _consume_loop() -> None:
    redis = RedisClient.get()
    connection = RabbitClient.connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=settings.rabbitmq_prefetch)

    # Declare exchange (idempotent)
    exchange = await channel.declare_exchange(
        settings.rabbitmq_exchange,
        type="topic",
        durable=True,
    )
    # Durable queue для материализации; production: arguments={"x-queue-type":"quorum"}
    queue = await channel.declare_queue(
        settings.rabbitmq_materialize_queue,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await queue.bind(exchange, routing_key=settings.rabbitmq_materialize_routing_key)

    logger.info(
        "Feed worker started: queue=%s, prefetch=%d",
        settings.rabbitmq_materialize_queue,
        settings.rabbitmq_prefetch,
    )

    async with queue.iterator() as it:
        async for message in it:
            if _shutdown.is_set():
                break
            try:
                await _handle_message(redis, message.body)
                await message.ack()
            except Exception:  # noqa: BLE001
                logger.exception("Ошибка обработки message — nack+requeue")
                # nack + requeue: сообщение придёт снова. Без dead-letter
                # это может крутиться вечно при отравленном сообщении —
                # для production обернуть в DLQ.
                await message.nack(requeue=True)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    await Database.connect()
    await RedisClient.connect()
    await RabbitClient.connect()
    _install_signal_handlers()
    try:
        await _consume_loop()
    finally:
        await RabbitClient.disconnect()
        await RedisClient.disconnect()
        await Database.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
