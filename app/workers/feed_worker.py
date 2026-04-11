"""Асинхронный воркер для fan-out ленты через Redis Streams.

Читает события из stream `post_events` в consumer group `feed_workers`,
выполняет fan-out поста по фолловерам автора (или удаление из фидов),
затем XACK. Это обязательно для high-fanout пользователей — иначе handler
`/post/create` блокируется надолго.

Запуск:
    python -m app.workers.feed_worker
"""
from __future__ import annotations

import asyncio
import signal
from datetime import datetime
from typing import Any
from uuid import UUID

from app.config import settings
from app.database import Database
from app.feed_cache import fan_out_post, remove_post_from_all_feeds
from app.redis_client import RedisClient

_shutdown = asyncio.Event()


def _install_signal_handlers() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown.set)


async def _ensure_group(redis: Any) -> None:
    """Создаёт consumer group, если её ещё нет."""
    try:
        await redis.xgroup_create(
            name=settings.feed_stream_key,
            groupname=settings.feed_stream_group,
            id="0",
            mkstream=True,
        )
        print(f"[worker] Создана consumer group {settings.feed_stream_group}")
    except Exception as exc:  # noqa: BLE001
        # BUSYGROUP — группа уже есть, это норма
        if "BUSYGROUP" in str(exc):
            return
        raise


async def _handle_event(redis: Any, fields: dict[str, str]) -> None:
    event_type = fields.get("event_type", "")
    try:
        post_id = UUID(fields["post_id"])
        author_id = UUID(fields["author_id"])
    except (KeyError, ValueError) as exc:
        print(f"[worker] Пропускаем событие с битыми полями: {fields!r} ({exc})")
        return

    if event_type == "created":
        created_at = datetime.fromisoformat(fields["created_at"])
        await fan_out_post(redis, post_id, author_id, created_at)
    elif event_type == "deleted":
        await remove_post_from_all_feeds(redis, post_id, author_id)
    else:
        print(f"[worker] Неизвестный event_type: {event_type!r}")


async def _drain_entries(
    redis: Any,
    stream: str,
    group: str,
    entries: list[tuple[str, dict[str, str]]],
) -> None:
    """Обрабатывает список записей из XREADGROUP и XACK'ает успешные."""
    for msg_id, fields in entries:
        try:
            await _handle_event(redis, fields)
            await redis.xack(stream, group, msg_id)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[worker] Ошибка обработки {msg_id}: {exc}. "
                "Сообщение остаётся в pending для повторной обработки.",
                flush=True,
            )
            # Не ACK — сообщение останется в PEL и будет переобработано
            # при следующем запуске или через autoclaim в будущем.


async def _recover_pending(
    redis: Any,
    stream: str,
    group: str,
    consumer: str,
    batch: int,
) -> None:
    """До старта основного цикла забираем свои pending сообщения.

    Сценарий: воркер упал посреди обработки, сообщение ACK не получило,
    осталось в PEL. При рестарте под тем же consumer name нужно сначала
    вычитать свой хвост (XREADGROUP id='0') и только потом переключаться
    на новые записи (id='>').
    """
    cursor = "0"
    while True:
        try:
            messages = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: cursor},
                count=batch,
                block=0,  # 0 — не блокируем, сразу возвращаем что есть
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[worker] Recovery XREADGROUP ошибка: {exc}", flush=True)
            return

        if not messages:
            print("[worker] Pending recovery завершён", flush=True)
            return

        total = 0
        for _stream_name, entries in messages:
            total += len(entries)
            await _drain_entries(redis, stream, group, entries)
            if entries:
                cursor = entries[-1][0]

        if total == 0:
            return


async def _process_loop() -> None:
    redis = RedisClient.get()
    await _ensure_group(redis)

    stream = settings.feed_stream_key
    group = settings.feed_stream_group
    consumer = settings.feed_stream_consumer
    block_ms = settings.feed_stream_block_ms
    batch = settings.feed_stream_batch

    print(
        f"[worker] Старт consumer {consumer} (group={group}, stream={stream})",
        flush=True,
    )

    # Перед основным циклом забираем собственные pending сообщения
    await _recover_pending(redis, stream, group, consumer, batch)

    while not _shutdown.is_set():
        try:
            # Читаем только новые сообщения (id='>' — только для consumer group)
            messages = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=batch,
                block=block_ms,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[worker] XREADGROUP ошибка: {exc}. Повтор через 1s.", flush=True)
            await asyncio.sleep(1)
            continue

        if not messages:
            continue

        # messages формат: [(stream_name, [(msg_id, {fields}), ...])]
        for _stream_name, entries in messages:
            await _drain_entries(redis, stream, group, entries)

    print("[worker] Shutdown requested, выходим.", flush=True)


async def main() -> None:
    await Database.connect()
    await RedisClient.connect()
    _install_signal_handlers()
    try:
        await _process_loop()
    finally:
        await RedisClient.disconnect()
        await Database.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
