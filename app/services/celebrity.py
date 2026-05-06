"""Защита от Lady Gaga effect (homework 6, урок 10).

Push-fan-out плохо работает для авторов с миллионами подписчиков:
один пост = N writes в Redis + N сообщений в RabbitMQ. Решение —
гибридная модель:
  - Обычные авторы: push (fan-out on write).
  - Celebrity (followers >= threshold): skip push, ленту читателя
    дочитываем при запросе (pull / fan-out on read), см. урок 10.

is_celebrity() кэшируется в Redis с коротким TTL — счётчик меняется
относительно редко по сравнению с частотой публикаций.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from app.config import settings
from app.database import Database

RedisAny = Any


def _cache_key(author_id: UUID) -> str:
    return f"celebrity:{author_id}"


async def followers_count(author_id: UUID) -> int:
    async with Database.master_connection() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM friendships WHERE friend_id = $1",
            author_id,
        )
    return int(row["c"]) if row else 0


async def is_celebrity(redis: RedisAny, author_id: UUID) -> bool:
    """Возвращает True, если автор — celebrity (skip push fan-out).

    Кэшируется в Redis на settings.celebrity_cache_ttl секунд.
    """
    key = _cache_key(author_id)
    cached = await redis.get(key)
    if cached is not None:
        return cached == b"1" or cached == "1"

    cnt = await followers_count(author_id)
    flag = cnt >= settings.celebrity_followers_threshold
    await redis.set(key, "1" if flag else "0", ex=settings.celebrity_cache_ttl)
    return flag


async def invalidate_celebrity_cache(redis: RedisAny, author_id: UUID) -> None:
    """Сбросить кэш — вызывать при подписке/отписке если важна точность."""
    await redis.delete(_cache_key(author_id))
