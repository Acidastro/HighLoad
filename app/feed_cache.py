"""Helpers для работы с Redis-кэшем ленты постов.

Схема ключей:
- feed:{user_id}  — Sorted Set, score=unix_timestamp, member=post_id
- post:{post_id}  — Hash с полями text, author_id, created_at (ISO)

При создании поста происходит fan-out-on-write: пост добавляется в
sorted set каждого фолловера автора. Размер ленты ограничен
settings.feed_max_size (по умолчанию 1000).

Все SQL-чтения здесь идут с MASTER, а не со slave. Причина: эти функции
вызываются на write-path (fan-out, add_friend_posts) и на cache-miss rebuild.
Slave lag при async-репликации может привести к воскрешению только что
удалённых постов в ленте (rebuild прочитает стейл slave → HSET post hash
обратно → пост снова "живой"). Читаем с мастера — корректнее, а редкость
этих вызовов делает нагрузку приемлемой.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from app.config import settings
from app.database import Database

# Redis client типизируем как Any: стабы redis-py ломают pyright на async-методах
RedisAny = Any

# TTL для post:{id} hash — ограничивает монотонный рост памяти Redis,
# eviction policy allkeys-lru почистит остатки под давлением
POST_HASH_TTL_SECONDS = 30 * 24 * 3600  # 30 дней


def feed_key(user_id: UUID) -> str:
    return f"feed:{user_id}"


def post_key(post_id: UUID) -> str:
    return f"post:{post_id}"


def _row_to_post_hash(row: Any) -> dict[str, str]:
    created_at: datetime = row["created_at"]
    return {
        "text": row["text"],
        "author_id": str(row["author_id"]),
        "created_at": created_at.isoformat(),
    }


async def store_post_hash(
    redis: RedisAny,
    post_id: UUID,
    text: str,
    author_id: UUID,
    created_at: datetime,
) -> None:
    """Кладёт тело поста в Redis Hash и ставит TTL."""
    key = post_key(post_id)
    async with redis.pipeline(transaction=False) as pipe:
        pipe.hset(
            key,
            mapping={
                "text": text,
                "author_id": str(author_id),
                "created_at": created_at.isoformat(),
            },
        )
        pipe.expire(key, POST_HASH_TTL_SECONDS)
        await pipe.execute()


async def delete_post_hash(redis: RedisAny, post_id: UUID) -> None:
    await redis.delete(post_key(post_id))


async def get_followers(author_id: UUID) -> list[UUID]:
    """Возвращает список user_id, которые добавили author_id в друзья.

    Эти пользователи увидят пост автора в своей ленте (fan-out targets).
    """
    async with Database.master_connection() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM friendships WHERE friend_id = $1",
            author_id,
        )
    return [row["user_id"] for row in rows]


async def fan_out_post(
    redis: RedisAny,
    post_id: UUID,
    author_id: UUID,
    created_at: datetime,
) -> None:
    """Добавляет пост в ленту всех фолловеров автора и сохраняет его тело."""
    followers = await get_followers(author_id)
    if not followers:
        return

    score = created_at.timestamp()
    post_id_str = str(post_id)
    cap = settings.feed_max_size

    async with redis.pipeline(transaction=False) as pipe:
        for follower_id in followers:
            key = feed_key(follower_id)
            pipe.zadd(key, {post_id_str: score})
            pipe.zremrangebyrank(key, 0, -(cap + 1))
        await pipe.execute()


async def publish_post_event(
    redis: RedisAny,
    event_type: str,
    post_id: UUID,
    author_id: UUID,
    created_at: datetime,
) -> None:
    """Публикует событие в Redis Stream для асинхронного fan-out.

    event_type: created | deleted
    """
    await redis.xadd(
        settings.feed_stream_key,
        {
            "event_type": event_type,
            "post_id": str(post_id),
            "author_id": str(author_id),
            "created_at": created_at.isoformat(),
        },
    )


async def remove_post_from_all_feeds(
    redis: RedisAny,
    post_id: UUID,
    author_id: UUID,
) -> None:
    """Удаляет пост из лент всех фолловеров (вызывается при delete поста)."""
    followers = await get_followers(author_id)
    if not followers:
        return

    post_id_str = str(post_id)
    async with redis.pipeline(transaction=False) as pipe:
        for follower_id in followers:
            pipe.zrem(feed_key(follower_id), post_id_str)
        await pipe.execute()


async def add_friend_posts_to_feed(
    redis: RedisAny,
    user_id: UUID,
    friend_id: UUID,
) -> None:
    """При добавлении друга — подмешиваем его последние посты в ленту user_id."""
    cap = settings.feed_max_size
    async with Database.master_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT id, text, author_id, created_at
            FROM posts
            WHERE author_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            """,
            friend_id,
            cap,
        )
    if not rows:
        return

    key = feed_key(user_id)
    async with redis.pipeline(transaction=False) as pipe:
        for row in rows:
            post_id_str = str(row["id"])
            score = row["created_at"].timestamp()
            pipe.zadd(key, {post_id_str: score})
            pipe.hset(
                post_key(row["id"]),
                mapping=_row_to_post_hash(row),
            )
            pipe.expire(post_key(row["id"]), POST_HASH_TTL_SECONDS)
        pipe.zremrangebyrank(key, 0, -(cap + 1))
        await pipe.execute()


async def remove_friend_posts_from_feed(
    redis: RedisAny,
    user_id: UUID,
    friend_id: UUID,
) -> None:
    """При удалении друга — вычищаем его посты из ленты user_id.

    Сканируем только содержимое самой ленты (<= feed_max_size элементов),
    а не все посты автора за всю историю. Для каждого post_id из ленты
    смотрим author_id в post:{id} hash и ZREM те, у кого автор == friend_id.
    """
    key = feed_key(user_id)
    post_ids: list[str] = await redis.zrange(key, 0, -1)
    if not post_ids:
        return

    async with redis.pipeline(transaction=False) as pipe:
        for pid in post_ids:
            pipe.hget(post_key(UUID(pid)), "author_id")
        authors: list[str | None] = await pipe.execute()

    friend_id_str = str(friend_id)
    to_remove = [
        pid
        for pid, author in zip(post_ids, authors, strict=True)
        if author == friend_id_str
    ]
    if to_remove:
        await redis.zrem(key, *to_remove)


async def rebuild_feed_for_user(
    redis: RedisAny,
    user_id: UUID,
) -> int:
    """Перестраивает ленту пользователя из БД.

    Используется при cache miss и в endpoint /post/feed/rebuild.
    Возвращает число добавленных постов.

    Атомарность: наполняем временный ключ `feed:{uid}:tmp:{nonce}` и
    делаем RENAME на боевой. Это исключает гонку "concurrent reader видит
    feed пустым между delete и zadd" и гонку "fan_out_post вклинился
    в середину rebuild". RENAME в Redis — O(1).
    """
    cap = settings.feed_max_size
    async with Database.master_connection() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.text, p.author_id, p.created_at
            FROM posts p
            INNER JOIN friendships f ON f.friend_id = p.author_id
            WHERE f.user_id = $1
            ORDER BY p.created_at DESC
            LIMIT $2
            """,
            user_id,
            cap,
        )

    key = feed_key(user_id)

    if not rows:
        # Пустая лента — стираем ключ (может остался с прошлого состояния)
        await redis.delete(key)
        return 0

    tmp_key = f"{key}:rebuild:{uuid4().hex}"
    try:
        async with redis.pipeline(transaction=False) as pipe:
            for row in rows:
                post_id = row["id"]
                score = row["created_at"].timestamp()
                pipe.zadd(tmp_key, {str(post_id): score})
                pipe.hset(
                    post_key(post_id),
                    mapping=_row_to_post_hash(row),
                )
                pipe.expire(post_key(post_id), POST_HASH_TTL_SECONDS)
            await pipe.execute()

        # Атомарная замена: RENAME перезатирает target ключ одной командой
        await redis.rename(tmp_key, key)
    finally:
        # Если RENAME не дошёл (исключение) — подчищаем мусор
        await redis.delete(tmp_key)

    return len(rows)


async def read_feed(
    redis: RedisAny,
    user_id: UUID,
    offset: int,
    limit: int,
) -> list[dict[str, str]]:
    """Читает срез ленты. На cache miss перестраивает из БД.

    Возвращает список dict-ов с ключами id, text, author_user_id.
    Если тело поста отсутствует в Redis (удалено или вытеснено LRU),
    пост пропускается — ZREM'им его сразу, чтобы не мусорил в sorted set.
    """
    key = feed_key(user_id)
    exists = await redis.exists(key)
    if not exists:
        await rebuild_feed_for_user(redis, user_id)

    # ZREVRANGE: от самых свежих к старым
    end = offset + limit - 1
    post_ids: list[str] = await redis.zrevrange(key, offset, end)
    if not post_ids:
        return []

    async with redis.pipeline(transaction=False) as pipe:
        for pid in post_ids:
            pipe.hgetall(post_key(UUID(pid)))
        hashes: list[dict[str, str]] = await pipe.execute()

    result: list[dict[str, str]] = []
    stale_ids: list[str] = []
    for pid, h in zip(post_ids, hashes, strict=True):
        if not h:
            stale_ids.append(pid)
            continue
        result.append({
            "id": pid,
            "text": h["text"],
            "author_user_id": h["author_id"],
        })

    # Чистим "призраков" из sorted set — их тела нет, значит пост удалён или вытеснен
    if stale_ids:
        await redis.zrem(key, *stale_ids)

    return result
