from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.auth import get_current_user
from app.config import settings
from app.database import Database
from app.feed_cache import (
    delete_post_hash,
    fan_out_post,
    publish_post_event,
    read_feed,
    rebuild_feed_for_user,
    remove_post_from_all_feeds,
    store_post_hash,
)
from app.models import Post, PostCreate, PostIdResponse, PostUpdate
from app.redis_client import RedisClient

router = APIRouter()


@router.post("/post/create", response_model=PostIdResponse)
async def create_post(
    payload: PostCreate,
    current_user: UUID = Depends(get_current_user),
) -> PostIdResponse:
    async with Database.master_connection() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO posts (author_id, text)
            VALUES ($1, $2)
            RETURNING id, created_at
            """,
            current_user,
            payload.text,
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create post",
        )

    post_id: UUID = row["id"]
    created_at = row["created_at"]

    redis = RedisClient.get()
    # Тело поста всегда сохраняем сразу (read-after-write согласованность)
    await store_post_hash(redis, post_id, payload.text, current_user, created_at)

    if settings.feed_fanout_async:
        # Публикуем событие — воркер сделает fan-out асинхронно
        await publish_post_event(redis, "created", post_id, current_user, created_at)
    else:
        await fan_out_post(redis, post_id, current_user, created_at)

    return PostIdResponse(id=post_id)


@router.put("/post/update", status_code=status.HTTP_200_OK)
async def update_post(
    payload: PostUpdate,
    current_user: UUID = Depends(get_current_user),
) -> dict[str, str]:
    async with Database.master_connection() as conn:
        row = await conn.fetchrow(
            """
            UPDATE posts
            SET text = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2 AND author_id = $3
            RETURNING id, created_at
            """,
            payload.text,
            payload.id,
            current_user,
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post not found or not owned by user",
        )

    # Достаточно обновить тело поста в Redis Hash — sorted set не трогаем
    redis = RedisClient.get()
    await store_post_hash(
        redis, payload.id, payload.text, current_user, row["created_at"],
    )

    return {"status": "ok"}


@router.put("/post/delete/{id}", status_code=status.HTTP_200_OK)
async def delete_post(
    id: UUID,  # noqa: A002 — имя параметра зафиксировано в openapi.json
    current_user: UUID = Depends(get_current_user),
) -> dict[str, str]:
    async with Database.master_connection() as conn:
        row = await conn.fetchrow(
            """
            DELETE FROM posts
            WHERE id = $1 AND author_id = $2
            RETURNING id
            """,
            id,
            current_user,
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post not found or not owned by user",
        )

    redis = RedisClient.get()
    await delete_post_hash(redis, id)

    if settings.feed_fanout_async:
        # created_at в событии "deleted" не используется потребителем, но поле
        # обязательное в схеме — передаём время удаления как event timestamp
        await publish_post_event(
            redis, "deleted", id, current_user, datetime.now(timezone.utc),
        )
    else:
        await remove_post_from_all_feeds(redis, id, current_user)

    return {"status": "ok"}


@router.get("/post/get/{id}", response_model=Post)
async def get_post(id: UUID) -> Post:  # noqa: A002
    async with Database.slave_connection() as conn:
        row = await conn.fetchrow(
            "SELECT id, text, author_id FROM posts WHERE id = $1",
            id,
        )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Post not found",
        )
    return Post(
        id=row["id"],
        text=row["text"],
        author_user_id=row["author_id"],
    )


@router.get("/post/feed", response_model=list[Post])
async def get_feed(
    offset: int = Query(0, ge=0, description="Оффсет с которого начинать выдачу"),
    limit: int = Query(10, ge=1, le=1000, description="Лимит постов в выдаче"),
    current_user: UUID = Depends(get_current_user),
) -> list[Post]:
    redis = RedisClient.get()
    items = await read_feed(redis, current_user, offset, limit)
    return [
        Post(
            id=UUID(item["id"]),
            text=item["text"],
            author_user_id=UUID(item["author_user_id"]),
        )
        for item in items
    ]


@router.post("/post/feed/rebuild", status_code=status.HTTP_200_OK)
async def rebuild_feed(
    current_user: UUID = Depends(get_current_user),
) -> dict[str, int | str]:
    """Перестраивает ленту текущего пользователя из БД.

    Recovery-эндпоинт homework 4 (не входит в основной функциональный
    контракт, но описан в openapi.json как вспомогательный).
    """
    redis = RedisClient.get()
    added = await rebuild_feed_for_user(redis, current_user)
    return {"status": "ok", "posts_loaded": added}
