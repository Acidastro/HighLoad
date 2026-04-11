from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.auth import get_current_user
from app.database import Database
from app.feed_cache import (
    add_friend_posts_to_feed,
    remove_friend_posts_from_feed,
)
from app.redis_client import RedisClient

router = APIRouter()


@router.put("/friend/set/{user_id}", status_code=status.HTTP_200_OK)
async def set_friend(
    user_id: UUID,
    current_user: UUID = Depends(get_current_user),
) -> dict[str, str]:
    if user_id == current_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot befriend yourself",
        )

    async with Database.master_connection() as conn:
        # Проверяем, что целевой пользователь существует
        exists = await conn.fetchval("SELECT 1 FROM users WHERE id = $1", user_id)
        if not exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Target user not found",
            )

        await conn.execute(
            """
            INSERT INTO friendships (user_id, friend_id)
            VALUES ($1, $2)
            ON CONFLICT (user_id, friend_id) DO NOTHING
            """,
            current_user,
            user_id,
        )

    # Обновляем ленту текущего пользователя — подмешиваем последние посты друга
    redis = RedisClient.get()
    await add_friend_posts_to_feed(redis, current_user, user_id)

    return {"status": "ok"}


@router.put("/friend/delete/{user_id}", status_code=status.HTTP_200_OK)
async def delete_friend(
    user_id: UUID,
    current_user: UUID = Depends(get_current_user),
) -> dict[str, str]:
    async with Database.master_connection() as conn:
        await conn.execute(
            """
            DELETE FROM friendships
            WHERE user_id = $1 AND friend_id = $2
            """,
            current_user,
            user_id,
        )

    redis = RedisClient.get()
    await remove_friend_posts_from_feed(redis, current_user, user_id)

    return {"status": "ok"}
