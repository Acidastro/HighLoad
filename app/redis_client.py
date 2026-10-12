from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis

from app.config import settings


class RedisClient:
    """Async Redis client singleton для кэша ленты постов.

    Хранит:
    - feed:{user_id}  — Sorted Set (score=timestamp, member=post_id)
    - post:{post_id}  — Hash (text, author_id, created_at)
    """

    # Типизируем как Any: стабы redis-py типизируют async-клиент как sync, ломая pyright
    _client: Any = None

    @classmethod
    async def connect(cls) -> None:
        client: Any = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        # Простая проверка, что Redis отвечает
        await client.ping()
        cls._client = client
        print(f"[Redis] Подключено к {settings.redis_url}")

    @classmethod
    async def disconnect(cls) -> None:
        if cls._client is not None:
            await cls._client.aclose()
            cls._client = None

    @classmethod
    def get(cls) -> Any:
        if cls._client is None:
            raise RuntimeError("Redis client не инициализирован")
        return cls._client
