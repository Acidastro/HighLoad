from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import asyncpg

from app.config import settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class Database:
    pool: asyncpg.Pool | None = None

    @classmethod
    async def connect(cls) -> None:
        cls.pool = await asyncpg.create_pool(
            host=settings.database_host,
            port=settings.database_port,
            user=settings.database_user,
            password=settings.database_password,
            database=settings.database_name,
            min_size=5,
            max_size=20,
        )

    @classmethod
    async def disconnect(cls) -> None:
        if cls.pool:
            await cls.pool.close()

    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncIterator[Any]:
        if cls.pool is None:
            raise RuntimeError("Database pool is not initialized")
        async with cls.pool.acquire() as conn:
            yield conn
