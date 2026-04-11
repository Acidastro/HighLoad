from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI

from app.database import Database
from app.redis_client import RedisClient
from app.routes.friends import router as friends_router
from app.routes.posts import router as posts_router
from app.routes.users import router as users_router

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await Database.connect()
    await RedisClient.connect()
    try:
        yield
    finally:
        await RedisClient.disconnect()
        await Database.disconnect()


app = FastAPI(
    title="Social Network API",
    description="Basic social network skeleton",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(users_router, tags=["users"])
app.include_router(friends_router, tags=["friends"])
app.include_router(posts_router, tags=["posts"])


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
