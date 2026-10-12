from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI

from app.config import settings
from app.database import Database
from app.dialogs.cluster import DialogsCluster
from app.dialogs.factory import DialogRepositoryHolder
from app.rabbit_client import RabbitClient
from app.redis_client import RedisClient
from app.routes.dialogs import router as dialogs_router
from app.routes.friends import router as friends_router
from app.routes.posts import router as posts_router
from app.routes.users import router as users_router
from app.routes.ws_feed import router as ws_feed_router
from app.ws.connection_manager import ConnectionManager
from app.ws.rabbit_bridge import RabbitWsBridge

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await Database.connect()
    await RedisClient.connect()
    # DialogsCluster нужен только для backend=postgres. Поднимаем безусловно —
    # инфраструктура из docker-compose та же, и так короче и однообразнее.
    await DialogsCluster.connect()
    await DialogRepositoryHolder.setup()

    # HW6: WebSocket realtime feed via RabbitMQ
    bridge: RabbitWsBridge | None = None
    if settings.feed_transport == "rabbitmq":
        await RabbitClient.connect()
        # forward declaration: bridge.bind_user/unbind_user будут заданы после
        # создания bridge — используем замыкание через локальные ссылки.
        manager_holder: dict[str, ConnectionManager] = {}

        async def _on_connect(uid):  # type: ignore[no-untyped-def]
            if bridge is not None:
                await bridge.bind_user(uid)

        async def _on_disconnect(uid):  # type: ignore[no-untyped-def]
            if bridge is not None:
                await bridge.unbind_user(uid)

        manager = ConnectionManager(
            on_first_connect=_on_connect,
            on_last_disconnect=_on_disconnect,
        )
        manager_holder["m"] = manager
        bridge = RabbitWsBridge(manager)
        await bridge.start(RabbitClient.connection())
        app.state.connection_manager = manager
        app.state.rabbit_bridge = bridge

    try:
        yield
    finally:
        if bridge is not None:
            await bridge.stop()
            await RabbitClient.disconnect()
        await DialogRepositoryHolder.teardown()
        await DialogsCluster.disconnect()
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
app.include_router(dialogs_router, tags=["dialogs"])
app.include_router(ws_feed_router, tags=["ws"])


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
