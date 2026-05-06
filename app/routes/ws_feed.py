"""WebSocket endpoint /post/feed/posted (homework 6, уроки 1, 2, 10).

Lifecycle:
1. Клиент открывает wss://.../post/feed/posted?token=<JWT>
2. authenticate_ws() — до accept(); если токен битый → close(4401).
3. accept(); ConnectionManager.connect() → bind в RabbitMQ (если первый коннект).
4. Серверный heartbeat-таск каждые 25s шлёт {"type":"ping"}.
5. Параллельный receive-loop: pong / любые сообщения; disconnect — exit.
6. finally: ConnectionManager.disconnect() → unbind (если последний коннект).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.auth import authenticate_ws
from app.config import settings

if TYPE_CHECKING:
    from app.ws.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)

router = APIRouter()

# WS close codes (4xxx — application-level, см. RFC 6455 §7.4.2)
WS_CLOSE_UNAUTHORIZED = 4401
WS_CLOSE_IDLE = 4408
WS_CLOSE_INTERNAL = 1011


def _get_manager(ws: WebSocket) -> ConnectionManager:
    return ws.app.state.connection_manager  # type: ignore[no-any-return]


async def _heartbeat(ws: WebSocket) -> None:
    """Шлём {"type":"ping"} каждые ws_heartbeat_interval_s секунд."""
    interval = settings.ws_heartbeat_interval_s
    while True:
        await asyncio.sleep(interval)
        await ws.send_json({
            "type": "ping",
            "ts": datetime.now(timezone.utc).isoformat(),
        })


@router.websocket("/post/feed/posted")
async def feed_ws(
    ws: WebSocket,
    token: str = Query(..., description="JWT access token"),
) -> None:
    user_id = authenticate_ws(token)
    if user_id is None:
        # Принимаем чтобы корректно отправить close code в браузер
        await ws.accept()
        await ws.send_json({"type": "error", "code": "AUTH_FAILED", "message": "Invalid token"})
        await ws.close(code=WS_CLOSE_UNAUTHORIZED)
        return

    await ws.accept()
    manager = _get_manager(ws)
    await manager.connect(user_id, ws)
    logger.info("WS connected: user_id=%s", user_id)

    heartbeat_task = asyncio.create_task(_heartbeat(ws), name=f"ws-hb-{user_id}")
    try:
        while True:
            # Ждём pong/любое сообщение от клиента; idle_timeout закрывает соединение
            try:
                msg = await asyncio.wait_for(
                    ws.receive_text(),
                    timeout=settings.ws_idle_timeout_s,
                )
            except asyncio.TimeoutError:
                await ws.close(code=WS_CLOSE_IDLE)
                return
            # Любое входящее сообщение игнорируем (feed однонаправленный),
            # но лог наличия — для отладки и проверки pong
            logger.debug("WS recv from %s: %s", user_id, msg)
    except WebSocketDisconnect:
        logger.info("WS disconnected: user_id=%s", user_id)
    except Exception:  # noqa: BLE001
        logger.exception("WS error: user_id=%s", user_id)
        with contextlib.suppress(Exception):
            await ws.close(code=WS_CLOSE_INTERNAL)
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await heartbeat_task
        await manager.disconnect(user_id, ws)
