"""Unit-тесты для ConnectionManager (homework 6, урок 3)."""
from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.ws.connection_manager import ConnectionManager


class FakeWS:
    """Минимальный stub для WebSocket — нужен только send_json."""

    def __init__(self, fail: bool = False) -> None:
        self.sent: list[dict] = []
        self.fail = fail

    async def send_json(self, payload: dict) -> None:
        if self.fail:
            raise RuntimeError("socket broken")
        self.sent.append(payload)


@pytest.mark.asyncio
async def test_connect_triggers_first_connect_callback_once() -> None:
    on_first = AsyncMock()
    on_last = AsyncMock()
    mgr = ConnectionManager(on_first_connect=on_first, on_last_disconnect=on_last)
    uid = uuid4()
    ws1, ws2 = FakeWS(), FakeWS()

    await mgr.connect(uid, ws1)  # type: ignore[arg-type]
    await mgr.connect(uid, ws2)  # type: ignore[arg-type]

    # Первый коннект — один вызов; второй коннект — НЕ должен повторно зватьcb
    on_first.assert_awaited_once_with(uid)
    on_last.assert_not_awaited()


@pytest.mark.asyncio
async def test_disconnect_last_triggers_callback() -> None:
    on_first = AsyncMock()
    on_last = AsyncMock()
    mgr = ConnectionManager(on_first_connect=on_first, on_last_disconnect=on_last)
    uid = uuid4()
    ws1, ws2 = FakeWS(), FakeWS()

    await mgr.connect(uid, ws1)  # type: ignore[arg-type]
    await mgr.connect(uid, ws2)  # type: ignore[arg-type]
    await mgr.disconnect(uid, ws1)  # type: ignore[arg-type]
    on_last.assert_not_awaited()  # ещё ws2 живой
    await mgr.disconnect(uid, ws2)  # type: ignore[arg-type]
    on_last.assert_awaited_once_with(uid)


@pytest.mark.asyncio
async def test_push_delivers_to_all_sockets() -> None:
    mgr = ConnectionManager()
    uid = uuid4()
    ws1, ws2 = FakeWS(), FakeWS()
    await mgr.connect(uid, ws1)  # type: ignore[arg-type]
    await mgr.connect(uid, ws2)  # type: ignore[arg-type]

    sent = await mgr.push(uid, {"type": "post.new", "id": 1})
    assert sent == 2
    assert ws1.sent == [{"type": "post.new", "id": 1}]
    assert ws2.sent == [{"type": "post.new", "id": 1}]


@pytest.mark.asyncio
async def test_push_drops_broken_socket() -> None:
    mgr = ConnectionManager()
    uid = uuid4()
    bad = FakeWS(fail=True)
    good = FakeWS()
    await mgr.connect(uid, bad)  # type: ignore[arg-type]
    await mgr.connect(uid, good)  # type: ignore[arg-type]

    sent = await mgr.push(uid, {"x": 1})
    assert sent == 1
    assert good.sent == [{"x": 1}]
    # битый сокет должен быть удалён из реестра
    assert mgr.total_connections() == 1


@pytest.mark.asyncio
async def test_push_to_unknown_user_returns_zero() -> None:
    mgr = ConnectionManager()
    sent = await mgr.push(uuid4(), {"x": 1})
    assert sent == 0
