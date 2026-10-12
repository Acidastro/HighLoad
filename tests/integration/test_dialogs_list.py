"""Интеграционный тест эндпоинта GET /dialog/{user_id}/list.

Тесты пишутся ДО реализации (TDD, итерация 5).
Проверяем:
  - 401 без токена,
  - сообщения возвращаются в обратном хронологическом порядке (свежие сверху),
  - выдача включает сообщения обоих направлений (A→B и B→A) — это и есть
    смысл симметричного chat_id,
  - изоляция диалогов: чужая переписка в выдаче не появляется,
  - пагинация limit/offset.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.auth import create_access_token
from app.dialogs.cluster import DialogsCluster
from app.main import app

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION") == "1",
    reason="integration tests skipped",
)


def _auth_headers(user_id: UUID) -> dict[str, str]:
    return {"Authorization": f"Bearer {create_access_token(str(user_id))}"}


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    await DialogsCluster.connect()
    for i in range(DialogsCluster.n_shards()):
        async with DialogsCluster.pool_by_index(i).acquire() as conn:
            await conn.execute("TRUNCATE messages RESTART IDENTITY")

    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
    finally:
        await DialogsCluster.disconnect()


async def test_list_requires_auth(client: AsyncClient) -> None:
    resp = await client.get(f"/dialog/{uuid4()}/list")
    assert resp.status_code in (401, 403)


async def test_list_empty_dialog(client: AsyncClient) -> None:
    me, other = uuid4(), uuid4()
    resp = await client.get(f"/dialog/{other}/list", headers=_auth_headers(me))
    assert resp.status_code == 200
    assert resp.json() == []


async def test_list_returns_both_directions_newest_first(
    client: AsyncClient,
) -> None:
    alice, bob = uuid4(), uuid4()
    # Чередуем направления — оба должны попасть в выдачу любого участника.
    bodies = ["a1", "b1", "a2", "b2", "a3"]
    for k, text in enumerate(bodies):
        sender, recipient = (alice, bob) if k % 2 == 0 else (bob, alice)
        resp = await client.post(
            f"/dialog/{recipient}/send",
            json={"text": text},
            headers=_auth_headers(sender),
        )
        assert resp.status_code == 200
        # Гарантируем разный created_at, чтобы порядок был строгий.
        await asyncio.sleep(0.005)

    # Алиса видит всю переписку, свежие — сверху.
    resp = await client.get(f"/dialog/{bob}/list", headers=_auth_headers(alice))
    assert resp.status_code == 200
    items = resp.json()
    assert [i["text"] for i in items] == list(reversed(bodies))

    # Боб видит ту же выдачу — это и есть симметрия chat_id.
    resp = await client.get(f"/dialog/{alice}/list", headers=_auth_headers(bob))
    assert [i["text"] for i in resp.json()] == list(reversed(bodies))


async def test_list_isolates_other_dialogs(client: AsyncClient) -> None:
    alice, bob, carol = uuid4(), uuid4(), uuid4()
    await client.post(
        f"/dialog/{bob}/send", json={"text": "to-bob"},
        headers=_auth_headers(alice),
    )
    await client.post(
        f"/dialog/{carol}/send", json={"text": "to-carol"},
        headers=_auth_headers(alice),
    )
    resp = await client.get(f"/dialog/{bob}/list", headers=_auth_headers(alice))
    texts = [i["text"] for i in resp.json()]
    assert texts == ["to-bob"], f"чужая переписка попала в выдачу: {texts}"


async def test_list_pagination(client: AsyncClient) -> None:
    alice, bob = uuid4(), uuid4()
    for k in range(7):
        await client.post(
            f"/dialog/{bob}/send", json={"text": f"m{k}"},
            headers=_auth_headers(alice),
        )
        await asyncio.sleep(0.002)

    resp = await client.get(
        f"/dialog/{bob}/list?limit=3&offset=0",
        headers=_auth_headers(alice),
    )
    page1 = [i["text"] for i in resp.json()]
    assert page1 == ["m6", "m5", "m4"]

    resp = await client.get(
        f"/dialog/{bob}/list?limit=3&offset=3",
        headers=_auth_headers(alice),
    )
    page2 = [i["text"] for i in resp.json()]
    assert page2 == ["m3", "m2", "m1"]


async def test_list_validates_limit(client: AsyncClient) -> None:
    me, other = uuid4(), uuid4()
    # limit > max разрешённого → 422
    resp = await client.get(
        f"/dialog/{other}/list?limit=10000",
        headers=_auth_headers(me),
    )
    assert resp.status_code == 422
