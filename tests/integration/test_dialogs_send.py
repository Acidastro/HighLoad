"""Интеграционный тест эндпоинта POST /dialog/{user_id}/send.

Тесты пишутся ДО реализации (TDD, итерация 4 учебного плана).
Проверяем:
  - 401 без токена,
  - 200 + сообщение появилось ровно в ОДНОМ шарде,
  - сообщение лежит на том шарде, который посчитал shard_for_chat,
  - запрос самому себе → 400 (не имеет смысла для текущего MVP).

Требуются поднятые dialogs-shard0/1 и валидный JWT.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.auth import create_access_token
from app.dialogs.cluster import DialogsCluster
from app.dialogs.sharding import compute_chat_id, shard_for_chat
from app.main import app

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION") == "1",
    reason="integration tests skipped",
)


def _auth_headers(user_id: UUID) -> dict[str, str]:
    token = create_access_token(str(user_id))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """ASGI-клиент с локально поднятым DialogsCluster.

    Полный lifespan не запускаем (Database/Redis не нужны для этих тестов) —
    инициализируем только то, что использует /dialog/send.
    """
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


async def test_send_requires_auth(client: AsyncClient) -> None:
    resp = await client.post(
        f"/dialog/{uuid4()}/send",
        json={"text": "hello"},
    )
    assert resp.status_code in (401, 403)


async def test_send_writes_to_exactly_one_shard(client: AsyncClient) -> None:
    sender, recipient = uuid4(), uuid4()
    resp = await client.post(
        f"/dialog/{recipient}/send",
        json={"text": "hi there"},
        headers=_auth_headers(sender),
    )
    assert resp.status_code == 200

    # Считаем сколько строк в каждом шарде.
    counts: list[int] = []
    for i in range(DialogsCluster.n_shards()):
        async with DialogsCluster.pool_by_index(i).acquire() as conn:
            row = await conn.fetchrow("SELECT count(*) AS c FROM messages")
            counts.append(int(row["c"]))

    assert sum(counts) == 1, f"ожидаем ровно одно сообщение во всём кластере: {counts}"
    # И именно на том шарде, который рассчитан хеш-функцией.
    n = DialogsCluster.n_shards()
    expected = shard_for_chat(compute_chat_id(sender, recipient), n)
    assert counts[expected] == 1


async def test_send_to_self_rejected(client: AsyncClient) -> None:
    me = uuid4()
    resp = await client.post(
        f"/dialog/{me}/send",
        json={"text": "note"},
        headers=_auth_headers(me),
    )
    assert resp.status_code == 400


async def test_send_validates_empty_body(client: AsyncClient) -> None:
    sender, recipient = uuid4(), uuid4()
    resp = await client.post(
        f"/dialog/{recipient}/send",
        json={"text": ""},
        headers=_auth_headers(sender),
    )
    assert resp.status_code == 422
