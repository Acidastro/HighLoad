"""Интеграционный тест DialogsCluster.

Требуется запущенные dialogs-shard0 (5440) и dialogs-shard1 (5441) — см. итерацию 1.
Тест: пишем 1000 сообщений в случайные пары пользователей и проверяем,
что распределение по шардам близко к равномерному (перекос < 10%).

Запуск: pytest tests/integration/test_dialogs_cluster.py -v
Скип: SKIP_INTEGRATION=1 pytest ...
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from app.dialogs.cluster import DialogsCluster
from app.dialogs.sharding import compute_chat_id

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION") == "1",
    reason="integration tests skipped",
)


@pytest.fixture
async def cluster() -> AsyncIterator[type[DialogsCluster]]:
    """Поднимает кластер для теста и чистит таблицы перед/после."""
    # Дефолтные настройки указывают на localhost:5440,5441 — см. app/config.py.
    await DialogsCluster.connect()
    # Чистим перед тестом, чтобы прогон был воспроизводим.
    for i in range(DialogsCluster.n_shards()):
        async with DialogsCluster.pool_by_index(i).acquire() as conn:
            await conn.execute("TRUNCATE messages RESTART IDENTITY")
    yield DialogsCluster
    await DialogsCluster.disconnect()


@pytest.mark.asyncio
async def test_writes_distribute_uniformly_across_shards(
    cluster: type[DialogsCluster],
) -> None:
    """1000 сообщений → перекос между двумя шардами < 10%."""
    n_messages = 1000

    for _ in range(n_messages):
        u1, u2 = uuid4(), uuid4()
        chat_id = compute_chat_id(u1, u2)
        async with cluster.connection_for_chat(chat_id) as conn:
            await conn.execute(
                "INSERT INTO messages "
                "(message_id, chat_id, sender_id, recipient_id, body) "
                "VALUES ($1, $2, $3, $4, $5)",
                uuid4(), chat_id, u1, u2, "hello",
            )

    counts: list[int] = []
    for i in range(cluster.n_shards()):
        async with cluster.pool_by_index(i).acquire() as conn:
            row = await conn.fetchrow("SELECT count(*) AS c FROM messages")
            counts.append(int(row["c"]))

    assert sum(counts) == n_messages, f"некоторые сообщения потерялись: {counts}"
    expected = n_messages / cluster.n_shards()
    max_dev = max(abs(c - expected) for c in counts) / expected
    assert max_dev < 0.10, f"перекос между шардами: {counts}"


@pytest.mark.asyncio
async def test_same_chat_always_lands_on_same_shard(
    cluster: type[DialogsCluster],
) -> None:
    """Все сообщения одного диалога должны попасть на один и тот же шард.

    Это инвариант локальности данных: /dialog/list должен идти в один шард.
    """
    a, b = uuid4(), uuid4()
    chat_id = compute_chat_id(a, b)
    expected_shard = next(
        i for i in range(cluster.n_shards())
        if cluster.pool_for_chat(chat_id) is cluster.pool_by_index(i)
    )

    for k in range(50):
        # Меняем направление: половина — от a к b, половина — наоборот.
        sender, recipient = (a, b) if k % 2 == 0 else (b, a)
        async with cluster.connection_for_chat(chat_id) as conn:
            await conn.execute(
                "INSERT INTO messages "
                "(message_id, chat_id, sender_id, recipient_id, body) "
                "VALUES ($1, $2, $3, $4, $5)",
                uuid4(), chat_id, sender, recipient, f"msg-{k}",
            )

    for i in range(cluster.n_shards()):
        async with cluster.pool_by_index(i).acquire() as conn:
            row = await conn.fetchrow(
                "SELECT count(*) AS c FROM messages WHERE chat_id = $1",
                chat_id,
            )
            count = int(row["c"])
            if i == expected_shard:
                assert count == 50
            else:
                assert count == 0, (
                    f"сообщение чата ушло не на свой шард: shard={i}, count={count}"
                )
