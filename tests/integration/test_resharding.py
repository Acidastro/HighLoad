"""Интеграционный тест полного цикла решардинга 2 → 3 шарда (homework 5, итерация 6).

Сценарий:
  1. Поднимаем 3 пула, но работаем как с 2 (read_n=write_n=2).
  2. Пишем M сообщений → они распределяются по shard0/shard1 по `% 2`.
  3. Включаем dual-write (write_n=3, read_n=2).
  4. Параллельно с живыми /send гоняем backfill.
  5. verify → все сообщения на новых местах.
  6. switch-read (read_n=3), disable-dual-write, cleanup.
  7. Проверяем: НИ ОДНО сообщение не потерялось, дублей нет, все строки лежат
     на правильном шарде по `% 3`.

Это самая ценная итерация — здесь видно настоящие race conditions.
Требуется поднятый dialogs-shard2 (профиль resharding):
    docker compose --profile resharding up -d dialogs-shard2
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.auth import create_access_token
from app.config import settings
from app.dialogs.cluster import DialogsCluster
from app.dialogs.sharding import shard_for_chat
from app.main import app
from scripts.resharding import (
    cmd_backfill,
    cmd_cleanup,
    cmd_disable_dual_write,
    cmd_enable_dual_write,
    cmd_switch_read,
    cmd_verify,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION") == "1",
    reason="integration tests skipped",
)

# Конфигурация теста переопределяет дефолтный список шардов (2) на 3.
SHARDS_3 = (
    "postgresql://postgres:postgres@localhost:5440/dialogs,"
    "postgresql://postgres:postgres@localhost:5441/dialogs,"
    "postgresql://postgres:postgres@localhost:5442/dialogs"
)


def _auth_headers(user_id: UUID) -> dict[str, str]:
    return {"Authorization": f"Bearer {create_access_token(str(user_id))}"}


@pytest.fixture
async def cluster_3() -> AsyncIterator[type[DialogsCluster]]:
    """3 пула, старт с read_n=write_n=2 (как до решардинга)."""
    saved = (settings.dialogs_shards, settings.dialogs_read_n, settings.dialogs_write_n)
    settings.dialogs_shards = SHARDS_3
    settings.dialogs_read_n = 2
    settings.dialogs_write_n = 2
    settings.dialogs_dual_write = False

    await DialogsCluster.connect()
    for i in range(DialogsCluster.n_shards()):
        async with DialogsCluster.pool_by_index(i).acquire() as conn:
            await conn.execute("TRUNCATE messages RESTART IDENTITY")
    try:
        yield DialogsCluster
    finally:
        await DialogsCluster.disconnect()
        (
            settings.dialogs_shards,
            settings.dialogs_read_n,
            settings.dialogs_write_n,
        ) = saved
        settings.dialogs_dual_write = False


async def _send(client: AsyncClient, sender: UUID, recipient: UUID, text: str) -> None:
    resp = await client.post(
        f"/dialog/{recipient}/send",
        json={"text": text},
        headers=_auth_headers(sender),
    )
    assert resp.status_code == 200, resp.text


async def _all_messages_set(cluster: type[DialogsCluster]) -> set[str]:
    """Собирает множество message_id со всех шардов."""
    seen: set[str] = set()
    for i in range(cluster.n_shards()):
        async with cluster.pool_by_index(i).acquire() as conn:
            rows = await conn.fetch("SELECT message_id FROM messages")
        for row in rows:
            seen.add(str(row["message_id"]))
    return seen


async def _all_messages_with_shard(
    cluster: type[DialogsCluster],
) -> dict[str, int]:
    """message_id → индекс шарда, на котором он лежит."""
    out: dict[str, int] = {}
    for i in range(cluster.n_shards()):
        async with cluster.pool_by_index(i).acquire() as conn:
            rows = await conn.fetch("SELECT message_id, chat_id FROM messages")
        for row in rows:
            out[str(row["message_id"])] = i
    return out


async def test_full_resharding_2_to_3_no_loss_no_dups(
    cluster_3: type[DialogsCluster],
) -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # ----- Этап 0: пишем M сообщений на N=2 -----
        users = [uuid4() for _ in range(20)]
        pre_count = 200
        sent_ids_pre: set[tuple[UUID, UUID, str]] = set()
        for k in range(pre_count):
            a, b = users[k % len(users)], users[(k + 1) % len(users)]
            text = f"pre-{k}"
            await _send(ac, a, b, text)
            sent_ids_pre.add((a, b, text))

        # На этом этапе все сообщения должны лежать только на shard 0/1.
        for i in range(2, cluster_3.n_shards()):
            async with cluster_3.pool_by_index(i).acquire() as conn:
                cnt = await conn.fetchval("SELECT count(*) FROM messages")
                assert cnt == 0, f"шард {i} не должен содержать данные на N=2"

        before_dual_write = await _all_messages_set(cluster_3)
        assert len(before_dual_write) == pre_count

        # ----- Этап 2: включаем dual-write -----
        await cmd_enable_dual_write(new_n=3)
        assert cluster_3.is_dual_write()

        # ----- Этап 3: backfill параллельно с продолжающимся /send -----
        live_count = 100

        async def live_writer() -> None:
            for k in range(live_count):
                a, b = users[k % len(users)], users[(k + 2) % len(users)]
                await _send(ac, a, b, f"live-{k}")
                await asyncio.sleep(0.001)

        await asyncio.gather(live_writer(), cmd_backfill(new_n=3))

        # ----- Этап 4: verify -----
        await cmd_verify(new_n=3)

        # ----- Этап 5: switch-read -----
        await cmd_switch_read(new_n=3)

        # Проверим, что после переключения /list читает корректную выдачу
        # (читаем диалог, в который точно писали).
        a, b = users[0], users[1]
        resp = await ac.get(f"/dialog/{b}/list", headers=_auth_headers(a))
        assert resp.status_code == 200
        # выдача должна содержать хотя бы одно сообщение — пары (a,b) точно были.
        assert len(resp.json()) > 0

        # ----- Этап 6: disable dual-write + cleanup -----
        await cmd_disable_dual_write()
        await cmd_cleanup(new_n=3)

        # ----- Финальные инварианты -----
        final_messages = await _all_messages_with_shard(cluster_3)

        # 1) ни одно сообщение из этапа 0 не потеряно
        assert before_dual_write.issubset(final_messages.keys()), (
            "потеряны сообщения, написанные ДО dual-write"
        )

        # 2) ровно pre_count + live_count уникальных message_id во всём кластере
        assert len(final_messages) == pre_count + live_count, (
            f"ожидаем {pre_count + live_count}, получили {len(final_messages)}"
        )

        # 3) каждый message_id лежит ровно на том шарде, который соответствует
        #    его chat_id по % 3 — после cleanup никаких «дубликатов на старом
        #    шарде» быть не должно.
        for i in range(cluster_3.n_shards()):
            async with cluster_3.pool_by_index(i).acquire() as conn:
                rows = await conn.fetch("SELECT chat_id FROM messages")
            for row in rows:
                expected = shard_for_chat(row["chat_id"], 3)
                assert expected == i, (
                    f"строка с chat_id на шарде {i}, а должна быть на {expected}"
                )


def test_resharding_validation_rejects_out_of_range(
    cluster_3: type[DialogsCluster],  # noqa: ARG001 — фикстура поднимает кластер
) -> None:
    """read_n/write_n должны быть в диапазоне [1, n_shards]."""
    with pytest.raises(RuntimeError):
        DialogsCluster.reconfigure(read_n=99)
    with pytest.raises(RuntimeError):
        DialogsCluster.reconfigure(write_n=0)
    DialogsCluster.reconfigure(read_n=2, write_n=2, dual_write=False)
