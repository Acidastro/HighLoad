"""Seed: одинаковый dataset диалогов в Tarantool и Postgres (homework 7).

Чтобы сравнение "до/после" было честным, оба хранилища стартуют с одинаковой
выборкой сообщений. Скрипт идемпотентен по факту: гонять много раз — данных
становится больше, но если запустить с чистых volumes — получишь ровно
NUM_USERS * MSGS_PER_USER сообщений в каждом бэкенде.

Запуск:
    export DIALOGS_SHARDS="postgresql://postgres:postgres@localhost:5440/dialogs,postgresql://postgres:postgres@localhost:5441/dialogs"
    .venv/bin/python scripts/seed_dialogs_tarantool.py \\
        --users 200 --msgs-per-user 50 --backend both
"""

from __future__ import annotations

import argparse
import asyncio
import random
from uuid import UUID, uuid4

import asyncpg
import asynctnt

from app.config import settings
from app.dialogs.sharding import compute_chat_id, shard_for_chat


async def seed_tarantool(messages: list[tuple[UUID, UUID, str]]) -> None:
    conn = asynctnt.Connection(host=settings.tarantool_host, port=settings.tarantool_port)
    await conn.connect()
    try:
        for from_id, to_id, text in messages:
            await conn.call("dialog_send", [str(from_id), str(to_id), text])
    finally:
        await conn.disconnect()


async def seed_postgres(messages: list[tuple[UUID, UUID, str]]) -> None:
    dsns = [d.strip() for d in settings.dialogs_shards.split(",") if d.strip()]
    pools = [await asyncpg.create_pool(dsn=d, min_size=1, max_size=4) for d in dsns]
    try:
        n = len(pools)
        for from_id, to_id, text in messages:
            chat_id = compute_chat_id(from_id, to_id)
            shard = shard_for_chat(chat_id, n)
            async with pools[shard].acquire() as c:
                await c.execute(
                    """
                    INSERT INTO messages
                        (message_id, chat_id, sender_id, recipient_id, body)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    uuid4(),
                    chat_id,
                    from_id,
                    to_id,
                    text,
                )
    finally:
        for p in pools:
            await p.close()


def build_messages(num_users: int, msgs_per_user: int) -> list[tuple[UUID, UUID, str]]:
    users = [uuid4() for _ in range(num_users)]
    rng = random.Random(42)
    out: list[tuple[UUID, UUID, str]] = []
    for sender in users:
        for i in range(msgs_per_user):
            recipient = rng.choice(users)
            if recipient == sender:
                continue
            out.append((sender, recipient, f"seed msg #{i} from {sender}"))
    return out


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--users", type=int, default=200)
    p.add_argument("--msgs-per-user", type=int, default=50)
    p.add_argument("--backend", choices=["both", "postgres", "tarantool"], default="both")
    args = p.parse_args()

    messages = build_messages(args.users, args.msgs_per_user)
    print(f"[seed] prepared {len(messages)} messages, backend={args.backend}")

    if args.backend in ("both", "tarantool"):
        await seed_tarantool(messages)
        print("[seed] tarantool: done")
    if args.backend in ("both", "postgres"):
        await seed_postgres(messages)
        print("[seed] postgres: done")


if __name__ == "__main__":
    asyncio.run(main())
