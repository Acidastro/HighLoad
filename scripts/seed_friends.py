"""
Скрипт для создания случайных дружеских связей между пользователями.

Для каждого из первых --users пользователей создаёт --friends-per-user
случайных друзей (выбираются из того же подмножества). Это нужно, чтобы
лента постов не была пустой на тестах homework 4.

Семантика записи friendships(user_id, friend_id):
  "user_id добавил friend_id в друзья".

Запуск:
    python scripts/seed_friends.py
    python scripts/seed_friends.py --users 1000 --friends-per-user 30
"""
from __future__ import annotations

import argparse
import asyncio
import random
from uuid import UUID

import asyncpg

DB_DSN = "postgresql://postgres:postgres@localhost:5432/social_network"
DEFAULT_USERS = 1000
DEFAULT_FRIENDS_PER_USER = 30
BATCH_SIZE = 5000


async def seed(users_limit: int, friends_per_user: int, dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        existing = await conn.fetchval("SELECT COUNT(*) FROM friendships")
        print(f"Уже в БД: {existing} связей friendships")
        if existing > users_limit * friends_per_user // 2:
            print("Связи уже есть, пропускаем.")
            return

        user_rows = await conn.fetch(
            "SELECT id FROM users ORDER BY id LIMIT $1",
            users_limit,
        )
        user_ids: list[UUID] = [row["id"] for row in user_rows]
        if len(user_ids) < 2:
            print("Недостаточно пользователей для создания связей.")
            return

        print(
            f"Генерируем связи для {len(user_ids)} пользователей, "
            f"по ~{friends_per_user} друзей на каждого...",
        )

        rng = random.Random(42)
        pairs: set[tuple[UUID, UUID]] = set()
        target_per_user = min(friends_per_user, len(user_ids) - 1)
        # Берём с запасом, чтобы после отсева self-id набрать target_per_user штук
        sample_size = target_per_user + 1
        for uid in user_ids:
            added = 0
            candidates = rng.sample(user_ids, sample_size)
            for fid in candidates:
                if fid == uid:
                    continue
                if (uid, fid) not in pairs:
                    pairs.add((uid, fid))
                    added += 1
                if added >= target_per_user:
                    break

        rows = list(pairs)
        total = len(rows)
        print(f"Подготовлено {total} связей, загружаем батчами по {BATCH_SIZE}...")

        inserted = 0
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            await conn.executemany(
                """
                INSERT INTO friendships (user_id, friend_id)
                VALUES ($1, $2)
                ON CONFLICT (user_id, friend_id) DO NOTHING
                """,
                batch,
            )
            inserted += len(batch)
            pct = inserted / total * 100
            print(f"  {inserted}/{total} ({pct:.1f}%)", end="\r", flush=True)

        final_count = await conn.fetchval("SELECT COUNT(*) FROM friendships")
        print(f"\nГотово! Всего связей в БД: {final_count}")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Генерация тестовых дружеских связей")
    parser.add_argument(
        "--users",
        type=int,
        default=DEFAULT_USERS,
        help="Сколько первых пользователей включить",
    )
    parser.add_argument(
        "--friends-per-user",
        type=int,
        default=DEFAULT_FRIENDS_PER_USER,
        help="Сколько друзей на пользователя",
    )
    parser.add_argument("--dsn", default=DB_DSN, help="DSN подключения к PostgreSQL")
    args = parser.parse_args()

    asyncio.run(seed(args.users, args.friends_per_user, args.dsn))


if __name__ == "__main__":
    main()
