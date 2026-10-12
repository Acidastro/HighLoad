"""
Скрипт для загрузки тестовых постов в таблицу posts.

Источник данных: https://github.com/OtusTeam/highload/blob/master/homework/posts.txt
Каждая строка файла — это отдельный пост. Посты распределяются между
уже существующими пользователями равномерно (round-robin).

Запуск:
    python scripts/load_posts.py
    python scripts/load_posts.py --posts-file /path/to/posts.txt
"""
from __future__ import annotations

import argparse
import asyncio
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import asyncpg

DB_DSN = "postgresql://postgres:postgres@localhost:5432/social_network"
DEFAULT_POSTS_PATH = Path(__file__).parent.parent / "posts.txt"
BATCH_SIZE = 5000


async def load(posts_file: Path, dsn: str) -> None:
    if not posts_file.exists():
        msg = (
            f"Файл с постами не найден: {posts_file}\n"
            "Скачайте posts.txt из "
            "https://github.com/OtusTeam/highload/blob/master/homework/posts.txt "
            "в корень проекта или передайте путь через --posts-file."
        )
        raise FileNotFoundError(msg)

    conn = await asyncpg.connect(dsn)
    try:
        existing = await conn.fetchval("SELECT COUNT(*) FROM posts")
        print(f"Уже в БД: {existing} постов")
        if existing > 10000:
            print("Посты уже загружены, пропускаем.")
            return

        user_rows = await conn.fetch("SELECT id FROM users ORDER BY id")
        user_ids: list[UUID] = [row["id"] for row in user_rows]
        if not user_ids:
            print("Нет пользователей в таблице users — сначала запустите seed_users.py")
            return

        print(f"Пользователей: {len(user_ids)}")
        print(f"Читаем {posts_file} ...")

        # Читаем построчно, пропускаем пустые строки
        with open(posts_file, encoding="utf-8", errors="ignore") as f:
            lines = [line.strip() for line in f if line.strip()]

        total = len(lines)
        print(f"Прочитано постов: {total}")

        # Каждому посту назначаем случайного автора и случайное время в пределах
        # последнего месяца (чтобы в фидах были разные метки времени).
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        month_ago = now - timedelta(days=30)
        span_seconds = int((now - month_ago).total_seconds())
        rng = random.Random(42)  # детерминированное распределение для повторяемости

        rows: list[tuple[UUID, str, datetime, datetime]] = []
        for i, text in enumerate(lines):
            author_id = user_ids[i % len(user_ids)]
            created_at = month_ago + timedelta(seconds=rng.randint(0, span_seconds))
            rows.append((author_id, text, created_at, created_at))

        print(f"Загружаем батчами по {BATCH_SIZE}...")
        inserted = 0
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            await conn.executemany(
                """
                INSERT INTO posts (author_id, text, created_at, updated_at)
                VALUES ($1, $2, $3, $4)
                """,
                batch,
            )
            inserted += len(batch)
            pct = inserted / total * 100
            print(f"  {inserted}/{total} ({pct:.1f}%)", end="\r", flush=True)

        print(f"\nГотово! Загружено {inserted} постов.")
        final_count = await conn.fetchval("SELECT COUNT(*) FROM posts")
        print(f"Итого в таблице posts: {final_count}")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Загрузка тестовых постов")
    parser.add_argument(
        "--posts-file",
        type=Path,
        default=DEFAULT_POSTS_PATH,
        help="Путь к файлу с постами (по умолчанию ./posts.txt)",
    )
    parser.add_argument("--dsn", default=DB_DSN, help="DSN подключения к PostgreSQL")
    args = parser.parse_args()

    asyncio.run(load(args.posts_file, args.dsn))


if __name__ == "__main__":
    main()
