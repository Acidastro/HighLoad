"""
Скрипт для загрузки данных из people.v2.csv в таблицу users.

Формат CSV (без заголовка): Фамилия Имя,YYYY-MM-DD,Город
"""
from __future__ import annotations

import asyncio
import csv
from datetime import date
from pathlib import Path

import asyncpg

DB_DSN = "postgresql://postgres:postgres@localhost:5432/social_network"
CSV_PATH = Path(__file__).parent.parent / "people.v2.csv"
BATCH_SIZE = 5000
FAKE_PASSWORD_HASH = "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj2NjlAhBm6"
GENDERS = ["M", "F"]


async def seed() -> None:
    conn = await asyncpg.connect(DB_DSN)

    existing = await conn.fetchval("SELECT COUNT(*) FROM users")
    print(f"Уже в БД: {existing} записей")

    if existing > 10000:
        print("Данные уже загружены, пропускаем.")
        await conn.close()
        return

    print(f"Читаем {CSV_PATH} ...")

    rows: list[tuple[str, str, str, str, date, str, str]] = []
    skipped = 0

    with open(CSV_PATH, encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            if len(line) < 3:
                skipped += 1
                continue

            full_name = line[0].strip()
            birthdate_str = line[1].strip()
            city = line[2].strip()

            parts = full_name.split(" ", 1)
            if len(parts) < 2:
                skipped += 1
                continue

            last_name, first_name = parts[0], parts[1]

            try:
                birthdate = date.fromisoformat(birthdate_str)
            except ValueError:
                skipped += 1
                continue

            rows.append((
                f"user_{i}@example.com",
                FAKE_PASSWORD_HASH,
                first_name,
                last_name,
                birthdate,
                GENDERS[i % 2],
                city,
            ))

    total = len(rows)
    print(f"Прочитано: {total} записей, пропущено: {skipped}")
    print(f"Загружаем батчами по {BATCH_SIZE}...")

    inserted = 0
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        await conn.executemany(
            """
            INSERT INTO users (email, password_hash, first_name, last_name,
                               birthdate, gender, city)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (email) DO NOTHING
            """,
            batch,
        )
        inserted += len(batch)
        print(f"  {inserted}/{total} ({inserted / total * 100:.1f}%)", end="\r", flush=True)

    print(f"\nГотово! Загружено {inserted} записей.")
    final_count = await conn.fetchval("SELECT COUNT(*) FROM users")
    print(f"Итого в таблице: {final_count} записей")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(seed())
