"""CLI решардинга диалогов (homework 5, итерация 6).

Прогоняем 6 этапов из docs/tdd/homework_5_plan.md, каждая стадия — отдельная
команда, чтобы было видно, как меняется состояние:

    python -m scripts.resharding enable-dual-write --new-n 3
    python -m scripts.resharding backfill         --new-n 3
    python -m scripts.resharding verify           --new-n 3
    python -m scripts.resharding switch-read      --new-n 3
    python -m scripts.resharding disable-dual-write
    python -m scripts.resharding cleanup          --new-n 3

В проде команды соответствуют изменениям ENV + rolling restart. Здесь — прямой
вызов на DialogsCluster для наглядности.

Идемпотентность держится на UNIQUE(message_id) + ON CONFLICT DO NOTHING.
"""

from __future__ import annotations

import argparse
import asyncio

from app.dialogs.cluster import DialogsCluster
from app.dialogs.sharding import shard_for_chat

BATCH = 500


async def cmd_enable_dual_write(new_n: int) -> None:
    """Этап 2: включить запись в два шарда (по старой и новой схеме).

    Чтения остаются на старой схеме (read_n не трогаем). Это окно, в которое
    мы можем безопасно делать backfill — все НОВЫЕ сообщения уже идут и в
    старый, и в новый шард, поэтому backfill закроет только исторический хвост.
    """
    DialogsCluster.reconfigure(write_n=new_n, dual_write=True)
    print(
        f"[OK] dual-write включён: read_n={DialogsCluster.read_n()}, "
        f"write_n={DialogsCluster.write_n()}",
    )


async def cmd_backfill(new_n: int) -> None:
    """Этап 3: перенести строки, у которых старый и новый шард различаются.

    Для каждого старого шарда читаем все строки, пересчитываем shard_for_chat
    по new_n, и если индекс новый != текущий — копируем на новый шард с тем же
    message_id. ON CONFLICT DO NOTHING защищает от дублей при параллельной
    работе с живым /send.
    """
    if not DialogsCluster.is_dual_write():
        raise RuntimeError(
            "backfill можно запускать только при включённом dual-write — "
            "иначе живые INSERT'ы в окне будут потеряны",
        )

    old_n = DialogsCluster.read_n()
    moved = 0

    for src_idx in range(old_n):
        src_pool = DialogsCluster.pool_by_index(src_idx)
        # Стримим по pk-id страницами, чтобы не держать всю таблицу в памяти
        # и при этом не зависеть от cursor API внутри транзакции.
        last_id = 0
        while True:
            async with src_pool.acquire() as src_conn:
                rows = await src_conn.fetch(
                    """
                    SELECT id, message_id, chat_id, sender_id, recipient_id,
                           body, created_at
                    FROM messages
                    WHERE id > $1
                    ORDER BY id
                    LIMIT $2
                    """,
                    last_id, BATCH,
                )
            if not rows:
                break
            last_id = int(rows[-1]["id"])
            for row in rows:
                new_idx = shard_for_chat(row["chat_id"], new_n)
                if new_idx == src_idx:
                    continue
                dst_pool = DialogsCluster.pool_by_index(new_idx)
                async with dst_pool.acquire() as dst_conn:
                    await dst_conn.execute(
                        """
                        INSERT INTO messages
                            (message_id, chat_id, sender_id,
                             recipient_id, body, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (message_id) DO NOTHING
                        """,
                        row["message_id"], row["chat_id"],
                        row["sender_id"], row["recipient_id"],
                        row["body"], row["created_at"],
                    )
                    moved += 1
    print(f"[OK] backfill: скопировано {moved} сообщений")


async def cmd_verify(new_n: int) -> None:
    """Этап 4: убедиться, что для каждого сообщения существует копия на ЦЕЛЕВОМ
    шарде по new_n. Если нет — дельта-докатка ещё нужна.
    """
    missing = 0
    for src_idx in range(DialogsCluster.read_n()):
        src_pool = DialogsCluster.pool_by_index(src_idx)
        async with src_pool.acquire() as src_conn:
            rows = await src_conn.fetch(
                "SELECT message_id, chat_id FROM messages",
            )
        for row in rows:
            new_idx = shard_for_chat(row["chat_id"], new_n)
            if new_idx == src_idx:
                continue
            dst_pool = DialogsCluster.pool_by_index(new_idx)
            async with dst_pool.acquire() as dst_conn:
                exists = await dst_conn.fetchval(
                    "SELECT 1 FROM messages WHERE message_id = $1",
                    row["message_id"],
                )
            if not exists:
                missing += 1
    if missing:
        raise RuntimeError(f"verify: {missing} сообщений отсутствуют на новых шардах")
    print("[OK] verify: все сообщения на месте")


async def cmd_switch_read(new_n: int) -> None:
    """Этап 5: переключить ЧТЕНИЯ на новую схему. Записи всё ещё dual-write —
    откат всё ещё дёшев (просто вернуть read_n=old_n).
    """
    DialogsCluster.reconfigure(read_n=new_n)
    print(f"[OK] switch-read: read_n={DialogsCluster.read_n()}")


async def cmd_disable_dual_write() -> None:
    """Этап 6.1: после полного switch read — выключаем dual-write. Точка
    невозврата: дальше cleanup, и старая схема перестаёт быть консистентной.
    """
    DialogsCluster.reconfigure(dual_write=False)
    print("[OK] dual-write выключен")


async def cmd_cleanup(new_n: int) -> None:
    """Этап 6.2: на каждом шарде удаляем строки, чей хеш по new_n указывает
    на ДРУГОЙ шард (т.е. они уже скопированы и на этом шарде лишние).
    """
    if DialogsCluster.is_dual_write():
        raise RuntimeError(
            "cleanup нельзя запускать при включённом dual-write — иначе мы "
            "удалим только что записанные сообщения",
        )
    deleted = 0
    for shard_idx in range(new_n):
        pool = DialogsCluster.pool_by_index(shard_idx)
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, chat_id FROM messages")
            ids_to_delete = [
                int(r["id"])
                for r in rows
                if shard_for_chat(r["chat_id"], new_n) != shard_idx
            ]
            if ids_to_delete:
                await conn.execute(
                    "DELETE FROM messages WHERE id = ANY($1::bigint[])",
                    ids_to_delete,
                )
                deleted += len(ids_to_delete)
    print(f"[OK] cleanup: удалено {deleted} «уехавших» строк")


async def _dispatch(command: str, new_n: int) -> None:
    if command == "disable-dual-write":
        await cmd_disable_dual_write()
        return
    if new_n <= 0:
        raise SystemExit(f"--new-n обязателен для {command}")
    if command == "enable-dual-write":
        await cmd_enable_dual_write(new_n)
    elif command == "backfill":
        await cmd_backfill(new_n)
    elif command == "verify":
        await cmd_verify(new_n)
    elif command == "switch-read":
        await cmd_switch_read(new_n)
    elif command == "cleanup":
        await cmd_cleanup(new_n)
    else:
        raise SystemExit(f"unknown command: {command}")


_ALL_COMMANDS = [
    "enable-dual-write", "backfill", "verify",
    "switch-read", "disable-dual-write", "cleanup",
]


async def _main() -> None:
    parser = argparse.ArgumentParser(prog="resharding")
    parser.add_argument("command", choices=_ALL_COMMANDS)
    parser.add_argument("--new-n", type=int, default=0)
    args = parser.parse_args()

    await DialogsCluster.connect()
    try:
        await _dispatch(args.command, args.new_n)
    finally:
        await DialogsCluster.disconnect()


if __name__ == "__main__":
    asyncio.run(_main())
