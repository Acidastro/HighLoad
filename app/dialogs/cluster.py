"""Менеджер пулов соединений к шардам диалогов.

Сравни с `app/database.py` (`Database` из ДЗ-3):
- Database работает с master + slaves (репликация = масштабирование READ).
- DialogsCluster — N независимых шардов (шардирование = масштабирование WRITE).

Между шардами нет связи: каждый pool — отдельный PostgreSQL-инстанс с одинаковой
схемой `messages`. Роутинг идёт через `shard_for_chat(chat_id, n_shards)`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import asyncpg

from app.config import settings
from app.dialogs.sharding import shard_for_chat

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _parse_shard_dsns(raw: str) -> list[str]:
    """Парсит строку DSN-ов через запятую в список.

    Сохраняем порядок: индекс в списке = индекс шарда (shard_id).
    Перестановка элементов поломает роутинг для всех уже записанных данных.
    """
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


class DialogsCluster:
    """Кластер шардов подсистемы диалогов.

    Lifecycle:
        await DialogsCluster.connect()    # в lifespan FastAPI
        pool = DialogsCluster.pool_for_chat(chat_id)
        async with DialogsCluster.connection_for_chat(chat_id) as conn:
            await conn.execute(...)
        await DialogsCluster.disconnect() # при остановке
    """

    _pools: list[asyncpg.Pool] = []
    # Текущая схема, по которой роутятся ЧТЕНИЯ (read_n) и ЗАПИСИ (write_n).
    # В нормальном режиме оба числа равны len(_pools), dual_write=False.
    # См. секцию "Resharding feature-flags" в app/config.py.
    _read_n: int = 0
    _write_n: int = 0
    _dual_write: bool = False

    @classmethod
    async def connect(cls) -> None:
        """Открывает пул на каждый шард в порядке из `settings.dialogs_shards`."""
        dsns = _parse_shard_dsns(settings.dialogs_shards)
        if not dsns:
            raise RuntimeError(
                "DIALOGS_SHARDS пуст — невозможно инициализировать кластер диалогов",
            )

        pools: list[asyncpg.Pool] = []
        for idx, dsn in enumerate(dsns):
            try:
                pool = await asyncpg.create_pool(
                    dsn=dsn,
                    min_size=settings.dialogs_pool_min_size,
                    max_size=settings.dialogs_pool_max_size,
                )
                pools.append(pool)
            except Exception as exc:
                # Если хоть один шард недоступен — закрываем уже открытые и падаем.
                # Частичная инициализация недопустима: запросы к недоступному
                # шарду тихо потерялись бы.
                for p in pools:
                    await p.close()
                raise RuntimeError(
                    f"Не удалось подключиться к шарду {idx}: {exc}",
                ) from exc

        cls._pools = pools

        n = len(pools)
        cls._read_n = settings.dialogs_read_n or n
        cls._write_n = settings.dialogs_write_n or n
        cls._dual_write = settings.dialogs_dual_write
        cls._validate_resharding_state()

        print(
            f"[DialogsCluster] Подключено {n} шард(ов); "
            f"read_n={cls._read_n}, write_n={cls._write_n}, "
            f"dual_write={cls._dual_write}",
        )

    @classmethod
    def _validate_resharding_state(cls) -> None:
        n = len(cls._pools)
        for name, value in (("read_n", cls._read_n), ("write_n", cls._write_n)):
            if not 1 <= value <= n:
                raise RuntimeError(
                    f"{name}={value} вне [1, {n}] — проверь настройки кластера",
                )
        # Случай read_n==write_n при dual_write=true допустим: на стадии
        # switch-read мы уже переключили чтения на новую схему, но dual-write
        # ещё не выключили — это safety net для отката.

    @classmethod
    async def disconnect(cls) -> None:
        """Закрывает все пулы."""
        for pool in cls._pools:
            await pool.close()
        cls._pools = []

    @classmethod
    def n_shards(cls) -> int:
        return len(cls._pools)

    @classmethod
    def pool_for_chat(cls, chat_id: bytes) -> asyncpg.Pool:
        """Пул для чтения/единственной записи (нормальный режим, без решардинга).

        Использует read_n как число шардов. Эквивалентно read_pool_for_chat,
        оставлено для обратной совместимости с тестами.
        """
        return cls.read_pool_for_chat(chat_id)

    @classmethod
    def read_pool_for_chat(cls, chat_id: bytes) -> asyncpg.Pool:
        """Шард, с которого читаем переписку. Определяется по read_n."""
        if not cls._pools:
            raise RuntimeError("DialogsCluster не инициализирован")
        idx = shard_for_chat(chat_id, cls._read_n)
        return cls._pools[idx]

    @classmethod
    def write_pools_for_chat(cls, chat_id: bytes) -> list[asyncpg.Pool]:
        """Список шардов, в которые пишем сообщение.

        - Без dual-write: один шард по write_n.
        - С dual-write: первый шард по read_n (старая схема), второй — по
          write_n (новая схема). Если индексы совпали — пишем только в один
          (отсортированный список без дублей сохраняет порядок).
        """
        if not cls._pools:
            raise RuntimeError("DialogsCluster не инициализирован")

        primary_idx = shard_for_chat(chat_id, cls._write_n)
        if not cls._dual_write:
            return [cls._pools[primary_idx]]

        secondary_idx = shard_for_chat(chat_id, cls._read_n)
        # Порядок: сначала read_n (старый), потом write_n (новый). Это нужно
        # для предсказуемой семантики мониторинга и логики отката.
        if primary_idx == secondary_idx:
            return [cls._pools[primary_idx]]
        return [cls._pools[secondary_idx], cls._pools[primary_idx]]

    @classmethod
    def pool_by_index(cls, shard_idx: int) -> asyncpg.Pool:
        """Прямой доступ к пулу по индексу — нужен для backfill/решардинга."""
        if not cls._pools:
            raise RuntimeError("DialogsCluster не инициализирован")
        n = len(cls._pools)
        if not 0 <= shard_idx < n:
            raise ValueError(f"shard_idx {shard_idx} вне [0, {n})")
        return cls._pools[shard_idx]

    @classmethod
    @asynccontextmanager
    async def connection_for_chat(cls, chat_id: bytes) -> AsyncIterator[Any]:
        """Acquire соединение для ЧТЕНИЯ из пула того шарда, где лежит chat_id."""
        pool = cls.read_pool_for_chat(chat_id)
        async with pool.acquire() as conn:
            yield conn

    # ----- Утилиты для скрипта решардинга и тестов -----

    @classmethod
    def is_dual_write(cls) -> bool:
        return cls._dual_write

    @classmethod
    def read_n(cls) -> int:
        return cls._read_n

    @classmethod
    def write_n(cls) -> int:
        return cls._write_n

    @classmethod
    def reconfigure(
        cls,
        *,
        read_n: int | None = None,
        write_n: int | None = None,
        dual_write: bool | None = None,
    ) -> None:
        """Точечная смена конфигурации без переподключения пулов.

        Используется CLI-скриптом решардинга (scripts/resharding.py) и
        интеграционными тестами, чтобы прогонять стадии без рестарта приложения.
        В проде это эквивалент смены ENV + rolling restart, но для учебной
        задачи прямой вызов нагляднее.
        """
        if read_n is not None:
            cls._read_n = read_n
        if write_n is not None:
            cls._write_n = write_n
        if dual_write is not None:
            cls._dual_write = dual_write
        cls._validate_resharding_state()
