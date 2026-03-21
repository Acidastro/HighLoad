from __future__ import annotations

import itertools
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import asyncpg

from app.config import settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _parse_slave_hosts(raw: str) -> list[tuple[str, int]]:
    """Парсит строку вида 'host1:port1,host2:port2' в список кортежей (host, port)."""
    result: list[tuple[str, int]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            host, port_str = entry.rsplit(":", 1)
            result.append((host.strip(), int(port_str.strip())))
        else:
            # Порт по умолчанию если не указан
            result.append((entry, 5432))
    return result


class Database:
    """Replication-aware database manager.

    Поддерживает два типа соединений:
    - master_connection() — для WRITE операций (INSERT/UPDATE/DELETE)
    - slave_connection()  — для READ операций (SELECT), round-robin по слейвам
    - connection()        — алиас для master_connection() (обратная совместимость)
    """

    # Пул мастера (WRITE)
    _master_pool: asyncpg.Pool | None = None

    # Пулы слейвов (READ), round-robin через итератор
    _slave_pools: list[asyncpg.Pool] = []
    _slave_cycle: itertools.cycle[asyncpg.Pool] | None = None  # type: ignore[type-arg]

    @classmethod
    async def connect(cls) -> None:
        """Инициализация всех connection pools: master + slaves."""

        # Инициализируем master pool
        cls._master_pool = await asyncpg.create_pool(
            host=settings.database_host,
            port=settings.database_port,
            user=settings.database_user,
            password=settings.database_password,
            database=settings.database_name,
            min_size=settings.database_pool_min_size,
            max_size=settings.database_pool_max_size,
        )

        # Инициализируем slave pools (round-robin)
        cls._slave_pools = []
        slave_hosts = _parse_slave_hosts(settings.database_slave_hosts)

        for host, port in slave_hosts:
            try:
                pool = await asyncpg.create_pool(
                    host=host,
                    port=port,
                    user=settings.database_user,
                    password=settings.database_password,
                    database=settings.database_name,
                    min_size=settings.database_pool_min_size,
                    max_size=settings.database_pool_max_size,
                )
                cls._slave_pools.append(pool)
            except Exception as exc:  # noqa: BLE001
                # Логируем предупреждение, но не падаем — будет fallback на master
                print(f"[Database] Предупреждение: не удалось подключиться к slave {host}:{port}: {exc}")

        # Создаём циклический итератор для round-robin
        if cls._slave_pools:
            cls._slave_cycle = itertools.cycle(cls._slave_pools)
            print(f"[Database] Подключено {len(cls._slave_pools)} slave(s) для read-операций")
        else:
            print("[Database] Слейвы недоступны — все операции направляются на master")

    @classmethod
    async def disconnect(cls) -> None:
        """Закрываем все connection pools."""
        if cls._master_pool:
            await cls._master_pool.close()
            cls._master_pool = None

        for pool in cls._slave_pools:
            await pool.close()
        cls._slave_pools = []
        cls._slave_cycle = None

    @classmethod
    def _get_next_slave_pool(cls) -> asyncpg.Pool:
        """Возвращает следующий slave pool в порядке round-robin."""
        if cls._slave_cycle is None:
            raise RuntimeError("Slave pools не инициализированы")
        return next(cls._slave_cycle)

    @classmethod
    @asynccontextmanager
    async def master_connection(cls) -> AsyncIterator[Any]:
        """Соединение для WRITE операций — всегда master."""
        if cls._master_pool is None:
            raise RuntimeError("Master database pool не инициализирован")
        async with cls._master_pool.acquire() as conn:
            yield conn

    @classmethod
    @asynccontextmanager
    async def slave_connection(cls) -> AsyncIterator[Any]:
        """Соединение для READ операций — round-robin по slave-узлам.

        Fallback на master если слейвы недоступны.
        """
        if not cls._slave_pools:
            # Fallback: слейвы недоступны — читаем с мастера
            async with cls.master_connection() as conn:
                yield conn
            return

        pool = cls._get_next_slave_pool()
        async with pool.acquire() as conn:
            yield conn

    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncIterator[Any]:
        """Алиас для master_connection() — обратная совместимость."""
        async with cls.master_connection() as conn:
            yield conn
