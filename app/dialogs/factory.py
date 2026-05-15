"""Фабрика DialogRepository по env-флагу DIALOGS_BACKEND (homework 7).

Хранит ссылку на синглтон, который lifespan FastAPI создаёт один раз на
старте; роуты получают его через Depends. Pool Tarantool тоже живёт здесь —
чтобы хозяин его жизненного цикла был один.
"""

from __future__ import annotations

from app.config import settings
from app.dialogs.pg_repository import PgDialogRepository
from app.dialogs.repository import DialogRepository
from app.dialogs.tarantool_client import TarantoolPool
from app.dialogs.tarantool_repository import TarantoolDialogRepository


class DialogRepositoryHolder:
    """Singleton-держатель активной реализации репозитория.

    Один на процесс. setup() вызывается из lifespan один раз, teardown()
    закрывает ресурсы (актуально для Tarantool — нужно закрыть коннект).
    """

    _repository: DialogRepository | None = None
    _tarantool_pool: TarantoolPool | None = None

    @classmethod
    async def setup(cls) -> None:
        backend = settings.dialogs_backend.lower()
        if backend == "tarantool":
            pool = TarantoolPool(
                host=settings.tarantool_host,
                port=settings.tarantool_port,
                reconnect_timeout=settings.tarantool_reconnect_timeout,
            )
            await pool.start()
            cls._tarantool_pool = pool
            cls._repository = TarantoolDialogRepository(pool)
            print(
                f"[DialogRepository] backend=tarantool, "
                f"{settings.tarantool_host}:{settings.tarantool_port}",
            )
        elif backend == "postgres":
            cls._repository = PgDialogRepository()
            print("[DialogRepository] backend=postgres (DialogsCluster)")
        else:
            raise RuntimeError(
                f"Неизвестный DIALOGS_BACKEND: {backend!r}. "
                f"Допустимые значения: 'postgres', 'tarantool'.",
            )

    @classmethod
    async def teardown(cls) -> None:
        if cls._tarantool_pool is not None:
            await cls._tarantool_pool.stop()
            cls._tarantool_pool = None
        cls._repository = None

    @classmethod
    def get(cls) -> DialogRepository:
        if cls._repository is None:
            raise RuntimeError(
                "DialogRepository не инициализирован — забыт вызов setup() в lifespan?",
            )
        return cls._repository


def get_dialog_repository() -> DialogRepository:
    """FastAPI Depends-провайдер."""
    return DialogRepositoryHolder.get()
