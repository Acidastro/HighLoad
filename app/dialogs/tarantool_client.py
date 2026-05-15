"""Async-клиент к Tarantool (homework 7).

Tarantool — однопоточный fiber-сервер, один TCP-коннект уже мультиплексирует
тысячи параллельных запросов. Поэтому здесь нет настоящего пула как в asyncpg:
держим одну `asynctnt.Connection`, и этого достаточно. Класс называется
TarantoolPool для симметрии с другими "pool"-обёртками в проекте.

Lifecycle:
    pool = TarantoolPool(host, port)
    await pool.start()    # в lifespan FastAPI на startup
    await pool.call("dialog_send", ["a", "b", "hi"])
    await pool.stop()     # на shutdown
"""

from __future__ import annotations

from typing import Any

import asynctnt


class TarantoolPool:
    """Тонкая обёртка над asynctnt.Connection.

    Зачем обёртка, а не использовать Connection напрямую:
      1. Чтобы lifespan FastAPI работал с одним методом start/stop.
      2. Чтобы в одном месте логировать таймауты/reconnect.
      3. Чтобы при необходимости расширить до round-robin по N коннектам без
         изменения вызывающего кода.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        reconnect_timeout: float = 1.0,
    ) -> None:
        self._host = host
        self._port = port
        self._reconnect_timeout = reconnect_timeout
        self._conn: asynctnt.Connection | None = None

    async def start(self) -> None:
        """Поднимает TCP-соединение и держит его до stop()."""
        conn = asynctnt.Connection(
            host=self._host,
            port=self._port,
            # При обрыве asynctnt сам пробует переподключиться раз в N сек.
            # Без этой опции после reconnect-сценария коннект остаётся мёртвым.
            reconnect_timeout=self._reconnect_timeout,
        )
        await conn.connect()
        self._conn = conn

    async def stop(self) -> None:
        if self._conn is not None:
            await self._conn.disconnect()
            self._conn = None

    async def call(self, func: str, args: list[Any]) -> list[Any]:
        """Вызывает UDF Tarantool и возвращает её результат.

        asynctnt возвращает Response с .body — массив из результатов функции
        (Tarantool по протоколу iproto всегда оборачивает результат в массив).
        Для Lua-функции, которая делает `return X` — body будет [X].
        """
        if self._conn is None:
            raise RuntimeError("TarantoolPool не запущен: вызови start() сначала")
        response = await self._conn.call(func, args)
        return list(response.body)
