from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ---------------------------------------------------------------------------
    # Master (WRITE) — существующие настройки
    # ---------------------------------------------------------------------------
    database_host: str = "localhost"
    database_port: int = 5432
    database_user: str = "postgres"
    database_password: str = "postgres"
    database_name: str = "social_network"

    # ---------------------------------------------------------------------------
    # Slaves (READ) — через запятую в формате host:port
    # Пример: "localhost:5433,localhost:5434"
    # В Docker: "postgres-slave1:5432,postgres-slave2:5432"
    # ---------------------------------------------------------------------------
    database_slave_hosts: str = "localhost:5433,localhost:5434"

    # ---------------------------------------------------------------------------
    # Connection pool settings
    # Увеличено с 20 до 50 для поддержки нагрузки 1000+ users (hw2 bottleneck)
    # ---------------------------------------------------------------------------
    database_pool_min_size: int = 5
    database_pool_max_size: int = 50

    # ---------------------------------------------------------------------------
    # JWT
    # ---------------------------------------------------------------------------
    jwt_secret: str = "your-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60

    # ---------------------------------------------------------------------------
    # Redis (cache для ленты постов)
    # ---------------------------------------------------------------------------
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    # Максимальный размер ленты одного пользователя (последние N постов)
    feed_max_size: int = 1000

    # ---------------------------------------------------------------------------
    # Async fan-out (Phase 7)
    # Если True — /post/create публикует событие в Redis Stream post_events
    # и возвращает управление сразу; fan-out делает воркер feed_worker.
    # Если False — fan-out выполняется синхронно в обработчике.
    # ---------------------------------------------------------------------------
    feed_fanout_async: bool = False
    feed_stream_key: str = "post_events"
    feed_stream_group: str = "feed_workers"
    feed_stream_consumer: str = "worker-1"
    feed_stream_block_ms: int = 5000
    feed_stream_batch: int = 100

    # ---------------------------------------------------------------------------
    # Dialogs sharding (homework 5)
    # Список DSN шардов через запятую, в порядке индексов: shard0, shard1, ...
    # Пример (локально):
    #   postgresql://postgres:postgres@localhost:5440/dialogs,
    #   postgresql://postgres:postgres@localhost:5441/dialogs
    # В Docker:
    #   postgresql://postgres:postgres@dialogs-shard0:5432/dialogs,
    #   postgresql://postgres:postgres@dialogs-shard1:5432/dialogs
    # ---------------------------------------------------------------------------
    dialogs_shards: str = (
        "postgresql://postgres:postgres@localhost:5440/dialogs,"
        "postgresql://postgres:postgres@localhost:5441/dialogs"
    )
    dialogs_pool_min_size: int = 2
    dialogs_pool_max_size: int = 10

    # ---------------------------------------------------------------------------
    # Resharding feature-flags (homework 5, итерация 6)
    # Нормальный режим: dialogs_read_n == dialogs_write_n == len(SHARDS).
    # Во время решардинга:
    #   1) WRITE_N = новое число шардов, READ_N = старое, DUAL_WRITE=true
    #      → INSERT в оба шарда, чтения по старой схеме.
    #   2) Backfill — переносит исторические данные на новые позиции.
    #   3) READ_N = WRITE_N → чтения переключаются на новую схему.
    #   4) DUAL_WRITE=false, cleanup — удаляем «уехавшие» данные на старых шардах.
    # 0 означает «как len(SHARDS)» (читается в DialogsCluster.connect()).
    # ---------------------------------------------------------------------------
    dialogs_read_n: int = 0
    dialogs_write_n: int = 0
    dialogs_dual_write: bool = False

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.database_user}:{self.database_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    class Config:
        env_file = ".env"


settings = Settings()
