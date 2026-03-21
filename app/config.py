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

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.database_user}:{self.database_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )

    class Config:
        env_file = ".env"


settings = Settings()
