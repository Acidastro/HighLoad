"""Unit-тесты на формат routing_key (homework 6, урок 5)."""
from __future__ import annotations

from uuid import UUID

from app.config import settings
from app.rabbit_client import user_routing_key


def test_user_routing_key_format() -> None:
    uid = UUID("12345678-1234-5678-1234-567812345678")
    rk = user_routing_key(uid)
    assert rk == f"feed.user.{uid}"
    # Парсится обратно так же, как в RabbitWsBridge
    parts = rk.split(".")
    assert len(parts) == 3
    assert parts[0] == "feed"
    assert parts[1] == "user"
    assert UUID(parts[2]) == uid


def test_user_routing_key_uses_settings_prefix() -> None:
    uid = UUID("12345678-1234-5678-1234-567812345678")
    rk = user_routing_key(uid)
    assert rk.startswith(settings.rabbitmq_user_routing_prefix + ".")
