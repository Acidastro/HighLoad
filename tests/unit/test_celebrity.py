"""Unit-тесты для is_celebrity (homework 6, урок 10)."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from app.config import settings
from app.services.celebrity import is_celebrity


class FakeRedis:
    def __init__(self) -> None:
        self.storage: dict[str, str] = {}

    async def get(self, key: str) -> bytes | None:
        v = self.storage.get(key)
        return v.encode() if v is not None else None

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.storage[key] = value

    async def delete(self, key: str) -> None:
        self.storage.pop(key, None)


@pytest.mark.asyncio
async def test_is_celebrity_caches_positive() -> None:
    redis = FakeRedis()
    author_id = uuid4()
    threshold = settings.celebrity_followers_threshold

    with patch(
        "app.services.celebrity.followers_count",
        new=AsyncMock(return_value=threshold + 1),
    ) as mock_count:
        first = await is_celebrity(redis, author_id)
        second = await is_celebrity(redis, author_id)

    assert first is True
    assert second is True
    # Кэш сработал — БД дёрнули один раз
    assert mock_count.await_count == 1
    assert redis.storage[f"celebrity:{author_id}"] == "1"


@pytest.mark.asyncio
async def test_is_celebrity_caches_negative() -> None:
    redis = FakeRedis()
    author_id = uuid4()

    with patch(
        "app.services.celebrity.followers_count",
        new=AsyncMock(return_value=5),
    ) as mock_count:
        first = await is_celebrity(redis, author_id)
        second = await is_celebrity(redis, author_id)

    assert first is False
    assert second is False
    assert mock_count.await_count == 1
    assert redis.storage[f"celebrity:{author_id}"] == "0"


@pytest.mark.asyncio
async def test_is_celebrity_threshold_boundary() -> None:
    redis = FakeRedis()
    author_id = uuid4()
    threshold = settings.celebrity_followers_threshold

    with patch(
        "app.services.celebrity.followers_count",
        new=AsyncMock(return_value=threshold),
    ):
        # Граница включает (>= threshold)
        result = await is_celebrity(redis, author_id)
    assert result is True
