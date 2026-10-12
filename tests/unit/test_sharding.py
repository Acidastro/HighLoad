"""Unit-тесты модуля роутинга шардов диалогов.

Тесты написаны ДО реализации (TDD, итерация 2 учебного плана).
В проекте идентификаторы пользователей — UUID, поэтому compute_chat_id
работает с UUID, а не int.
"""

from __future__ import annotations

import random
from uuid import UUID, uuid4

import pytest

from app.dialogs.sharding import compute_chat_id, shard_for_chat


def _uuid(seed: int) -> UUID:
    """Детерминированный UUID для тестов."""
    rng = random.Random(seed)
    return UUID(int=rng.getrandbits(128), version=4)


class TestComputeChatId:
    def test_symmetric(self) -> None:
        # Ключевой инвариант: chat_id(A,B) == chat_id(B,A),
        # иначе переписка размажется по двум шардам.
        a, b = _uuid(1), _uuid(2)
        assert compute_chat_id(a, b) == compute_chat_id(b, a)

    def test_different_pairs_have_different_ids(self) -> None:
        a, b, c = _uuid(1), _uuid(2), _uuid(3)
        assert compute_chat_id(a, b) != compute_chat_id(a, c)
        assert compute_chat_id(a, b) != compute_chat_id(b, c)

    def test_returns_32_bytes_sha256(self) -> None:
        cid = compute_chat_id(_uuid(42), _uuid(100))
        assert isinstance(cid, bytes)
        assert len(cid) == 32

    def test_self_dialog_is_allowed(self) -> None:
        # Сам с собой — валидный кейс (черновики, заметки).
        u = _uuid(7)
        cid = compute_chat_id(u, u)
        assert isinstance(cid, bytes)
        assert len(cid) == 32

    def test_separator_prevents_byte_collision(self) -> None:
        # Подвох: без разделителя UUID-байты двух пар могли бы склеиться
        # неоднозначно. Разделитель '|' это исключает.
        a, b, c = _uuid(11), _uuid(22), _uuid(33)
        assert compute_chat_id(a, b) != compute_chat_id(b, c)


class TestShardForChat:
    def test_in_range(self) -> None:
        cid = compute_chat_id(_uuid(1), _uuid(2))
        assert 0 <= shard_for_chat(cid, n_shards=2) < 2
        assert 0 <= shard_for_chat(cid, n_shards=8) < 8

    def test_deterministic(self) -> None:
        cid = compute_chat_id(_uuid(100), _uuid(200))
        assert shard_for_chat(cid, 2) == shard_for_chat(cid, 2)
        assert shard_for_chat(cid, 4) == shard_for_chat(cid, 4)

    def test_rejects_zero_shards(self) -> None:
        cid = compute_chat_id(_uuid(1), _uuid(2))
        with pytest.raises(ValueError):
            shard_for_chat(cid, n_shards=0)

    def test_distribution_uniform_over_random_pairs(self) -> None:
        # 10k случайных пар — перекос между двумя шардами не должен превышать 5%.
        n_pairs = 10_000
        n_shards = 2
        counts = [0] * n_shards
        for _ in range(n_pairs):
            counts[
                shard_for_chat(compute_chat_id(uuid4(), uuid4()), n_shards)
            ] += 1
        skew = abs(counts[0] - counts[1]) / n_pairs
        assert skew < 0.05, f"shard skew too high: {counts}, skew={skew:.3f}"

    def test_distribution_uniform_for_n_shards_4(self) -> None:
        n_pairs = 10_000
        n_shards = 4
        counts = [0] * n_shards
        for _ in range(n_pairs):
            counts[
                shard_for_chat(compute_chat_id(uuid4(), uuid4()), n_shards)
            ] += 1
        expected = n_pairs / n_shards
        max_dev = max(abs(c - expected) for c in counts) / expected
        assert max_dev < 0.10, f"shard skew too high for N=4: {counts}"
