"""Pure-функции роутинга для подсистемы диалогов.

Никакого I/O, никаких зависимостей — только хеш-функция.
Это намеренно: модуль легко покрывается unit-тестами и переиспользуется
скриптом решардинга (см. итерацию 6 учебного плана).

Модель:
- `compute_chat_id(u1, u2)`   — детерминированный симметричный ключ диалога.
- `shard_for_chat(cid, n)`    — детерминированный роутинг chat_id → индекс шарда.
"""

from __future__ import annotations

import hashlib
from uuid import UUID

# Версионируем схему хеша. Если в будущем поменяем алгоритм — старые данные
# смигрируем, новые пойдут с другим префиксом. Без этого изменение хеш-функции
# = потеря всего трафика (роутинг перестанет находить старые сообщения).
_CHAT_ID_HASH_VERSION = b"v1"
_SEPARATOR = b"|"


def compute_chat_id(user_a: UUID, user_b: UUID) -> bytes:
    """Возвращает 32-байтовый симметричный идентификатор диалога.

    Симметричность достигается сортировкой пары: chat_id(A,B) == chat_id(B,A).
    Это критично для локальности данных — обе стороны переписки попадают на
    один и тот же шард, и /dialog/list читает с одного узла.
    """
    # UUID сравниваются как 128-битные числа — порядок детерминированный.
    lo, hi = (user_a, user_b) if user_a.int <= user_b.int else (user_b, user_a)
    payload = (
        _CHAT_ID_HASH_VERSION
        + _SEPARATOR
        + lo.bytes
        + _SEPARATOR
        + hi.bytes
    )
    return hashlib.sha256(payload).digest()


def shard_for_chat(chat_id: bytes, n_shards: int) -> int:
    """Возвращает индекс шарда [0, n_shards) для данного chat_id.

    Берём первые 8 байт SHA-256 как big-endian uint64 → mod N.
    SHA-256 равномерен, поэтому распределение по шардам тоже равномерное.
    """
    if n_shards <= 0:
        raise ValueError("n_shards must be positive")
    if len(chat_id) < 8:
        raise ValueError("chat_id is too short, expected >= 8 bytes")

    bucket = int.from_bytes(chat_id[:8], "big")
    return bucket % n_shards
