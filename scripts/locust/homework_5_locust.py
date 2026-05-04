"""Нагрузочный тест подсистемы диалогов (homework 5, итерация 7).

Сценарий:
  - 70% запросов: POST /dialog/{user_id}/send (пишем на шарды).
  - 30% запросов: GET  /dialog/{user_id}/list (читаем с шардов).

Особенности:
  - Каждый виртуальный пользователь (HttpUser) — это отдельный UUID,
    JWT генерится клиентом без регистрации (FK на users в messages нет,
    /dialog/* не проверяет существование пользователей в users-кластере).
  - "Эффект Леди Гаги": если LADY_GAGA_RATIO > 0, выделяется 1 пользователь,
    который генерирует половину write-трафика (можно подменить на 0.9 для
    более жёсткого профиля).

Запуск:
    # Базовый профиль (равномерный трафик)
    locust -f scripts/locust/homework_5_locust.py \\
        --host http://localhost:8080 \\
        --users 200 --spawn-rate 50 --run-time 120s --headless \\
        --csv reports/hw5_uniform

    # Профиль "Леди Гага" (1 пользователь генерирует ~50% записей)
    LADY_GAGA_RATIO=0.5 locust -f scripts/locust/homework_5_locust.py \\
        --host http://localhost:8080 \\
        --users 200 --spawn-rate 50 --run-time 120s --headless \\
        --csv reports/hw5_lady_gaga
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import jwt  # type: ignore[import-untyped]
from locust import HttpUser, between, task  # type: ignore[import-untyped]

# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------

# Должен совпадать с app/config.py / .env проекта.
JWT_SECRET = os.environ.get("JWT_SECRET", "your-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"

# Размер пула «получателей» — это и есть количество разных диалогов на одного
# пользователя. Чем больше — тем больше chat_id и тем равномернее распределение
# по шардам.
RECIPIENTS_POOL_SIZE = int(os.environ.get("RECIPIENTS_POOL_SIZE", "500"))

# Доля трафика, которую генерирует один «звёздный» пользователь.
# 0 — выключено. 0.5 — половина всех INSERT идёт от него (имитирует Леди Гагу).
LADY_GAGA_RATIO = float(os.environ.get("LADY_GAGA_RATIO", "0"))

# Общий пул «получателей» — у всех HttpUser один и тот же, чтобы chat_id
# покрывали много разных пар, а не размазывались бесконечно.
_RECIPIENTS: list[UUID] = [uuid4() for _ in range(RECIPIENTS_POOL_SIZE)]

# Звёздный пользователь — общий на весь забег.
_LADY_GAGA: UUID = uuid4()


def _create_token(user_id: UUID) -> str:
    """Генерим JWT тем же алгоритмом, что и app/auth.py — без обращения к API."""
    payload = {
        "sub": str(user_id),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


class DialogsUser(HttpUser):
    """Виртуальный пользователь: пишет/читает диалоги."""

    wait_time = between(0.1, 0.5)

    user_id: UUID
    token: str

    def on_start(self) -> None:
        # Каждый VU получает свой UUID + JWT.
        self.user_id = uuid4()
        self.token = _create_token(self.user_id)
        self.client.headers["Authorization"] = f"Bearer {self.token}"

    @task(7)
    def send_message(self) -> None:
        # С вероятностью LADY_GAGA_RATIO отправителем становится «звезда»,
        # чтобы воспроизвести hot-write profile.
        if LADY_GAGA_RATIO > 0 and random.random() < LADY_GAGA_RATIO:
            sender_id = _LADY_GAGA
            headers = {"Authorization": f"Bearer {_create_token(_LADY_GAGA)}"}
        else:
            sender_id = self.user_id
            headers = None  # дефолтные

        recipient = random.choice(_RECIPIENTS)
        if recipient == sender_id:
            return

        self.client.post(
            f"/dialog/{recipient}/send",
            json={"text": f"hi from {sender_id} at {datetime.now(timezone.utc).isoformat()}"},
            headers=headers,
            name="/dialog/[user_id]/send",
        )

    @task(3)
    def list_dialog(self) -> None:
        recipient = random.choice(_RECIPIENTS)
        if recipient == self.user_id:
            return
        self.client.get(
            f"/dialog/{recipient}/list?limit=50",
            name="/dialog/[user_id]/list",
        )
