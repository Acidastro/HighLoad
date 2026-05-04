"""
homework_3_locust.py — Нагрузочный тест для Homework 3: Репликация PostgreSQL

Сценарий:
  - 80% запросов: GET /user/search?first_name=...&last_name=...
  - 20% запросов: GET /user/get/{id}

Тестовые данные: people.v2.csv (формат: "Фамилия Имя,дата,город")
  Для поиска используются префиксы имён из CSV.

Запуск:
    # Базовый запуск (headless)
    locust -f scripts/locust/homework_3_locust.py \
        --host http://localhost:8080 \
        --users 100 --spawn-rate 20 --run-time 120s --headless

    # С веб-интерфейсом (http://localhost:8089)
    locust -f scripts/locust/homework_3_locust.py --host http://localhost:8080
"""

from __future__ import annotations

import csv
import random
from pathlib import Path

from locust import HttpUser, between, task  # type: ignore[import-untyped]

# ---------------------------------------------------------------------------
# Загрузка тестовых данных из CSV
# ---------------------------------------------------------------------------

# Путь к CSV файлу относительно корня проекта
_CSV_PATH = Path(__file__).parent.parent.parent / "people.v2.csv"

def _load_search_params(max_entries: int = 5000) -> list[tuple[str, str]]:
    """Загружает из CSV пары (first_name, last_name) для поиска.

    CSV формат каждой строки: "Фамилия Имя,дата,город"
    Берём первые 2 буквы имени и фамилии как префикс для LIKE-запроса.
    """
    params: list[tuple[str, str]] = []

    if not _CSV_PATH.exists():
        # Fallback — стандартные тестовые данные
        return [
            ("Ал", "Ив"),
            ("Ан", "Пе"),
            ("Ма", "Си"),
            ("Дм", "Ко"),
            ("Ел", "Но"),
        ]

    with open(_CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= max_entries:
                break
            if not row:
                continue
            # Строка: "Фамилия Имя" в первой колонке
            name_parts = row[0].strip().split()
            if len(name_parts) >= 2:
                last_name = name_parts[0]  # Фамилия
                first_name = name_parts[1]  # Имя
                # Берём префикс из 2-3 символов
                fn_prefix = first_name[:2] if len(first_name) >= 2 else first_name
                ln_prefix = last_name[:2] if len(last_name) >= 2 else last_name
                params.append((fn_prefix, ln_prefix))

    return params if params else [("Ал", "Ив"), ("Ан", "Пе"), ("Ма", "Си")]


# Загружаем данные при импорте модуля
_SEARCH_PARAMS = _load_search_params()


# ---------------------------------------------------------------------------
# Locust User
# ---------------------------------------------------------------------------

class SocialNetworkReadUser(HttpUser):  # type: ignore[misc]
    """Пользователь нагрузочного теста — только READ операции.

    Распределение запросов:
    - 80%: GET /user/search (поиск по имени/фамилии)
    - 20%: GET /user/get/{id} (получение пользователя по ID)
    """

    # Задержка между запросами: 0.5–2 секунды (имитация реального пользователя)
    wait_time = between(0.5, 2.0)

    # Список user_id, полученных через поиск — для GET /user/get/{id}
    known_user_ids: list[str] = []

    def on_start(self) -> None:
        """Инициализация при старте: получаем несколько user_id через поиск."""
        # Выполняем несколько поисков чтобы накопить user_id
        for _ in range(3):
            fn, ln = random.choice(_SEARCH_PARAMS)
            with self.client.get(
                "/user/search",
                params={"first_name": fn, "last_name": ln},
                catch_response=True,
                name="[init] GET /user/search",
            ) as response:
                if response.status_code == 200:
                    try:
                        users = response.json()
                        for user in users[:5]:
                            if "id" in user:
                                self.known_user_ids.append(str(user["id"]))
                    except Exception:  # noqa: BLE001
                        pass

    @task(8)  # type: ignore[misc]
    def search_users(self) -> None:
        """80% запросов: GET /user/search — поиск по имени и фамилии."""
        fn_prefix, ln_prefix = random.choice(_SEARCH_PARAMS)

        with self.client.get(
            "/user/search",
            params={"first_name": fn_prefix, "last_name": ln_prefix},
            catch_response=True,
            name="GET /user/search",
        ) as response:
            if response.status_code == 200:
                try:
                    users = response.json()
                    # Сохраняем найденные ID для использования в get_user
                    for user in users[:3]:
                        if "id" in user and user["id"] not in self.known_user_ids:
                            self.known_user_ids.append(str(user["id"]))
                    # Ограничиваем список чтобы не раздувался
                    if len(self.known_user_ids) > 200:
                        self.known_user_ids = self.known_user_ids[-100:]
                    response.success()
                except Exception:  # noqa: BLE001
                    response.success()
            elif response.status_code == 404:
                # Нет результатов — не ошибка
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @task(2)  # type: ignore[misc]
    def get_user_by_id(self) -> None:
        """20% запросов: GET /user/get/{id} — получение пользователя по ID."""
        if not self.known_user_ids:
            # Если ещё нет известных ID — пропускаем
            return

        user_id = random.choice(self.known_user_ids)

        with self.client.get(
            f"/user/get/{user_id}",
            catch_response=True,
            name="GET /user/get/{id}",
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 404:
                # Пользователь мог быть удалён — не считаем ошибкой
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")
