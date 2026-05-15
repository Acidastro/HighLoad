"""Нагрузочный тест ДЗ-7: сравнение бэкендов диалогов Postgres vs Tarantool.

Контракт API не менялся (POST /dialog/{user_id}/send, GET /dialog/{user_id}/list),
поэтому переиспользуем готовый сценарий из ДЗ-5. Отличается ТОЛЬКО переменная
среды DIALOGS_BACKEND, которой переключается бэкенд приложения, а не сам тест.

Методология "до/после":
    # 1. Stage Postgres
    export DIALOGS_BACKEND=postgres
    docker compose up -d
    python scripts/seed_dialogs_tarantool.py   # seed обоих хранилищ
    locust -f scripts/locust/homework_7_locust.py \\
        --host http://localhost:8080 --users 200 --spawn-rate 50 \\
        --run-time 5m --headless \\
        --csv reports/homework_7_tarantool/pg_uniform

    # 2. Stage Tarantool
    export DIALOGS_BACKEND=tarantool
    docker compose up -d --no-deps app
    locust -f scripts/locust/homework_7_locust.py \\
        --host http://localhost:8080 --users 200 --spawn-rate 50 \\
        --run-time 5m --headless \\
        --csv reports/homework_7_tarantool/tnt_uniform

    # 3. Профиль "lady_gaga" — добавь LADY_GAGA_RATIO=0.5 к команде locust.

Принципы честного бенчмарка (см. урок 11):
  - одинаковая железка, та же compose-сборка;
  - одинаковый pre-seed dataset в обоих хранилищах;
  - первые 30s прогона выкидываются (warm-up);
  - длительность ≥ 5 минут;
  - сценарий тот же, меняется только бэкенд (env-флаг).
"""

# Сам сценарий описан в homework_5_locust.py и не дублируется здесь, чтобы
# гарантировать побайтовое совпадение нагрузки.
from scripts.locust.homework_5_locust import DialogsUser  # noqa: F401  # type: ignore[unused-import]

__all__ = ["DialogsUser"]
