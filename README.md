# Social Network API

Социальная сеть на FastAPI + PostgreSQL. Домашнее задание по высоконагруженным системам.

## Стек

- **Python 3.11+**, FastAPI, asyncpg
- **PostgreSQL 16** (Docker)
- **Locust** (нагрузочное тестирование, проект `/Users/newuser/PycharmProjects/locust`)

## Локальный запуск

### 1. База данных

```bash
docker compose up -d
```

PostgreSQL поднимается на `localhost:5432`. Миграция `001_create_users.sql` применяется автоматически.

### 2. Зависимости

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Загрузка тестовых данных (~1M анкет)

```bash
python scripts/seed_users.py
```

Загружает `people.v2.csv` (~1M записей) в таблицу `users`.

### 4. Запуск приложения

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API: http://localhost:8000 · Swagger: http://localhost:8000/docs

## API Endpoints

| Метод | Path | Описание |
|-------|------|----------|
| `POST` | `/user/register` | Регистрация пользователя |
| `POST` | `/login` | Авторизация, возвращает JWT |
| `GET`  | `/user/get/{user_id}` | Получить анкету по ID |
| `GET`  | `/user/search?first_name=X&last_name=Y` | Поиск по префиксу имени и фамилии |
| `GET`  | `/health` | Healthcheck |

### Пример поиска

```bash
curl "http://localhost:8000/user/search?first_name=Ив&last_name=Ив"
```

Запрос: `WHERE first_name LIKE 'Ив%' AND last_name LIKE 'Ив%' ORDER BY id`

## Переменные окружения (`.env`)

```
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres
DATABASE_NAME=social_network
JWT_SECRET=your-secret-key
```

## Миграции

| Файл | Описание |
|------|----------|
| `migrations/001_create_users.sql` | Создание таблицы `users` |
| `migrations/002_add_search_index.sql` | Составной B-tree индекс для `/user/search` |

Применить миграцию вручную:

```bash
docker exec -i social_network_db psql -U postgres -d social_network \
  < migrations/002_add_search_index.sql
```

## Нагрузочное тестирование (Homework 2)

Результаты в `docs/tdd/hw2-load-testing.md`, графики в `reports/charts/`.

### Итоги (до vs после индекса)

| Users | p50 до  | p50 после | Улучшение |
|-------|---------|-----------|-----------|
| 1     | 72 ms   | 19 ms     | 3.8x      |
| 10    | 62 ms   | 16 ms     | 3.9x      |
| 100   | 320 ms  | 77 ms     | **4.2x**  |
| 1000  | 20 000 ms | 9 700 ms | 2.1x    |

### Запуск тестов

```bash
cd /Users/newuser/PycharmProjects/locust
REPORTS=/Users/newuser/PycharmProjects/HighLoad/reports

locust -f locust_app/locustfile_search.py \
  --headless --users 100 --spawn-rate 20 --run-time 120s \
  --host http://127.0.0.1:8000 \
  --csv $REPORTS/before_index/L3 --html $REPORTS/before_index/L3.html
```

## Остановка

```bash
docker compose down        # остановить
docker compose down -v     # остановить + удалить данные
```
