# Social Network API

Учебная социальная сеть на FastAPI + PostgreSQL + Redis. Домашние задания
курса OTUS «Высоконагруженные системы».

## Стек

- **Python 3.11+**, FastAPI, asyncpg, pydantic v2
- **PostgreSQL 16** — 1 master + 2 slave, streaming replication
- **Redis 7** — кэш ленты постов + Redis Streams для async fan-out
- **Docker Compose** — вся инфраструктура одним `up`
- **Locust** — нагрузочное тестирование (проект `/Users/newuser/PycharmProjects/locust`)

## Что реализовано по домашкам

| HW | Тема | Ключевое |
|----|------|----------|
| 1 | Базовый функционал | Регистрация, авторизация (JWT), получение анкеты |
| 2 | Индексы и поиск | Поиск по префиксу имени/фамилии с B-tree индексом, нагрузочные замеры |
| 3 | Репликация PostgreSQL | Master + 2 slave, round-robin чтение со слейвов, тест failover |
| 4 | Лента постов и кэширование | Посты, друзья, фид через Redis Sorted Set + Hash, fan-out-on-write, опциональный async fan-out через Redis Streams |

Отчёты: [`reports/homework_3_report.md`](reports/homework_3_report.md),
[`reports/homework_4_report.md`](reports/homework_4_report.md).

## Архитектура стека

```
┌─────────────┐   writes    ┌──────────────────┐
│  FastAPI    │────────────▶│ postgres-master  │
│   app       │             │  (social_network)│
│             │   reads     └────────┬─────────┘
│             │──┐                   │ streaming replication
│             │  │          ┌────────┴─────────┐
│             │  └─round-rr▶│ postgres-slave1  │
│             │             │ postgres-slave2  │
│             │             └──────────────────┘
│             │
│             │   cache     ┌──────────────────┐
│             │◀───────────▶│  redis:7         │
└──────┬──────┘   stream    │  feed:{uid}      │
       │                    │  post:{id}       │
       │                    │  post_events     │
       │                    └────────┬─────────┘
       │                             │ XREADGROUP
       │                    ┌────────┴─────────┐
       └───────sync path───▶│  feed-worker     │
                            │  (consumer grp)  │
                            └──────────────────┘
```

## Быстрый запуск

```bash
docker compose up -d --build
```

Этого достаточно — поднимаются 7 контейнеров:
`postgres-master`, `postgres-slave1`, `postgres-slave2`, `redis`, `migrate`
(one-shot), `app`, `feed-worker`. Все миграции из `migrations/*.sql`
автоматически накатываются сервисом `migrate` на мастере до старта `app`
и `feed-worker`.

API доступно на `http://localhost:8080` · Swagger `http://localhost:8080/docs`.

### Тестовые данные

```bash
# Локальный Python с зависимостями — для запуска скриптов с хоста
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

python scripts/seed_users.py    # ~1M анкет из people.v2.csv (для hw1-3)
python scripts/load_posts.py    # ~8.4k постов из posts.txt (для hw4)
python scripts/seed_friends.py  # 30k случайных дружеских связей (для hw4)
```

## API Endpoints

### Users (hw1-2)

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `POST` | `/user/register` | — | Регистрация |
| `POST` | `/login` | — | JWT-токен |
| `GET`  | `/user/get/{user_id}` | — | Анкета по ID |
| `GET`  | `/user/search?first_name=X&last_name=Y` | — | Префиксный поиск |

### Friends (hw4)

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `PUT` | `/friend/set/{user_id}` | Bearer | Добавить друга |
| `PUT` | `/friend/delete/{user_id}` | Bearer | Удалить друга |

### Posts & Feed (hw4)

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| `POST` | `/post/create` | Bearer | Создать пост |
| `PUT` | `/post/update` | Bearer | Обновить текст поста |
| `PUT` | `/post/delete/{id}` | Bearer | Удалить пост |
| `GET` | `/post/get/{id}` | — | Пост по ID (публично) |
| `GET` | `/post/feed?offset=0&limit=10` | Bearer | Лента друзей (кэш Redis, cap=1000) |
| `POST` | `/post/feed/rebuild` | Bearer | Пересобрать ленту из БД (recovery) |

### Service

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/health` | Healthcheck |
| `GET` | `/docs` | Swagger UI (автогенерация FastAPI) |
| `GET` | `/openapi.json` | Автогенерированная OpenAPI спека |

Эталонная OpenAPI спека проекта: [`docs/openapi.json`](docs/openapi.json).

## Переменные окружения

Полный список — в `app/config.py` (`Settings`). Основные:

```
# PostgreSQL master (WRITE)
DATABASE_HOST=postgres-master
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres
DATABASE_NAME=social_network
# Slaves (READ), через запятую host:port
DATABASE_SLAVE_HOSTS=postgres-slave1:5432,postgres-slave2:5432
DATABASE_POOL_MIN_SIZE=5
DATABASE_POOL_MAX_SIZE=50

# JWT
JWT_SECRET=your-secret-key-change-in-production
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=60

# Redis (hw4)
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
FEED_MAX_SIZE=1000

# Async fan-out (hw4 Phase 7), по умолчанию выключен
FEED_FANOUT_ASYNC=false
FEED_STREAM_KEY=post_events
FEED_STREAM_GROUP=feed_workers
FEED_STREAM_CONSUMER=worker-1
```

Значения выше — дефолты для Docker Compose (внутри сети `postgres_net`).
Для локального запуска без Docker замените `DATABASE_HOST` и `REDIS_HOST`
на `localhost`.

## Миграции

| Файл | Описание |
|------|----------|
| `migrations/001_create_users.sql` | Таблица `users` |
| `migrations/002_add_search_index.sql` | Составной B-tree индекс для `/user/search` |
| `migrations/003_test_write_table.sql` | Таблица `write_test` для failover-эксперимента (hw3) |
| `migrations/004_create_posts_and_friends.sql` | Таблицы `posts`, `friendships` + индексы |

Миграции применяются автоматически one-shot сервисом `migrate` при каждом
`docker compose up`. Все миграции идемпотентны
(`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`), повторный
прогон безопасен. `app` и `feed-worker` имеют
`depends_on: migrate: service_completed_successfully`, так что HTTP-трафик
не начнёт обслуживаться до применения схемы.

## Нагрузочное тестирование (hw2, hw3)

Результаты и графики — в `reports/before_index/`, `reports/after_indexes/`,
`reports/before_replication/`, `reports/after_replication/`.
Подробности — в [`reports/homework_3_report.md`](reports/homework_3_report.md)
и [`docs/tdd/hw2-load-testing.md`](docs/tdd/hw2-load-testing.md).

### Итоги hw2 (до vs после индекса на `/user/search`)

| Users | p50 до  | p50 после | Улучшение |
|-------|---------|-----------|-----------|
| 1     | 72 ms   | 19 ms     | 3.8x      |
| 10    | 62 ms   | 16 ms     | 3.9x      |
| 100   | 320 ms  | 77 ms     | **4.2x**  |
| 1000  | 20 000 ms | 9 700 ms | 2.1x    |

### Итоги hw3 (репликация, 100 users × 60s)

| Метрика | Без репликации | С репликацией | Δ |
|---|---|---|---|
| p95 | 41 ms | 24 ms | **-41%** |
| p99 | 630 ms | 82 ms | **-87%** |
| RPS | 81.5 | 82.8 | +1.5% |

## Полезные команды

```bash
# Статус стека
docker compose ps

# Логи приложения / воркера
docker logs -f social_network_app
docker logs -f social_network_feed_worker

# Проверка репликации
docker exec social_network_master \
    psql -U postgres -d social_network \
    -c "SELECT application_name, state FROM pg_stat_replication;"

# Состояние Redis Streams (актуально при FEED_FANOUT_ASYNC=true)
docker exec social_network_redis redis-cli XLEN post_events
docker exec social_network_redis redis-cli XINFO GROUPS post_events

# Форс-пересборка ленты через API
curl -X POST http://localhost:8080/post/feed/rebuild \
    -H "Authorization: Bearer <JWT>"

# Повторный прогон миграций (вручную)
docker compose up -d migrate
```

## Остановка

```bash
docker compose down        # остановить, данные сохраняются
docker compose down -v     # остановить и удалить все volumes (⚠️ потеря данных)
```

## Структура проекта

```
app/
  main.py            — FastAPI + lifespan (Database, RedisClient)
  config.py          — Settings (pydantic-settings)
  database.py        — master/slave pool с round-robin
  redis_client.py    — async Redis singleton
  auth.py            — JWT helpers + get_current_user dependency
  models.py          — Pydantic модели
  feed_cache.py      — Redis-кэш ленты (fan-out, rebuild, инвалидация)
  routes/
    users.py         — /user/*, /login
    friends.py       — /friend/*
    posts.py         — /post/*, /post/feed, /post/feed/rebuild
  workers/
    feed_worker.py   — Redis Streams consumer (async fan-out)

migrations/          — идемпотентные SQL миграции (прогоняются migrate-сервисом)
scripts/             — seed_users, load_posts, seed_friends, failover_test
docker/              — postgresql.conf, pg_hba.conf, init-slave.sh
docs/                — openapi.json, homework-специфичные заметки
reports/             — отчёты по ДЗ + CSV/HTML от Locust
```

## Homework 6 — realtime лента через WebSocket + RabbitMQ

Кратко:
- `POST /post/create` публикует событие в exchange `posts.events` (topic) с routing_key `feed.materialize`.
- `feed_worker_rmq` читает очередь `feed.materialize`, материализует ленту в Redis (push) и публикует целевые события `feed.user.<S>` для каждого подписчика.
- Каждый WS-инстанс держит **свою exclusive очередь** `ws.<id>` и динамически биндит её на `feed.user.<uid>` для активных коннектов. Получает события — `ws.send_json` в браузер.
- Для celebrity (`followers >= 10000`) push-fan-out **скипается**, посты дочитываются pull'ом при `GET /post/feed`.

Запуск:
```bash
docker compose up -d
# WS-клиент (в учебных целях):
wscat -c "ws://localhost:8080/post/feed/posted?token=$JWT"
# триггер от друга:
curl -X POST http://localhost:8080/post/create \
     -H "Authorization: Bearer $JWT_FRIEND" \
     -H "Content-Type: application/json" \
     -d '{"text":"hi"}'
# должно прилететь {"type":"post.new", ...}
```

Management UI: `http://localhost:15672` (guest/guest).

### Масштабирование RabbitMQ

В HW6 в compose поднят **один узел** RabbitMQ — этого достаточно для учебного сценария. Ниже — план масштабирования для production по мере роста нагрузки.

#### 1. Vertical scaling (низковисящий фрукт)

Поднять CPU/RAM/диск на узле брокера. RAM — для очередей, NVMe SSD — для durable/quorum очередей. Часто это покупает 5–10× headroom без архитектурных изменений.

#### 2. Clustering (3+ узлов)

Развернуть кластер из 3 узлов (`rabbitmq-1/2/3`) с автоматическим cluster_formation. **Метаданные** (определения exchange, queue, bindings, users) реплицируются на все узлы. **Данные очередей по умолчанию НЕ реплицируются** — очередь живёт на одном узле; падение узла = очередь недоступна, пока он не вернётся.

Используется в связке с durable + quorum queues (см. п. 4) для отказоустойчивости.

#### 3. Mirrored queues (НЕ использовать)

Старая модель репликации очередей через master-slave. **Deprecated с 3.10**, удалена в 4.0. Если встречается в legacy-документации — игнорируй для новых проектов.

#### 4. Quorum queues (рекомендуемо для production)

Современная replicated очередь на алгоритме Raft. Durable, переживает падение меньшинства узлов в кластере (например, 1 из 3). В нашем коде уже включено для `feed.materialize`:

```python
queue = await channel.declare_queue(
    settings.rabbitmq_materialize_queue,
    durable=True,
    arguments={"x-queue-type": "quorum"},
)
```

Для `ws.<id>` quorum **не нужен** — exclusive + auto_delete очереди эфемерны, потеря безопасна (клиенты переподключатся и пересоздадут bindings).

#### 5. Sharding plugin

При росте throughput на одной очереди (десятки тысяч сообщений в секунду) одна очередь становится bottleneck'ом — её обрабатывает один узел кластера. Решение — **rabbitmq-sharding plugin**: декларируется специальный exchange, который распределяет сообщения между N квазиочередями по hash(routing_key). Каждую читает своя группа consumers.

Для нас: если `feed.materialize` начнёт упираться — заменяем её на 8/16 шардированных, воркеры распределяются по группам. Линейный рост пропускной способности.

#### 6. Consistent-hash exchange

Альтернатива sharding plugin. Plugin `rabbitmq-consistent-hash-exchange`: распределяет сообщения по очередям по hash(routing_key). Удобно когда нужно гарантировать «все события одного автора идут одному воркеру» (упорядоченность per-author).

#### 7. Federation / Shovel (гео-распределение)

Для multi-region: события из брокера в DC1 проксируются в брокер DC2.
- **Federation** — публикация одного брокера автоматически реплицируется в exchange другого. Хорошо для глобального fan-out.
- **Shovel** — простая «тележка», которая забирает сообщения из очереди одного брокера и кладёт в exchange другого. Хорошо для миграции и явных перевозок.

#### 8. Lazy queues

Если очередь в норме большая (миллионы сообщений), хранить её содержимое в RAM нерационально. Lazy queues пишут на диск **сразу**, в RAM держат только хвост. Медленнее, но не съедают память. Для бэклога `feed.materialize` при большом spike'е — полезно.

#### 9. Streams (RabbitMQ Streams)

Не путать с Redis Streams. RabbitMQ 3.9+ поддерживает append-only streams для high-throughput сценариев с replay сообщений. Альтернатива Kafka внутри той же инфры. Не используется в HW6, но имеет смысл если потребуется replay/audit log событий постов.

#### План эволюции для нашей соцсети

| Стадия | Узлов | Очередь `feed.materialize` | WS-очереди | Когда переходить |
|---|---|---|---|---|
| HW6 (учебный) | 1 | quorum (single-node) | exclusive | сейчас |
| Bootstrap | 3 | quorum | exclusive | первые тысячи DAU |
| Growth | 3–5 | sharded quorum (8 шардов) | exclusive | десятки тысяч DAU |
| Scale | 5+ | sharded quorum + lazy | exclusive | миллионы постов в сутки |
| Multi-region | 3+ на регион | sharded quorum + federation | exclusive | глобальный продукт |

WS-очереди не нуждаются в шардинге — каждый инстанс держит свою. Бутылочное горлышко на WS-уровне снимается **добавлением WS-инстансов** (см. урок 9 в TDD), не настройкой брокера.

### Линейная масштабируемость WS-сервиса

Каждый инстанс приложения создаёт собственную exclusive очередь и биндит её на `feed.user.<uid>` для своих коннектов. RabbitMQ маршрутизирует копию сообщения только в очереди тех инстансов, у которых есть binding — каждый инстанс не видит чужих сообщений.

Шардирование коннектов по инстансам делает LB (haproxy/nginx) round-robin. Sticky session не нужен — клиент при reconnect может попасть на любой инстанс.

```bash
# Запустить 3 копии WS-инстанса
docker compose up -d --scale app=3
```

LB перед сервисом должен поддерживать WebSocket upgrade (проверь `proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection "upgrade";` в nginx).

## Дальнейшее чтение

- [`reports/homework_3_report.md`](reports/homework_3_report.md) — отчёт по репликации
- [`reports/homework_4_report.md`](reports/homework_4_report.md) — отчёт по ленте и кэшу
- [`docs/openapi.json`](docs/openapi.json) — контракт API
