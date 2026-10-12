# Homework 7 — Перенос модуля диалогов в Tarantool (in-memory + UDF)

## 0. Цель ДЗ

Перенести хранение одного из модулей приложения в in-memory СУБД с поддержкой
application server (UDF). Логику модуля переписать на хранимые процедуры.
Провести нагрузочное тестирование «до/после» и сравнить.

Источник: `docs/homework_7.md`.

## 1. Принятые решения

| Решение | Что выбрано | Почему |
|---|---|---|
| Модуль | **Диалоги** | Уже шардирован в PG (ДЗ-5) → есть готовый baseline и Locust-сценарий. Простая схема, read-heavy профиль. |
| In-memory СУБД | **Tarantool 2.11** (не Redis) | Полноценные composite-индексы, first-class Lua-UDF, в задании прямо упомянут. Redis в проекте уже занят кэшем ленты (ДЗ-4). |
| Движок | **memtx** | Read-heavy, объём диалогов помещается в RAM. vinyl на чтениях медленнее. |
| Ключ чата | синтетический `chat_key = min(uA,uB) .. ':' .. max(uA,uB)` | Симметризует пару. Тот же приём, что и `compute_chat_id` из ДЗ-5. |
| Индекс для list | composite TREE `(chat_key, created_at, id)` | Один индекс, итератор `REQ` даёт `ORDER BY created_at DESC LIMIT N` без сканов. `id` третьим — tie-breaker для стабильной пагинации. |
| Python-драйвер | **asynctnt** | Native asyncio (`await conn.call`), не блокирует event loop FastAPI. |
| Переключение бэкендов | env-флаг `DIALOGS_BACKEND=postgres|tarantool` + Strategy pattern (`DialogRepository` Protocol) | Честный A/B-бенчмарк без правок HTTP-слоя между прогонами. |
| Durability | WAL + snapshot в `tarantool_data` volume | Сообщения переживают `docker compose restart`. |

## 2. Архитектура

```
HTTP Client / Locust
        │
        ▼
   FastAPI (app)  ──── DIALOGS_BACKEND=postgres ──► PgDialogRepository ──► DialogsCluster (2 шарда PG)
                  └── DIALOGS_BACKEND=tarantool ──► TarantoolDialogRepository
                                                            │
                                                            ▼ conn.call (iproto)
                                                  ┌──────────────────────────┐
                                                  │ Tarantool 2.11           │
                                                  │  UDF (Lua):              │
                                                  │    dialog_send           │
                                                  │    dialog_list           │
                                                  │      │                   │
                                                  │      ▼                   │
                                                  │  space dialog_messages   │
                                                  │  PK(id) + by_chat        │
                                                  │  (chat_key,created_at,id)│
                                                  └──────────────────────────┘
```

Код приложения общается с Tarantool **только** через
`conn.call('dialog_send'|'dialog_list', ...)`. Никаких прямых обращений к
space через драйвер — это требование ДЗ и проверка инкапсуляции схемы.

## 3. Схема Tarantool

### Space `dialog_messages`

| Поле | Тип | Назначение |
|---|---|---|
| `id` | unsigned | PK, sequence `dialog_messages_seq` |
| `from_user_id` | string | UUID отправителя |
| `to_user_id` | string | UUID получателя |
| `chat_key` | string | `min(uA,uB) .. ':' .. max(uA,uB)` |
| `text` | string | до 10 000 символов |
| `created_at` | number | unix ts в миллисекундах |

### Индексы

| Индекс | Тип | Поля | Unique | Назначение |
|---|---|---|---|---|
| `primary` | TREE | `id` | да (sequence) | физический порядок, доступ по id |
| `by_chat` | TREE | `chat_key`, `created_at`, `id` | нет | `dialog_list` через `iterator='REQ'` |

DDL — в `tarantool/init.lua`.

## 4. Контракт UDF

| UDF | Аргументы | Возврат |
|---|---|---|
| `dialog_send(from, to, text)` | 3 строки | `{ id, created_at }` или `error()` при невалидных входных данных |
| `dialog_list(a, b, limit, offset)` | 2 строки + 2 числа | массив `{ id, from_user_id, to_user_id, text, created_at }` DESC по `created_at`; `limit` clamp до 500 |

Гранты: `box.schema.user.grant('guest', 'execute', 'function', '<name>')`.
Код — в `tarantool/app/dialogs.lua`.

## 5. Как проверить локально

```bash
# 1. Поднять стек
docker compose up -d
docker compose logs tarantool   # видим "Tarantool ready, listening on 3301"

# 2. Seed обоих хранилищ одинаковым набором
PYTHONPATH=. .venv/bin/python scripts/seed_dialogs_tarantool.py \
  --users 100 --msgs-per-user 30 --backend both

# 3. Дёрнуть UDF напрямую через REPL Tarantool
docker compose exec tarantool tarantoolctl connect /var/run/tarantool/tarantool.sock
# > box.space.dialog_messages:len()
# > require('net.box').connect('127.0.0.1:3301'):call('dialog_list', {'alice','bob',5,0})

# 4. Переключить FastAPI на Tarantool и дёрнуть HTTP
export DIALOGS_BACKEND=tarantool
docker compose up -d --force-recreate --no-deps app
docker compose logs app | grep DialogRepository
# → [DialogRepository] backend=tarantool, tarantool:3301
```

## 6. Нагрузочное тестирование

### 6.1. Конфигурация стенда

| Параметр | Значение |
|---|---|
| Hardware | Apple Silicon (MBP), Darwin 25.2.0 |
| Docker | Desktop, Linux VM, default resources |
| Postgres | 16-alpine, 2 шарда (`dialogs-shard0`/`shard1`) |
| Tarantool | 2.11 (memtx, 256 MB), 1 инстанс, WAL mode = write |
| FastAPI | uvicorn standard, 1 воркер, asynctnt 2.4 / asyncpg |
| Locust | 2.43.x, headless, 100 users, spawn-rate 25/s |

### 6.2. Методология

- Бэкенд переключается env-флагом `DIALOGS_BACKEND` — код, инфраструктура, версия docker-compose **не меняются**, только перезапускается `app`-контейнер.
- Pre-seed одинаковым датасетом: `seed_dialogs_tarantool.py --users 100 --msgs-per-user 30 --backend both` → ~2971 сообщений идентично в оба хранилища (fixed `random.Random(42)`).
- Сценарий: `scripts/locust/homework_5_locust.py` (тот же, что в ДЗ-5). 70% `POST send`, 30% `GET list?limit=50`. `wait_time = between(0.1, 0.5)`.
- Длительность каждого прогона: **2 минуты** (≈40k запросов на прогон — статистически достаточно для p99).
- Два профиля: **uniform** (`LADY_GAGA_RATIO=0`) и **lady_gaga** (`LADY_GAGA_RATIO=0.5` — один пользователь делает 50% записей в один и тот же чат).

### 6.3. Результаты — Uniform

| Метрика | Postgres (sharded) | Tarantool | Δ |
|---|---|---|---|
| **RPS total** | **325.6** | **326.5** | ~0 |
| `/send` p50 | 2 ms | 2 ms | 0 |
| `/send` p95 | 6 ms | 6 ms | 0 |
| `/send` p99 | 15 ms | 17 ms | +2 ms (PG лучше) |
| `/send` max | 276 ms | **100 ms** | −64% (TNT лучше) |
| `/list` p50 | 2 ms | 2 ms | 0 |
| `/list` p95 | 6 ms | 6 ms | 0 |
| `/list` p99 | 12 ms | 16 ms | +4 ms (PG лучше) |
| `/list` max | 281 ms | **65 ms** | −77% (TNT лучше) |
| failures | 0% | 0% | — |

### 6.4. Результаты — Lady Gaga (50% записей в один чат)

| Метрика | Postgres (sharded) | Tarantool | Δ |
|---|---|---|---|
| **RPS total** | 322.5 | 325.7 | +1% |
| `/send` p50 | 4 ms | **2 ms** | −50% (TNT лучше) |
| `/send` p95 | 10 ms | **5 ms** | −50% (TNT лучше) |
| `/send` p99 | 32 ms | **9 ms** | −72% (TNT лучше) |
| `/send` max | 442 ms | **66 ms** | −85% (TNT лучше) |
| `/list` p50 | 3 ms | **2 ms** | −33% |
| `/list` p95 | 9 ms | **5 ms** | −44% |
| `/list` p99 | 26 ms | **8 ms** | −69% |
| `/list` max | 333 ms | **63 ms** | −81% |
| failures | 0% | 0% | — |

## 7. Выводы

### Где Tarantool однозначно выиграл

1. **Хвост распределения (p99.9, max) — всегда.**
   - Uniform: PG max 281 ms vs TNT **100 ms** на `/send`, 65 ms на `/list`.
   - Lady Gaga: PG max 442 ms vs TNT **66 ms**.

   Объяснение: у Postgres задержки определяются дисковым I/O — `fsync` WAL может занять много при общем дисковом давлении, плюс холодные страницы в page cache → миллисекунды на чтение. Tarantool **всё в RAM**, нет компонента, дающего >100 ms.

2. **Lady Gaga effect — катастрофически выгодно для Tarantool.**
   PG деградирует по всем процентилям при горячем чате (p99 14→30 ms, max 281→442 ms), потому что **шардинг по `chat_id`** концентрирует ВЕСЬ горячий трафик в один шард — lock contention на одной таблице, WAL одного диска. Tarantool однопоточный по дизайну — для него «горячий чат» ничем не отличается от «холодного».

   Конкретно: TNT в lady_gaga даже **слегка быстрее** uniform (p99 9 ms vs 17 ms) — потому что меньше разнообразных tuple'ов прогревают индекс по `chat_key`.

### Где Tarantool слегка проиграл

- **p99 в uniform-сценарии**: TNT 17 ms vs PG 15 ms на `/send`, 16 vs 12 ms на `/list`.

   Объяснение: PG с 2 шардами даёт 2 параллельных storage-движка → запросы могут выполняться **физически параллельно**. Tarantool — один fiber-loop, при 100 одновременных пользователях короткие операции могут стоять в очереди event loop. Чтобы перебить — нужен **vshard** (горизонтальный шардинг Tarantool).

### Где паритет

- **Throughput (RPS)**: на этой нагрузке оба упёрлись в latency × users, не в storage. Оба отдают ~325 RPS. Чтобы реально нагрузить storage, нужны 500–1000 пользователей.

### Главный практический вывод

При **bounded latency** (SLA «p99 ≤ 30 ms всегда») Tarantool в режиме hot-spot выигрывает у шардированного Postgres в **3× по p99** и в **6.7× по max latency**. Это и есть тот сценарий, ради которого вообще берут in-memory СУБД с прикладной логикой на UDF.

## 8. Что бы дальше улучшить

- **Keyset pagination** вместо offset для `/list` — убирает O(N) на deep paging (релевантно для длинных чатов).
- **VShard** — горизонтальный шардинг Tarantool, чтобы получить параллелизм по ядрам и снять p99 в uniform-сценарии.
- **Async replication Tarantool** — HA-кластер из 2 узлов вместо одиночного инстанса.
- **Дольше прогон (5–10 минут)** + **больше пользователей (500–1000)** — увидим, где реально упрётся хранилище, а не клиентская latency.
- **Снять `docker stats`** во время прогонов — для оценки CPU/RAM footprint.
