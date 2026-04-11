# Отчёт: Homework 4 — Лента постов друзей с Redis-кэшем

## Что сделано

Реализовал ленту постов друзей с кэшированием в Redis, fan-out-on-write
и опциональной асинхронной доставкой через Redis Streams.

### Схема данных (миграция 004)

| Таблица       | Поля                                                               |
|---------------|--------------------------------------------------------------------|
| `posts`       | `id`, `author_id → users.id`, `text`, `created_at`, `updated_at`   |
| `friendships` | `(user_id, friend_id)` PK, CHECK `user_id <> friend_id`            |

Индексы: `idx_posts_author_created(author_id, created_at DESC)`,
`idx_friendships_user(user_id)`, `idx_friendships_friend(friend_id)`.

### Реализованные эндпоинты

| Метод / путь                     | Назначение                                   |
|----------------------------------|----------------------------------------------|
| `PUT /friend/set/{user_id}`      | Добавить друга (+ подмешать его посты в ленту)|
| `PUT /friend/delete/{user_id}`   | Удалить друга (+ вычистить его посты из ленты)|
| `POST /post/create`              | Создать пост + fan-out в ленты фолловеров    |
| `PUT /post/update`               | Обновить текст поста                         |
| `PUT /post/delete/{id}`          | Удалить пост + инвалидация кэша              |
| `GET /post/get/{id}`             | Получить пост (публичный)                    |
| `GET /post/feed`                 | Лента друзей с пагинацией, cap=1000          |
| `POST /post/feed/rebuild`        | Форс-пересборка кэша из БД (для recovery)    |

## Кэширование

**Схема ключей:**
- `feed:{user_id}` — Redis Sorted Set, `score = created_at.timestamp()`,
  `member = post_id`. Capped на 1000 последних постов (`ZREMRANGEBYRANK`).
- `post:{post_id}` — Redis Hash с полями `text`, `author_id`, `created_at`.
  TTL 30 дней + eviction `allkeys-lru`.

**Fan-out-on-write:** при создании поста `SELECT user_id FROM friendships WHERE friend_id = $1`
(мастер, чтобы не словить slave lag) → для каждого фолловера `ZADD feed:{uid} + ZREMRANGEBYRANK`
в pipeline. Post hash заливается раз, фиды получают только `post_id`, что делает
`/post/update` и `/post/delete` простыми и дешёвыми.

**Cache miss rebuild:** если `feed:{uid}` пустой, читаем из БД
`SELECT posts JOIN friendships` и собираем новый snapshot во временном
ключе `feed:{uid}:rebuild:{nonce}`, затем атомарно `RENAME` на боевой ключ.
Это исключает гонку между rebuild и fan-out.

**Read-source решение:** все SQL-чтения в feed-кэше идут с master, а не со
слейвов. Причина: при async-репликации slave может отставать на секунды, и
при cache-miss rebuild "воскрешает" только что удалённые посты. Master даёт
read-after-write гарантию, а нагрузка приемлема, потому что rebuild и fan-out —
редкие операции.

## Асинхронный fan-out (опционально, Phase 7)

Поднят отдельный сервис `feed-worker` (тот же образ, другой entrypoint
`python -m app.workers.feed_worker`):

- При `FEED_FANOUT_ASYNC=true` `/post/create` делает `XADD post_events {event_type, post_id, author_id, created_at}` и возвращает управление немедленно.
- Воркер в consumer group `feed_workers` читает через `XREADGROUP`, вызывает `fan_out_post` и делает `XACK`.
- На старте воркер сначала забирает свои pending сообщения (`XREADGROUP id='0'`), чтобы после рестарта обработать всё, что было в PEL до падения. Это обеспечивает at-least-once.
- Включается одной env-переменной, дефолт — sync fan-out.

## Инфраструктура

Добавил в `docker-compose.yml`:
- `redis:7-alpine` с `--maxmemory 512mb --maxmemory-policy allkeys-lru --appendonly yes --appendfsync everysec` (AOF нужен чтобы не терять state Redis Streams после рестарта), healthcheck, volume `redis_data`.
- `feed-worker` с `restart: unless-stopped`, `depends_on: redis healthy`.
- `restart: unless-stopped` для `app` и `redis`.
- `PYTHONUNBUFFERED=1` в Dockerfile — иначе логи воркера буферизуются и не видны в `docker logs`.

## Тестовые данные

- `scripts/load_posts.py` — загружает `posts.txt` (8392 строки) и раскидывает по авторам round-robin со случайным `created_at` в пределах последнего месяца (детерминированно, seed=42).
- `scripts/seed_friends.py` — генерирует по ~30 случайных друзей на каждого из первых 1000 пользователей (30000 связей).

Тестовые количества: users 999 931, posts 8 392, friendships 30 000.

## Проверка

Прогнал end-to-end вручную через httpx (sync- и async-режимы), сценарий:

| # | Проверка                                                          | Результат |
|---|-------------------------------------------------------------------|-----------|
| 1 | `POST /user/register` × 2, `POST /login` × 2                      | 200 / JWT |
| 2 | `PUT /friend/set` — добавление друга                              | 200       |
| 3 | `PUT /friend/set` на самого себя — CHECK constraint               | 400       |
| 4 | `POST /post/create` с JWT                                         | 200 + id  |
| 5 | `GET /post/get/{id}` публично                                     | 200       |
| 6 | `GET /post/feed` — пост виден в ленте фолловера                   | 200       |
| 7 | `PUT /post/update` — новый текст виден в ленте                    | 200       |
| 8 | `PUT /post/update` на чужой пост                                  | 404       |
| 9 | `PUT /post/delete/{id}` — пост исчезает из лент всех фолловеров   | 200       |
|10 | `PUT /post/delete/{id}` на чужой пост                             | 404       |
|11 | `POST /post/feed/rebuild` — форс-пересборка                       | 200       |
|12 | `GET /post/feed` без токена                                       | 401       |
|13 | `PUT /friend/delete` + `GET /post/feed` — лента пуста             | 200, []   |

Async-путь проверен отдельно: с `FEED_FANOUT_ASYNC=true` создан пост → `XLEN post_events = 1`,
после обработки воркером `pending = 0`, `entries-read = 1`, пост попал в ленту фолловера
за <200 мс.

Код проходит `pyright` (0 errors) и `ruff` (strict E/W/F/I/UP/B/C4/SIM, 0 warnings).

## Что важно понимать при эксплуатации

- Миграции применяются one-shot сервисом `migrate` в docker-compose, который
  прогоняет `migrations/*.sql` в лексикографическом порядке на каждом `up`.
  Все миграции идемпотентны (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`),
  поэтому повторный запуск безопасен. `app` и `feed-worker` стартуют только
  после `migrate: service_completed_successfully`, так что на любом volume
  (и свежем, и уже инициализированном hw1-3) схема гарантированно актуальна.
- Дефолтный режим fan-out — **синхронный**, что достаточно для учебной нагрузки.
  Async-ветка включается одной env-переменной и проверена отдельно.
- Все cache-операции, которые участвуют в write-path, читают с мастера, чтобы не
  получать "воскресение" удалённых постов через stale slave.

## Запуск

```bash
docker compose up -d --build
python scripts/load_posts.py
python scripts/seed_friends.py
```

## Выводы

- Sorted Set + отдельный Hash на тело поста — правильный компромисс: invalidation
  при delete/update становится тривиальной, без re-serialization member-ов.
- Fan-out-on-write даёт `O(log N)` чтение ленты, ценой дополнительной работы при записи;
  для социальной сети с соотношением чтения/записи ~100:1 это выгодно.
- Redis Streams как queue работает аккуратно: consumer group + PEL + recovery на старте
  дают at-least-once без внешнего брокера.
- Slave-lag в реальности ломает rebuild кэша — эту грабель словил на smoke-тесте и починил, переведя SQL-чтения в feed-кэше на мастер.
