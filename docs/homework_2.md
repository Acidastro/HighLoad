# Homework 2 — Нагрузочное тестирование /user/search

## До индекса

Seq Scan по 1M строк. При 100+ конкурентных запросах деградация:

| Users | p50     | p95      |
|-------|---------|----------|
| 1     | 72 ms   | 270 ms   |
| 10    | 62 ms   | 220 ms   |
| 100   | 320 ms  | 1 100 ms |
| 1000  | 20 000 ms | 42 000 ms |

## Индекс

```sql
CREATE INDEX CONCURRENTLY idx_users_first_last_name_prefix
    ON users (first_name varchar_pattern_ops, last_name varchar_pattern_ops);
```

**Почему такой:** запрос использует `LIKE 'prefix%'` — B-tree покрывает prefix-поиск через range scan.
`varchar_pattern_ops` нужен чтобы планировщик использовал индекс при нелатинской локали.
Составной — чтобы оба условия фильтровались одним проходом.

## После индекса

Bitmap Index Scan. Execution time типичного запроса: 11 ms.

| Users | p50     | p95     | Улучшение p50 |
|-------|---------|---------|---------------|
| 1     | 19 ms   | 130 ms  | 3.8x          |
| 10    | 16 ms   | 130 ms  | 3.9x          |
| 100   | 77 ms   | 520 ms  | **4.2x**      |
| 1000  | 9 700 ms | 26 000 ms | 2.1x       |

При 1000 users бутылочное горлышко — asyncpg pool (max_size=20), не БД.

## Артефакты

- `migrations/002_add_search_index.sql` — SQL индекса
- `reports/charts/` — графики latency и throughput
- `reports/before_index/`, `reports/after_index/` — сырые CSV/HTML от Locust
