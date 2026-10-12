-- Миграция: добавление составного B-tree индекса для поиска пользователей
-- по префиксу имени и фамилии.
--
-- Запрос который оптимизируем:
--   SELECT ... FROM users
--   WHERE first_name LIKE $1 AND last_name LIKE $2
--   ORDER BY id
--   где $1 = 'prefix%' и $2 = 'prefix%'
--
-- Обоснование выбора индекса:
--
--   1. B-tree (а не GIN/GiST):
--      GIN и GiST индексы предназначены для полнотекстового поиска (tsvector/tsquery)
--      и паттернов типа '%substring%' (pg_trgm). Для prefix-поиска (LIKE 'x%')
--      B-tree работает эффективно, потому что строки хранятся в лексикографическом
--      порядке, и диапазон 'x' <= s < 'y' точно соответствует LIKE 'x%'.
--      PostgreSQL автоматически оптимизирует LIKE 'prefix%' в range scan по B-tree.
--
--   2. Составной индекс (first_name, last_name) vs два отдельных:
--      При двух отдельных индексах PostgreSQL может использовать только один
--      из них, затем фильтровать результаты вручную (bitmap scan или seq scan).
--      Составной индекс позволяет одним проходом отфильтровать по обоим условиям,
--      значительно сокращая количество возвращаемых строк до применения ORDER BY.
--
--   3. Порядок колонок в составном индексе:
--      (first_name, last_name) — сначала фильтр по имени, потом по фамилии.
--      Выбор порядка основан на предположении о большей селективности имени
--      (меньше строк проходит первый фильтр). На практике порядок можно
--      поменять если фамилии окажутся более селективными.
--
--   4. Почему text_pattern_ops (varchar_pattern_ops):
--      По умолчанию PostgreSQL использует locale-aware сравнение (collation).
--      Для LIKE-запросов нужен C-locale или специальный operator class.
--      varchar_pattern_ops создаёт индекс без collation, который PostgreSQL
--      может использовать для LIKE 'prefix%' в любой locale.

-- Основной составной индекс для поиска по префиксу имени и фамилии
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_first_last_name_prefix
    ON users (first_name varchar_pattern_ops, last_name varchar_pattern_ops);

-- Дополнительный индекс для сортировки по id после фильтрации
-- (UUID как PK уже имеет индекс, но явно упоминаем для документации)
-- PRIMARY KEY (id) уже создаёт индекс автоматически — отдельная миграция не нужна.

-- Проверка результата (запустить вручную после миграции):
-- EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
-- SELECT id, first_name, last_name, birthdate, interests, city
-- FROM users
-- WHERE first_name LIKE 'Al%' AND last_name LIKE 'Ko%'
-- ORDER BY id;
