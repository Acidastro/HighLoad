#!/bin/bash
# init-master.sh — инициализация мастера: создание replication user и тестовой БД
# Запускается как docker-entrypoint-initdb.d скрипт (один раз при первом запуске)

set -e

echo "=== [init-master.sh] Создание replication user ==="

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Создаём пользователя для репликации
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
            CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator_password';
        END IF;
    END
    \$\$;

    -- Выдаём права на pg_monitor для мониторинга репликации
    GRANT pg_monitor TO replicator;
EOSQL

echo "=== [init-master.sh] Replication user создан ==="
echo "=== [init-master.sh] Инициализация завершена ==="
