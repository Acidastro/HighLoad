#!/bin/bash
# init-slave.sh — инициализация слейва через pg_basebackup с мастера
# Запускается как ENTRYPOINT переопределение в docker-compose для slave-контейнеров
# Переменные окружения:
#   MASTER_HOST      — хост мастера (по умолчанию: postgres-master)
#   MASTER_PORT      — порт мастера (по умолчанию: 5432)
#   REPL_USER        — пользователь репликации (по умолчанию: replicator)
#   REPL_PASSWORD    — пароль репликации (по умолчанию: replicator_password)
#   SLAVE_NAME       — имя слейва для application_name (slave1 или slave2)

set -e

MASTER_HOST="${MASTER_HOST:-postgres-master}"
MASTER_PORT="${MASTER_PORT:-5432}"
REPL_USER="${REPL_USER:-replicator}"
REPL_PASSWORD="${REPL_PASSWORD:-replicator_password}"
SLAVE_NAME="${SLAVE_NAME:-slave1}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

echo "=== [init-slave.sh] Запуск инициализации слейва: $SLAVE_NAME ==="
echo "=== [init-slave.sh] Мастер: $MASTER_HOST:$MASTER_PORT ==="

# Ждём доступности мастера
until PGPASSWORD="$REPL_PASSWORD" pg_isready -h "$MASTER_HOST" -p "$MASTER_PORT" -U "$REPL_USER"; do
    echo "=== [init-slave.sh] Ожидание мастера ($MASTER_HOST:$MASTER_PORT)... ==="
    sleep 2
done

echo "=== [init-slave.sh] Мастер доступен ==="

# Если PGDATA уже инициализирован — просто запускаем postgres
if [ -f "$PGDATA/PG_VERSION" ]; then
    echo "=== [init-slave.sh] PGDATA уже инициализирован, запуск postgres ==="
    exec docker-entrypoint.sh postgres -c config_file=/etc/postgresql/postgresql.conf
fi

echo "=== [init-slave.sh] Выполняем pg_basebackup с мастера ==="

# Создаём базовую копию с мастера
PGPASSWORD="$REPL_PASSWORD" pg_basebackup \
    -h "$MASTER_HOST" \
    -p "$MASTER_PORT" \
    -U "$REPL_USER" \
    -D "$PGDATA" \
    -Fp \
    -Xs \
    -P \
    -R

echo "=== [init-slave.sh] pg_basebackup завершён ==="

# Дополняем primary_conninfo именем слейва для кворумной репликации
cat >> "$PGDATA/postgresql.auto.conf" <<EOF

# Добавлено init-slave.sh: application_name для кворумной синхронной репликации
primary_conninfo = 'host=$MASTER_HOST port=$MASTER_PORT user=$REPL_USER password=$REPL_PASSWORD application_name=$SLAVE_NAME'
EOF

echo "=== [init-slave.sh] primary_conninfo обновлён с application_name=$SLAVE_NAME ==="

# Устанавливаем права на PGDATA
chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"

echo "=== [init-slave.sh] Запуск postgres на слейве ==="
exec docker-entrypoint.sh postgres -c config_file=/etc/postgresql/postgresql.conf
