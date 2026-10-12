#!/bin/bash
# failover_test.sh — Автоматизация теста отказоустойчивости PostgreSQL
#
# Сценарий:
#   1. Запускаем нагрузку на запись (через API endpoint /write-test)
#   2. Убиваем master (docker stop)
#   3. Определяем самый свежий слейв
#   4. Промоутим slave1 до нового мастера
#   5. Переключаем slave2 на новый мастер
#   6. Проверяем количество потерянных транзакций
#
# Использование:
#   chmod +x scripts/failover_test.sh
#   ./scripts/failover_test.sh [async|sync]
#
# Аргументы:
#   async — тест с асинхронной репликацией (ожидаются потери)
#   sync  — тест с синхронной кворумной репликацией (ожидается 0 потерь)

set -e

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------
REPLICATION_MODE="${1:-async}"
MASTER_CONTAINER="social_network_master"
SLAVE1_CONTAINER="social_network_slave1"
SLAVE2_CONTAINER="social_network_slave2"
APP_HOST="http://localhost:8080"
WRITE_DURATION_SECONDS=30
PG_USER="postgres"
PG_DB="social_network"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ---------------------------------------------------------------------------
# Функции
# ---------------------------------------------------------------------------

check_prerequisites() {
    log_info "Проверка зависимостей..."
    command -v docker >/dev/null 2>&1 || { log_error "docker не найден"; exit 1; }
    command -v curl >/dev/null 2>&1 || { log_error "curl не найден"; exit 1; }
    log_success "Зависимости проверены"
}

check_containers_running() {
    log_info "Проверка запущенных контейнеров..."
    for container in "$MASTER_CONTAINER" "$SLAVE1_CONTAINER" "$SLAVE2_CONTAINER"; do
        if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
            log_error "Контейнер $container не запущен. Запустите: docker-compose up -d"
            exit 1
        fi
    done
    log_success "Все контейнеры запущены"
}

setup_sync_replication() {
    log_info "Настройка СИНХРОННОЙ кворумной репликации на master..."
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "ALTER SYSTEM SET synchronous_commit = 'on';"
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (slave1, slave2)';"
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT pg_reload_conf();"
    log_success "Синхронная репликация включена"
}

setup_async_replication() {
    log_info "Настройка АСИНХРОННОЙ репликации на master..."
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "ALTER SYSTEM SET synchronous_commit = 'off';"
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "ALTER SYSTEM SET synchronous_standby_names = '';"
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT pg_reload_conf();"
    log_success "Асинхронная репликация включена"
}

check_replication_status() {
    log_info "Состояние репликации на master:"
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT client_addr, application_name, state, sync_state, write_lag, flush_lag, replay_lag FROM pg_stat_replication;" || true
}

get_master_write_count() {
    # Считаем количество строк записанных в write_test таблицу на master
    docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM write_test;" 2>/dev/null || echo "0"
}

get_slave_row_count() {
    local container="$1"
    docker exec "$container" psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM write_test;" 2>/dev/null || echo "0"
}

get_slave_lsn() {
    local container="$1"
    docker exec "$container" psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT pg_last_wal_receive_lsn();" 2>/dev/null || echo "0/0"
}

write_load_test() {
    # Нагрузочная запись: вставляем строки напрямую через psql
    local total_written=0
    local start_time
    start_time=$(date +%s)
    local end_time=$((start_time + WRITE_DURATION_SECONDS))

    log_info "Запуск нагрузки на запись (${WRITE_DURATION_SECONDS}s)..."

    while [ "$(date +%s)" -lt "$end_time" ]; do
        # Пакетная вставка 10 строк
        docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
            "INSERT INTO write_test (payload) SELECT 'test_' || generate_series(1,10);" >/dev/null 2>&1 && {
            total_written=$((total_written + 10))
        } || true
        sleep 0.1
    done

    echo "$total_written"
}

promote_slave_to_master() {
    local container="$1"
    log_info "Промоут $container до нового мастера..."
    docker exec "$container" su -c "pg_ctl promote -D /var/lib/postgresql/data" postgres
    sleep 3

    # Проверяем что промоут прошёл
    local is_recovery
    is_recovery=$(docker exec "$container" psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT pg_is_in_recovery();" 2>/dev/null || echo "true")

    if [ "$is_recovery" = "f" ]; then
        log_success "$container успешно промоутирован до мастера"
    else
        log_error "Промоут не удался — $container всё ещё в режиме recovery"
        exit 1
    fi
}

repoint_slave_to_new_master() {
    local slave_container="$1"
    local new_master_container="$2"

    log_info "Переключение $slave_container на новый мастер $new_master_container..."

    docker exec "$slave_container" psql -U "$PG_USER" -d "$PG_DB" -c \
        "ALTER SYSTEM SET primary_conninfo = 'host=$new_master_container port=5432 user=replicator password=replicator_password application_name=slave2';"
    docker exec "$slave_container" psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT pg_reload_conf();" || true

    # pg_wal_replay_resume() если слейв был на паузе
    docker exec "$slave_container" psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT pg_wal_replay_resume();" 2>/dev/null || true

    log_success "$slave_container переключён на $new_master_container"
}

# ---------------------------------------------------------------------------
# Основной сценарий
# ---------------------------------------------------------------------------

echo ""
echo "================================================================"
echo "  PostgreSQL Failover Test — Режим: $(echo "$REPLICATION_MODE" | tr '[:lower:]' '[:upper:]')"
echo "================================================================"
echo ""

check_prerequisites
check_containers_running

# Настройка режима репликации
if [ "$REPLICATION_MODE" = "sync" ]; then
    setup_sync_replication
else
    setup_async_replication
fi

# Проверяем состояние репликации перед тестом
check_replication_status

# Ждём синхронизации
sleep 2

# Сбрасываем таблицу write_test
log_info "Очистка таблицы write_test..."
docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
    "TRUNCATE TABLE write_test;" 2>/dev/null || \
docker exec "$MASTER_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
    "CREATE TABLE IF NOT EXISTS write_test (id SERIAL PRIMARY KEY, payload TEXT, created_at TIMESTAMP DEFAULT NOW()); TRUNCATE write_test;"

# Запускаем нагрузку на запись в фоне
log_info "Запуск нагрузки на запись в фоне..."
write_load_test &
WRITE_PID=$!

# Ждём немного чтобы нагрузка набрала обороты
sleep $((WRITE_DURATION_SECONDS / 3))

# Записываем LSN слейвов ДО остановки мастера
log_info "LSN slave1 до остановки мастера: $(get_slave_lsn "$SLAVE1_CONTAINER")"
log_info "LSN slave2 до остановки мастера: $(get_slave_lsn "$SLAVE2_CONTAINER")"

# Считаем количество строк на мастере в момент остановки
ROWS_ON_MASTER_AT_KILL=$(get_master_write_count)
log_info "Строк на master в момент остановки: $ROWS_ON_MASTER_AT_KILL"

# УБИВАЕМ МАСТЕР
echo ""
log_warn ">>> УБИВАЕМ MASTER (docker stop $MASTER_CONTAINER) <<<"
docker stop "$MASTER_CONTAINER"
log_success "Master остановлен"
echo ""

# Ждём пока нагрузочный тест завершится
wait $WRITE_PID 2>/dev/null || true

# Ждём слейвы
sleep 5

# Определяем самый свежий слейв
LSN1=$(get_slave_lsn "$SLAVE1_CONTAINER")
LSN2=$(get_slave_lsn "$SLAVE2_CONTAINER")
log_info "LSN slave1 после остановки: $LSN1"
log_info "LSN slave2 после остановки: $LSN2"

# Для простоты промоутим slave1 (в продакшне нужно сравнивать LSN)
log_info "Промоутируем slave1 до нового мастера..."
promote_slave_to_master "$SLAVE1_CONTAINER"

# Переключаем slave2 на новый мастер
repoint_slave_to_new_master "$SLAVE2_CONTAINER" "$SLAVE1_CONTAINER"

# ---------------------------------------------------------------------------
# Подсчёт потерь
# ---------------------------------------------------------------------------
sleep 3

ROWS_ON_NEW_MASTER=$(get_slave_row_count "$SLAVE1_CONTAINER")
ROWS_ON_SLAVE2=$(get_slave_row_count "$SLAVE2_CONTAINER")

echo ""
echo "================================================================"
echo "  РЕЗУЛЬТАТЫ ТЕСТА ОТКАЗОУСТОЙЧИВОСТИ"
echo "================================================================"
echo "  Режим репликации:          $(echo "$REPLICATION_MODE" | tr '[:lower:]' '[:upper:]')"
echo "  Строк на master (до kill): $ROWS_ON_MASTER_AT_KILL"
echo "  Строк на новом master:     $ROWS_ON_NEW_MASTER"
echo "  Строк на slave2:           $ROWS_ON_SLAVE2"

LOST=$((ROWS_ON_MASTER_AT_KILL - ROWS_ON_NEW_MASTER))
if [ "$LOST" -lt 0 ]; then LOST=0; fi

echo "  Потеряно транзакций:       $LOST"
echo ""

if [ "$LOST" -eq 0 ]; then
    log_success "Потерь транзакций НЕТ — репликация сработала корректно"
    if [ "$REPLICATION_MODE" = "async" ]; then
        log_warn "(При асинхронной репликации потери возможны — повезло с тайминг'ом)"
    fi
else
    if [ "$REPLICATION_MODE" = "async" ]; then
        log_warn "Потеряно $LOST транзакций — ожидаемо при АСИНХРОННОЙ репликации"
    else
        log_error "Потеряно $LOST транзакций при СИНХРОННОЙ репликации — НЕОЖИДАННО!"
        log_error "Проверьте настройки synchronous_standby_names"
    fi
fi

echo "================================================================"
echo ""
log_info "Для восстановления системы:"
echo "  docker-compose up -d"
echo "  (slave1 теперь работает как master на порту 5433)"
echo ""
