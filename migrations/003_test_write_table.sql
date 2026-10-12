-- 003_test_write_table.sql
-- Создание тестовой таблицы для эксперимента отказоустойчивости (failover test)
-- Используется в scripts/failover_test.sh для замера потерь транзакций

CREATE TABLE IF NOT EXISTS write_test (
    id         SERIAL PRIMARY KEY,
    payload    TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Комментарий для документации
COMMENT ON TABLE write_test IS 'Тестовая таблица для замера потерь транзакций при failover';
COMMENT ON COLUMN write_test.payload IS 'Произвольная полезная нагрузка (не индексируется)';
