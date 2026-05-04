-- Миграция шарда диалогов: таблица сообщений.
-- Применяется идентично на каждом шарде (dialogs-shard0, dialogs-shard1, ...).
-- Шарды между собой НЕ связаны: репликации/FK между шардами нет.

CREATE TABLE IF NOT EXISTS messages (
    -- Локальный для шарда автоинкремент. Глобальная уникальность не гарантируется.
    id           BIGSERIAL   PRIMARY KEY,
    -- Глобально уникальный идентификатор сообщения. Нужен для:
    --   1. Dual-write при решардинге: один и тот же message_id вставляется в два
    --      шарда — `ON CONFLICT (message_id) DO NOTHING` гарантирует идемпотентность.
    --   2. Backfill во время решардинга может работать одновременно с live-трафиком
    --      и не создавать дублей.
    -- В отличие от BIGSERIAL — не зависит от шарда, генерится приложением (UUIDv4).
    message_id   UUID        NOT NULL UNIQUE,
    -- Симметричный ключ диалога: SHA-256(min(u1,u2) || '|' || max(u1,u2)) = 32 байта.
    chat_id      BYTEA       NOT NULL,
    sender_id    UUID        NOT NULL,
    recipient_id UUID        NOT NULL,
    body         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Покрывающий индекс под основной запрос /dialog/list:
--   WHERE chat_id = $1 ORDER BY created_at DESC, id DESC LIMIT ... OFFSET ...
-- id DESC нужен как tie-breaker при коллизии created_at.
CREATE INDEX IF NOT EXISTS idx_messages_chat_created
    ON messages (chat_id, created_at DESC, id DESC);
