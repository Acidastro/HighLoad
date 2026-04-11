-- Таблица постов пользователей
CREATE TABLE IF NOT EXISTS posts (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    author_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    text       TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для выборки постов конкретного автора, отсортированных по времени
CREATE INDEX IF NOT EXISTS idx_posts_author_created
    ON posts (author_id, created_at DESC);

-- Таблица дружеских связей (направленная: user_id -> friend_id).
-- Семантика: user_id добавил friend_id в друзья.
CREATE TABLE IF NOT EXISTS friendships (
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    friend_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, friend_id),
    CONSTRAINT no_self_friendship CHECK (user_id <> friend_id)
);

-- Индекс для быстрого поиска друзей пользователя (кого добавил user_id)
CREATE INDEX IF NOT EXISTS idx_friendships_user
    ON friendships (user_id);

-- Индекс для обратной связи (кто добавил данного пользователя) — нужен для fan-out при публикации поста
CREATE INDEX IF NOT EXISTS idx_friendships_friend
    ON friendships (friend_id);
