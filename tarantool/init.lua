-- tarantool/init.lua — entrypoint, исполняется при старте инстанса.
--
-- Содержит:
--   1. box.cfg{...}        — поднимаем БД, открываем iproto-порт 3301.
--   2. Грант гостю          — упрощение для учебной задачи.
--   3. Schema bootstrap     — space dialog_messages + sequence + два индекса.
--
-- UDF (dialog_send, dialog_list) будут подключены на уроках 6-8 через require.

box.cfg{
    listen = 3301,
    memtx_memory = 256 * 1024 * 1024,  -- 256 MB под данные
    wal_mode = 'write',
    log_level = 5,
}

-- Гостевому пользователю даём super — чтобы не возиться с паролями в учебной задаче.
-- В production так делать НЕЛЬЗЯ.
box.once('grant_guest_v1', function()
    box.schema.user.grant('guest', 'super', nil, nil, { if_not_exists = true })
end)

-- ---------------------------------------------------------------------------
-- Schema: space dialog_messages
-- ---------------------------------------------------------------------------

-- Sequence для автогенерации id (аналог SERIAL).
box.schema.sequence.create('dialog_messages_seq', { if_not_exists = true })

-- Space dialog_messages — здесь будут жить все сообщения диалогов.
local messages = box.schema.space.create('dialog_messages', {
    if_not_exists = true,
    format = {
        { name = 'id',           type = 'unsigned' },
        { name = 'from_user_id', type = 'string'   },  -- UUID отправителя
        { name = 'to_user_id',   type = 'string'   },  -- UUID получателя
        { name = 'chat_key',     type = 'string'   },  -- min(uA,uB)..':'..max(uA,uB)
        { name = 'text',         type = 'string'   },
        { name = 'created_at',   type = 'number'   },  -- unix ts в миллисекундах
    },
})

-- Primary index: по id. Sequence будет автоматически подставлять next id,
-- если в insert передать nil вместо id.
messages:create_index('primary', {
    parts = { 'id' },
    sequence = 'dialog_messages_seq',
    if_not_exists = true,
})

-- Secondary index: composite (chat_key, created_at, id).
--   - chat_key      — фиксирует пару пользователей (симметрично).
--   - created_at    — сортировка по времени.
--   - id            — tie-breaker для стабильной пагинации при равных ts.
-- Через этот индекс будет работать выборка "последние N сообщений чата DESC".
messages:create_index('by_chat', {
    parts = { 'chat_key', 'created_at', 'id' },
    unique = false,
    type = 'tree',
    if_not_exists = true,
})

-- ---------------------------------------------------------------------------
-- UDF: подключаем модуль и регистрируем функции для удалённого вызова.
-- ---------------------------------------------------------------------------

-- Путь к Lua-модулям. /opt/tarantool/?.lua означает: для require('app.dialogs')
-- искать файл /opt/tarantool/app/dialogs.lua. Точки в имени модуля
-- автоматически превращаются в слэши.
package.path = '/opt/tarantool/?.lua;/opt/tarantool/?/init.lua;' .. package.path

local dialogs = require('app.dialogs')

-- Шаг 2: делаем функции глобально доступными по имени, под которым клиент
-- будет их звать. rawset обходит метатаблицы — это стандартный приём.
rawset(_G, 'dialog_send', dialogs.dialog_send)
rawset(_G, 'dialog_list', dialogs.dialog_list)

-- Шаг 3: регистрируем в системном каталоге функций + выдаём guest право
-- на execute. Без этих двух строк iproto-клиент получит "access denied".
box.schema.func.create('dialog_send', { if_not_exists = true })
box.schema.func.create('dialog_list', { if_not_exists = true })
box.schema.user.grant('guest', 'execute', 'function', 'dialog_send',
                      { if_not_exists = true })
box.schema.user.grant('guest', 'execute', 'function', 'dialog_list',
                      { if_not_exists = true })

print('[init.lua] Tarantool ready, listening on 3301')
print('[init.lua] space dialog_messages: ' .. messages:len() .. ' tuples loaded from snap/xlog')
print('[init.lua] UDF registered: dialog_send, dialog_list')
