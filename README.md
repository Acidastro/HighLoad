# Social Network API

Базовый скелет социальной сети на FastAPI + PostgreSQL.

## Требования

- Docker и Docker Compose
- Python 3.10+

## Локальный запуск

### 1. Запуск базы данных

```bash
docker-compose up -d
```

PostgreSQL будет доступен на `localhost:5432`. Миграции применятся автоматически при первом запуске.

### 2. Установка зависимостей

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# или venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 3. Запуск приложения

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API будет доступен на http://localhost:8000

## API Endpoints

### Регистрация пользователя

```
POST /user/register
Content-Type: application/json

{
    "email": "user@example.com",
    "password": "password123",
    "first_name": "Иван",
    "last_name": "Иванов",
    "birthdate": "1990-01-15",
    "gender": "male",
    "interests": "программирование, музыка",
    "city": "Москва"
}
```

### Авторизация

```
POST /login
Content-Type: application/json

{
    "email": "user@example.com",
    "password": "password123"
}
```

Возвращает JWT токен.

### Получение анкеты пользователя

```
GET /user/get/{user_id}
```

## Swagger UI

Документация API доступна по адресу: http://localhost:8000/docs

## Переменные окружения

Можно переопределить через файл `.env`:

```
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres
DATABASE_NAME=social_network
JWT_SECRET=your-secret-key
```

## Остановка

```bash
docker-compose down
```

Для удаления данных:

```bash
docker-compose down -v
```
