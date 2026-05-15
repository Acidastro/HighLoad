# Dockerfile для FastAPI приложения Social Network
# Python 3.11+ в соответствии со стандартами проекта

FROM python:3.11-slim

# Небуферизованный stdout/stderr — критично для docker logs фоновых воркеров
ENV PYTHONUNBUFFERED=1

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости (нужны для asyncpg компиляции)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код приложения (включая workers для feed_worker сервиса)
COPY app/ ./app/

# Порт приложения
EXPOSE 8080

# Запуск через uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
