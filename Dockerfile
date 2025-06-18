# syntax=docker/dockerfile:1.4

# --- ЭТАП 1: Установщик зависимостей ---
# Этот этап будет кешироваться очень агрессивно
FROM python:3.11-slim AS deps-installer

# Установка системных зависимостей и Poetry
# Этот слой будет пересобираться, только если изменится эта команда

RUN echo ${CI_REGISTRY}
RUN echo $CI_REGISTRY 
RUN echo "SHTTP_PROXY=${SHTTP_PROXY}" && echo "SHTTPS_PROXY=${SHTTPS_PROXY}"


RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    pip install --no-cache-dir "poetry==2.1.3" && \
    rm -rf /var/lib/apt/lists/*

# Создаем директорию для нашего проекта
WORKDIR /app

# Копируем ТОЛЬКО файлы, описывающие зависимости.
# Docker пересоберет следующий шаг, только если эти два файла изменятся.
COPY pyproject.toml poetry.lock ./

# Устанавливаем зависимости.
# Этот слой, самый долгий, будет взят из кеша, если pyproject.toml и poetry.lock не менялись.
RUN poetry config virtualenvs.in-project true && \
    poetry install --no-interaction --no-ansi --no-root --only main


RUN find /app/.venv -name "__pycache__" -o -name "*.py[co]" | xargs rm -rf

# --- ЭТАП 2: Финальный образ ---
FROM python:3.11-slim AS final

# Установка только тех системных пакетов, что нужны для работы
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg libsm6 libxext6 && \
    rm -rf /var/lib/apt/lists/*

# Создаем пользователя без прав root
RUN adduser --system --group --no-create-home appuser

WORKDIR /app

# Копируем УЖЕ УСТАНОВЛЕННЫЕ зависимости из предыдущего этапа
COPY --from=deps-installer /app/.venv ./.venv

# Используем pip из нашего виртуального окружения, чтобы установить streamlit
RUN ./.venv/bin/pip install streamlit aio-pika python-dotenv

# Копируем ВЕСЬ КОД нашего приложения.
# Этот слой будет пересобираться при любом изменении кода, но это очень быстрая операция.
COPY --chown=appuser:appuser . .

# Настраиваем переменные окружения
ENV POETRY_NO_INTERACTION=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# Переключение на пользователя без прав root (рекомендуется раскомментировать для продакшена)
# USER appuser

EXPOSE 8501

# Команда для запуска
CMD ["/app/.venv/bin/streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]