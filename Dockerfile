# syntax=docker/dockerfile:1.4

# ЭТАП 1
FROM python:3.11-slim AS builder

# 1. Аргументы для прокси, которые будут использоваться во время сборки
ARG SHTTP_PROXY
ARG SHTTPS_PROXY

# 2. Настройка прокси для системных утилит (apt) и переменных окружения
# Это самый надежный способ заставить все инструменты (apt, curl, pip, poetry) работать через прокси.
RUN <<EOT
#!/bin/bash
set -e
if [ -n "$SHTTP_PROXY" ]; then
    echo "Acquire::http::Proxy \"${SHTTP_PROXY}\";" > /etc/apt/apt.conf.d/proxy.conf
    echo "Acquire::https::Proxy \"${SHTTPS_PROXY}\";" >> /etc/apt/apt.conf.d/proxy.conf
    export http_proxy="$SHTTP_PROXY"
    export https_proxy="$SHTTPS_PROXY"
fi
EOT

# 3. Установка системных пакетов, необходимых только для сборки
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 4. Установка Poetry
ENV POETRY_VERSION=2.1.3
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.in-project true && \
    poetry install --only main --no-interaction --no-ansi

COPY . .

FROM python:3.11-slim AS final

# 1. Аргументы для прокси и конфигурации, которые нужны во время запуска
ARG SHTTP_PROXY
ARG SHTTPS_PROXY

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    libsm6 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# 3. Создание пользователя без прав root для безопасного запуска
RUN adduser --system --group --no-create-home appuser

# 4. Копирование собранного проекта (код + .venv) из этапа "builder"
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    SHTTP_PROXY=${SHTTP_PROXY} \
    SHTTPS_PROXY=${SHTTPS_PROXY} 

# Переключение на пользователя без прав root
# USER appuser

# Открытие порта, на котором будет работать Streamlit
EXPOSE 8501

# 8. Команда для запуска приложения
# Запускаем main.py, который, в свою очередь, должен запускать streamlit
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]