FROM python:3.11-slim AS builder

# Аргументы для прокси
ARG SHTTP_PROXY=""
ARG SHTTPS_PROXY=""

# Установка переменных окружения для прокси (если переданы)
ENV SHTTP_PROXY=${SHTTP_PROXY}
ENV SHTTPS_PROXY=${SHTTPS_PROXY}

RUN echo "SHTTP_PROXY=${SHTTP_PROXY}" && echo "SHTTPS_PROXY=${SHTTPS_PROXY}"

# Обновление и установка необходимых пакетов
RUN apt-get update && apt-get upgrade -y --no-install-recommends curl
RUN apt-get install ffmpeg libsm6 libxext6 -y

# Установка poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="$PATH:/root/.local/bin"

# Добавление poetry в PATH
COPY gpt_db/data/confs/pyproject.toml gpt_db/data/confs/poetry.lock README.md ./
COPY gpt_db/ ./gpt_db/

RUN poetry config virtualenvs.in-project true --local && \
    poetry lock && \
    poetry install --only main --no-interaction --no-ansi --no-cache

FROM python:3.11-slim

# Копирование файлов и зависимостей из builder-образа
COPY --from=builder /root/.local /root/.local
COPY --from=builder /.venv /.venv
COPY --from=builder gpt_db/ ./gpt_db/

# Аргументы для прокси
ARG SHTTP_PROXY=""
ARG SHTTPS_PROXY=""
ARG RMQ_AI_URL=""

# Установка переменных окружения для прокси (если переданы)
ENV SHTTP_PROXY=${SHTTP_PROXY}
ENV SHTTPS_PROXY=${SHTTPS_PROXY}
ENV RMQ_AI_URL=${RMQ_AI_URL}

# Настройка переменной окружения для python
ENV PATH="/.venv/bin:$PATH"

# Если необходимо, замените параметры в конфиге
RUN sed -i "s|RMQ_AI_URL|${RMQ_AI_URL}|g" gpt_db/data/confs/config.py

# Права доступа для папки с проектом
RUN chmod -R g+rw /gpt_db

# Запуск приложения
CMD ["python", "/gpt_db/main.py"]