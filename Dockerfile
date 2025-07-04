# syntax=docker/dockerfile:1.4

########################
# ЭТАП 1 – deps-cache  #
########################
FROM python:3.11-slim AS deps

# ---- системные утилиты + Poetry -------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl && \
    pip install --no-cache-dir "poetry==2.1.3" && \
    rm -rf /var/lib/apt/lists/*

# ---- копируем только манифесты и ставим ТОЛЬКО зависимости ----------------
WORKDIR /app
COPY pyproject.toml poetry.lock ./

# виртуальное окружение внутри проекта, dev-группы не нужны
RUN poetry config virtualenvs.in-project true && \
    poetry install --no-interaction --no-ansi --only main --no-root

########################
# ЭТАП 2 – builder     #
########################
FROM deps AS builder

# ---- копируем исходный код (и README) -------------------------------------
COPY gpt_db ./gpt_db
COPY README.md .

# ---- ставим сам проект в то же .venv --------------------------------------
RUN poetry install --no-interaction --no-ansi --only-root

COPY main.py .

# по желанию: лёгкая чистка мусора, но без strip *.so
RUN find /app/.venv -name '__pycache__' -o -name '*.py[co]' | xargs rm -rf

########################
# ЭТАП 3 – runtime     #
########################
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg libsm6 libxext6 && \
    rm -rf /var/lib/apt/lists/*

# необязательно, но полезно для безопасности
RUN adduser --system --group --no-create-home appuser
WORKDIR /app

# ---- переносим готовое окружение + приложение -----------------------------
COPY --from=builder /app ./

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1

# USER appuser        # раскомментируйте в продакшене
#EXPOSE 8501
COPY rpc.py .
COPY agent_ver2.py .
COPY promts.py .
#CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
CMD ["python", "rpc.py"]