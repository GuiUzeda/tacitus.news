# syntax=docker/dockerfile:1
# Stage 1: Builder
FROM python:3.11-slim-bookworm as builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY common /app/common
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e /app/common

COPY services/editorial/requirements.txt /app/services/editorial/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install torch --index-url https://download.pytorch.org/whl/cpu

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/services/editorial/requirements.txt

RUN python -m spacy download pt_core_news_lg

# Stage 2: Runtime
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/services/editorial \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

# Install Playwright Browsers & Deps
RUN playwright install --with-deps chromium firefox \
    && rm -rf /var/lib/apt/lists/*

COPY services/editorial/scripts/download.py /app/services/editorial/scripts/download.py
RUN python services/editorial/scripts/download.py

# Copy Codebase
COPY common /app/common
COPY services/editorial /app/services/editorial
