# syntax=docker/dockerfile:1
# Stage 1: Builder
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim as builder


ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

# 1. Copy Workspace Metadata
COPY pyproject.toml uv.lock ./
COPY common/pyproject.toml common/pyproject.toml
COPY services/backend/pyproject.toml services/backend/pyproject.toml
COPY services/editorial/pyproject.toml services/editorial/pyproject.toml
COPY database/pyproject.toml database/pyproject.toml

# 2. Copy Common Source
COPY common common

# 3. Install Editorial Dependencies
# --frozen: Use uv.lock
# --package editorial: Only install editorial + dependencies
# --no-dev: Exclude dev-dependencies
ENV UV_PROJECT_ENVIRONMENT="/app/.venv"
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package editorial --no-dev

# Stage 2: Runtime
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/services/editorial:/app" \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the venv from builder
COPY --from=builder /app/.venv /app/.venv

# Install Playwright Browsers & Deps
# Explicitly use the venv python
RUN  playwright install --with-deps chromium firefox \
    && rm -rf /var/lib/apt/lists/*

COPY services/editorial/app/scripts/download.py services/editorial/app/scripts/download.py
# Run download script (using venv python)
RUN python services/editorial/app/scripts/download.py
# Copy Source Code
COPY common common
COPY services/editorial/app services/editorial/app



WORKDIR /app/services/editorial
