FROM python:3.11-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 1. Copy Workspace Metadata
COPY pyproject.toml uv.lock ./
COPY common/pyproject.toml common/pyproject.toml
COPY services/backend/pyproject.toml services/backend/pyproject.toml
COPY services/editorial/pyproject.toml services/editorial/pyproject.toml
COPY database/pyproject.toml database/pyproject.toml

# 2. Copy Common Source (Required for workspace resolution)
COPY common common

# 3. Install Backend Dependencies
# --frozen: Use uv.lock
# --package backend: Only install backend + dependencies
# --no-dev: Exclude dev-dependencies
ENV UV_PROJECT_ENVIRONMENT="/app/.venv"
RUN uv sync --frozen --package backend --no-dev --no-install-project

# 4. Copy Backend Source
COPY services/backend/app services/backend/app

# 5. Environment
ENV PATH="/app/.venv/bin:$PATH"
# We add /app/services/backend to PYTHONPATH so "app.main" works as before
# We also add /app so "common" works (though common is installed as package, so might not be needed in PYTHONPATH if installed in venv)
ENV PYTHONPATH="/app/services/backend:/app"

WORKDIR /app/services/backend

ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
