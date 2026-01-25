# Tacitus Monorepo

Welcome to the Tacitus News monorepo.

## 🏗️ Structure

- `common/`: Shared library (`news_events_lib`) containing models and core logic.
- `database/`: Database migration management using Alembic.
- `services/backend/`: FastAPI backend service.
- `services/editorial/`: Python-based news analysis and aggregation workers.
- `services/frontend/`: Next.js frontend application.

## 💾 Database Migrations

Migrations are managed as a separate workspace package using **Alembic**.

### Commands

Run all commands from the project root:

```bash
# Check current migration version
uv run --package database alembic current

# Apply all migrations to latest
uv run --package database alembic upgrade head

# Create a new migration (autogenerate)
uv run --package database alembic revision --autogenerate -m "description_of_change"

# Rollback one migration
uv run --package database alembic downgrade -1
```

### Configuration
The migration settings are located in `database/alembic.ini`. It uses the `PG_DSN` environment variable to connect to the database.
