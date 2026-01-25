# Contributing to Tacitus.news

Welcome to the Tacitus Intelligence Engine. This project follows a **Senior Engineering Workflow** designed for stability, observability, and a clean linear history.

Please follow these guidelines to maintain the integrity of the codebase.

## ⚡ Quick Start (Development)

This project uses the **Docker Compose Override Pattern**. You do not need to manually configure a development environment.

1. **Clone the repo:**

    ```bash
    git clone https://github.com/guiuzeda/tacitus.news.git
    cd tacitus.news
    ```

2. **Start the Stack:**

Docker automatically loads `docker-compose.override.yaml` for local development (enabling hot-reloading and exposing ports).

    ```bash
        docker compose up -d
    ```

3. **Access Services:**

* **Frontend:** `http://localhost:3000`
* **Backend API:** `http://localhost:8000/docs`
* **Log Dashboard (Dozzle):** `http://localhost:8080`
* **Database:** Exposed on `localhost:5432`

---

## 🌳 Git Workflow: Trunk-Based Development

We strictly enforce a **Linear History** on `main`. Do not push directly to `main`.

### 1. Branching Strategy

* **Source of Truth:** `main` (Always deployable).
* **Feature Branches:** Create short-lived branches for every task.
* `feat/heartbeat-mechanism`
* `fix/rss-parsing-error`
* `chore/update-dependencies`

### 2. Commit Strategy (Local)

While working on your branch, commit as often as you like ("wip", "saving work"). These messy commits will be squashed later.

### 3. Merging (The "Squash" Ritual)

We use **Squash Merges** to keep the `main` history clean.

* **Option A (GitHub UI):** Open a Pull Request and select "Squash and Merge".
* **Option B (CLI):**

```bash
git checkout main
git merge --squash feat/my-feature
git commit -m "feat(worker): add heartbeat mechanism to base class"
git branch -D feat/my-feature
git push origin main

```

---

## 📝 Conventional Commits

All merge commits to `main` **must** follow the [Conventional Commits](https://www.conventionalcommits.org/) specification. This drives our automated changelogs and semantic versioning.

**Format:** `<type>(<scope>): <description>`

| Type | Description | Example |
| --- | --- | --- |
| `feat` | A new feature | `feat(harvester): add support for Metropoles RSS` |
| `fix` | A bug fix | `fix(api): handle null dates in article schema` |
| `chore` | Maintenance/Config | `chore(docker): update python base image to 3.12` |
| `refactor` | Code restructuring | `refactor(core): decouple rate limiter from session` |
| `docs` | Documentation only | `docs: update CONTRIBUTING.md` |
| `perf` | Performance improvements | `perf(db): add index to published_date column` |

---

## 🛠️ Tooling & Quality Checks

### Pre-Commit Hooks

We use `pre-commit` to enforce formatting and linting before code enters the repo.

```bash
pip install pre-commit
pre-commit install

```

* **Python:** Black, Isort, Flake8.
* **Secrets:** Detect-Secrets (prevents API keys from being committed).

### Hot Reloading Strategy

To prevent "Reload Storms" (where changing one file restarts all containers), we use `watchmedo` with targeted patterns.

* **Core Logic:** Changes to `app/core` restart all workers.
* **Worker Logic:** Changes to `app/workers/enhancer.py` **only** restart the Enhancer container.

If you add a new worker, update the `command` section in `docker-compose.override.yaml` to exclude it from neighbor watches.

---

## 🚀 Releases

We do not use release branches. We use **Git Tags** on `main`.

To cut a release:

```bash
git tag -a v1.0.0 -m "MVP Release: Triad Model"
git push origin v1.0.0

```

This tag marks the specific commit running in production.
