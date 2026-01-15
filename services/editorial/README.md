# 📰 Tacitus Editorial Pipeline

"Semi-Autonomous Intelligence for the Modern Editor."

This service is the heart of Tacitus.news. It is an ETL (Extract, Transform, Load) pipeline designed to ingest thousands of news articles, filter out noise, cluster them into "Events," and prepare them for human review.

## 🧠 Philosophy

The system is designed as a Cyborg Pipeline:

* **Automated Grunt Work:** AI handles the fetching, reading, filtering, clustering, and initial verification of news.
* **Human Command:** You (the Editor) use the CLI to resolve ambiguities and push the final "Publish" button.

*Goal: "The machine prepares the briefing; the human signs off on it."*

## 🏗️ Architecture

The pipeline moves data through a series of PostgreSQL Queues (articles_queue, events_queue).

### ⚙️ Database Automation (pg_cron)

Critical maintenance and scoring logic is offloaded to the database to ensure consistency and performance:

*   **Hot Score Decay (`update_hot_scores_job`):** Runs every 15 mins. Updates `hot_score` using the formula: `EditorialScore / (HoursElapsed + 2)^1.5`.
*   **The Archivist (`archivist_daily_sweep`):** Runs daily at 03:00 UTC. Archives events that are >7 days old (with low score) or >14 days old.
*   **Queue Janitor (`clean_editorial_queues_job`):** Runs daily at 03:00 UTC. Deletes completed/failed queue items older than 3 days.


## 🔁 Part 1: The Automated Loop (Cron Jobs & Workers)

These scripts are designed to run continuously or periodically to build up the backlog of intelligence.

### 1. news_getter.py (The Harvester)
* **Role:** Scans RSS feeds and Sitemaps defined in `data/feeds.json` to discover new article URLs.
* **Intelligence:**
    *   Deduplicates URLs against the database.
    *   Pushes new discoveries to the `ENRICH` queue.
* **Run:** `python news_getter.py`

### 2. workers/enricher.py (The Refiner)
* **Role:** Reads from `ENRICH` queue. The heavy lifter that turns a URL into structured intelligence.
* **Intelligence:**
    *   **Fetch:** Robust fetching with **Browser Fallback** (Playwright) for 403/429 blocks.
    *   **Extraction:** Cleans HTML to text using `trafilatura`.
    *   **Vectorization:** Generates embeddings (`nomic-embed-text-v1.5`) for semantic search.
    *   **NLP:** Extracts Entities (SpaCy).
    *   **LLM Analysis:** Uses **Tiered LLMs** (Intern/Senior) to generate Portuguese Summary, Key Points, Stance, and Clickbait scores per article.
* **Flow:** Routes to `FILTER` (if title was ambiguous) or `CLUSTER`.
* **Run:** `python workers/enricher.py`

### 3. news_filter.py (The Gatekeeper)
* **Role:** Reads from FILTER queue. Filters out noise (Sports, Gossip, Horoscopes).
* **Intelligence:** Uses "Intern" Tier LLM (e.g., Gemma-3-4B, Llama-3-8B) via `LLMRouter` to classify headlines in batches.
* **Run:** `python news_filter.py`

### 4. news_cluster.py (The Organizer)
* **Role:** Reads from CLUSTER queue. Groups related articles into Events.
* **Intelligence:** Uses Reciprocal Rank Fusion (RRF), combining:
    * Semantic Search (Vector Cosine Distance)
    * Keyword Search (Postgres TS_RANK)
* **Logic:**
    * **Match:** Merges into existing event immediately.
    * **New:** Creates a new event.
    * **Updates:** Aggregates `editorial_score`, `best_source_rank`, `main_topic_counts`, and Interest counts via `EventAggregator`.
    * **Ambiguous:** Creates a MergeProposal and flags it for review.
* **Run:** `python news_cluster.py`

### 5. workers/reviewer.py (The Auditor)
* **Role:** An automated worker that processes pending MergeProposals before they reach the human Editor.
* **Intelligence:** Uses "Mid" Tier LLM (via `LLMRouter`) to perform Event Co-reference Resolution. It compares the candidate article (or source event) against the target event to determine if they refer to the exact same real-world incident.
* **Logic:**
    * **High Confidence Match:** Auto-merges the article or event.
    * **High Confidence Mismatch:** Auto-rejects the proposal (triggers "New Event").
    * **Unsure:** Leaves the proposal for human review in the CLI.
* **Run:** `python workers/reviewer.py`

### 6. news_merger.py (The Deduplicator)
* **Role:** Scans active events to find "Split Brain" duplicates (events that should be merged but were separated).
* **Intelligence:** Hybrid Search (Vector + Keyword) comparing Event vs Event.
* **Logic:**
    * **Auto-Merge:** Distance < 0.05. Merges events immediately.
    * **Proposal:** Distance < 0.15 (or < 0.23 with strong keyword match). Creates an `event_merge` proposal.
* **Run:** `python news_merger.py`

### 7. workers/enhancer.py (The Analyst)
* **Role:** Reads from `ENHANCER` queue. Aggregates intelligence from articles into the Event.
* **Intelligence:**
    *   **Aggregation:** Updates Event-level stats (Bias, Stance Distribution, `ai_impact_score`) via `EventAggregator`.
    *   **Synthesis:** Uses "Senior" Tier LLM to generate the "Ground News" style event summary (Left/Center/Right).
    *   **Debounce:** Prevents re-summarizing active events too frequently.
* **Run:** `python workers/enhancer.py`

### 8. scripts/backfill_interests.py (The Entity Fixer)
* **Role:** A utility script to re-process existing articles and events to extract or update Named Entities (Interests) using SpaCy.
* **Intelligence:** Uses `pt_core_news_lg` to extract Person, Place, Org, and Topic entities.
* **Logic:**
    * **Phase 1:** Iterates through Events, re-analyzing linked articles and re-aggregating interest counts.
    * **Phase 2:** Scans orphan articles (not in events) to ensure they have searchable entities.
* **Run:** `python scripts/backfill_interests.py`

## 🕹️ Part 2: The Human Loop (CLI)

These steps involve high-level analysis or human finalization.

### 9. cli.py (The Control Room)
The central dashboard for the Editor.
* **[1] Review Merges:** Resolves "Ambiguous Clusters" (Article-Event or Event-Event).
* **[2] Queue Manager:** Retry failed jobs, reset stuck processing, or inspect pipeline health.
* **[3] Manual Search & Link:** Search for events and manually link an article or query.
* **[4] Inspect Tool:** Deep dive into an Event or Article by ID/Title.
* **[5] Find & Merge Duplicates:** Utility to clean up split events.
* **[6] Publishing Review:** Final sign-off for events in the `PUBLISHER` queue before they go live.
* **[8] Check for Blocking:** Inspects the Enricher queue for 403/429 errors to identify blocked sources.
* **Run:** `python cli.py`

## 📊 Key Metrics

*   **Editorial Score:** The raw importance score derived from source rankings and entity matches.
*   **Hot Score:** A dynamic, time-decayed score used to rank "Trending" events. Formula: `EditorialScore / (Age + 2)^1.5`.
*   **AI Impact Score:** An AI-generated assessment of the event's potential societal or industry impact.
*   **Category Tag:** High-level classification (e.g., "Politics", "Technology") assigned during enhancement.

## 🧠 AI Architecture (LLM Router)

The system uses a `LLMRouter` to dynamically select models based on task complexity and cost:

*   **Intern Tier (Fast/Cheap):** `gemma-3-4b-it`, `llama-3.1-8b-instant`. Used for Filtering and simple checks.
*   **Mid Tier (Balanced):** `gemma-3-12b-it`, `llama-3.3-70b`. Used for Co-reference resolution.
*   **Senior Tier (High Quality):** `gemma-3-27b-it`. Used for Summarization and complex Analysis.

Includes automatic **Retries**, **Rate Limit Handling**, and **Pydantic Validation** for structured JSON outputs.

## 🖥️ Monitoring

To visualize all workers running simultaneously in a grid layout, use the provided Tmux script located in the project root:

```bash
./monitor_tmux.sh
```

This will:
1. Create a `tmux` session named `tacitus_logs`.
2. Split the screen into a grid monitoring all workers + the backend.
3. Attach to the session (Press `Ctrl+B` then `d` to detach).

### 🎮 Tmux Navigation Cheatsheet

Once inside the grid, use the following shortcuts (prefix is `Ctrl+b` by default):

* **Navigate Panes:** Press `Ctrl+b` then `Arrow Keys` (Left, Right, Up, Down).
* **Scroll Mode:** Press `Ctrl+b` then `[` (Use arrow keys or PageUp/Down to scroll). Press `q` to exit scroll mode.
* **Detach (Keep running):** Press `Ctrl+b` then `d`.
* **Re-attach:** Run `tmux attach -t tacitus_logs`.
* **Kill Session:** Run `tmux kill-session -t tacitus_logs`.

## �️ Setup & Configuration

### Environment Variables:
Ensure .env contains:
```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/tacitus
GEMINI_API_KEY=xyz...
```

### Feeds:
Edit data/feeds.json to add newspapers/sources. The feeds_seeder.py script loads these into the DB.

### Local Models:
The system requires local NLP models (SpaCy) and remote LLM access (Gemini):
```bash
pip install spacy
python -m spacy download pt_core_news_lg
```
