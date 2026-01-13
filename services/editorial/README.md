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



## 🔁 Part 1: The Automated Loop (Cron Jobs)

These scripts are designed to run continuously or periodically to build up the backlog of intelligence.

### 1. news_getter.py (The Harvester)
* **Role:** Fetches raw HTML from RSS feeds and Sitemaps defined in data/feeds.json. Uses a HarvesterFactory to handle site-specific scraping logic (e.g., CNN, Band, Poder360).
* **Intelligence:**
    * Extracts clean text using trafilatura.
    * Vectorizes text immediately using nomic-embed-text-v1.5.
    * Extracts Interests (Entities) using SpaCy (pt_core_news_lg).
* **Run:** `python news_getter.py`

### 2. news_filter.py (The Gatekeeper)
* **Role:** Reads from FILTER queue. Filters out noise (Sports, Gossip, Horoscopes).
* **Intelligence:** Uses a "Small LLM" (Gemma-3-4B-IT) to classify headlines in batches of 50.
* **Run:** `python news_filter.py`

### 3. news_cluster.py (The Organizer)
* **Role:** Reads from CLUSTER queue. Groups related articles into Events.
* **Intelligence:** Uses Reciprocal Rank Fusion (RRF), combining:
    * Semantic Search (Vector Cosine Distance)
    * Keyword Search (Postgres TS_RANK)
* **Logic:**
    * **Match:** Merges into existing event immediately.
    * **New:** Creates a new event.
    * **Updates:** Aggregates `editorial_score`, `best_source_rank`, and Interest counts via `EventAggregator`.
    * **Ambiguous:** Creates a MergeProposal and flags it for review.
* **Run:** `python news_cluster.py`

### 4. workers/reviewer.py (The Auditor)
* **Role:** An automated worker that processes pending MergeProposals before they reach the human Editor.
* **Intelligence:** Uses a "Medium LLM" (via `CloudNewsAnalyzer`) to perform Event Co-reference Resolution. It compares the candidate article (or source event) against the target event to determine if they refer to the exact same real-world incident.
* **Logic:**
    * **High Confidence Match:** Auto-merges the article or event.
    * **High Confidence Mismatch:** Auto-rejects the proposal (triggers "New Event").
    * **Unsure:** Leaves the proposal for human review in the CLI.
* **Run:** `python workers/reviewer.py`

### 5. news_merger.py (The Deduplicator)
* **Role:** Scans active events to find "Split Brain" duplicates (events that should be merged but were separated).
* **Intelligence:** Hybrid Search (Vector + Keyword) comparing Event vs Event.
* **Logic:**
    * **Auto-Merge:** Distance < 0.05. Merges events immediately.
    * **Proposal:** Distance < 0.15 (or < 0.23 with strong keyword match). Creates an `event_merge` proposal.
* **Run:** `python news_merger.py`

### 6. scripts/backfill_interests.py (The Entity Fixer)
* **Role:** A utility script to re-process existing articles and events to extract or update Named Entities (Interests) using SpaCy.
* **Intelligence:** Uses `pt_core_news_lg` to extract Person, Place, Org, and Topic entities.
* **Logic:**
    * **Phase 1:** Iterates through Events, re-analyzing linked articles and re-aggregating interest counts.
    * **Phase 2:** Scans orphan articles (not in events) to ensure they have searchable entities.
* **Run:** `python scripts/backfill_interests.py`

## 🕹️ Part 2: The Human Loop (CLI)

These steps involve high-level analysis or human finalization.

### 7. workers/enhancer.py (The Analyst)
* **Role:** Reads from ENHANCER queue. Aggregates intelligence and prepares the event for publication.
* **Intelligence:**
    * **Batch Processing:** Analyzes new articles in batches to extract Entities, Stance, and Clickbait scores.
    * **Aggregation:** Updates Event-level stats (Bias Distribution, Interest Counts) via `EventAggregator`.
    * **Summarization:** Generates/Updates the "Ground News" style summary (Left/Center/Right).
    * **Flow:** Moves completed events to the `PUBLISHER` queue.
* **Run:** `python workers/enhancer.py`

### 8. cli.py (The Control Room)
The central dashboard for the Editor.
* **[1] Review Merges:** Resolves "Ambiguous Clusters" (Article-Event or Event-Event).
* **[2] Queue Manager:** Retry failed jobs, reset stuck processing, or inspect pipeline health.
* **[3] Manual Search & Link:** Search for events and manually link an article or query.
* **[4] Inspect Tool:** Deep dive into an Event or Article by ID/Title.
* **[5] Find & Merge Duplicates:** Utility to clean up split events.
* **[6] Publishing Review:** Final sign-off for events in the `PUBLISHER` queue before they go live.
* **Run:** `python cli.py`

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
