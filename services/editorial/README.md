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

```mermaid
graph TD
    A[NewsGetter] -->|Raw Articles| B(Filter Queue)
    B --> C[NewsFilter]
    C -->|Approved IDs| D(Cluster Queue)
    D --> E[NewsCluster]
    E -->|High Confidence| F(Enhancer Queue)
    E -->|Ambiguous/Proposals| G[NewsReviewer]
    G -->|Auto-Match| F
    G -->|Still Ambiguous| H[CLI: Review Merges]
    H -->|Manual Resolve| F
    F --> I[NewsEnhancer]
    I -->|Draft Event| J[CLI: Publisher]
    J -->|Publish| K[Front End DB]
```

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
    * **Ambiguous:** Creates a MergeProposal and flags it for review.
* **Run:** `python news_cluster.py`

### 4. news_reviewer.py (The Auditor)
* **Role:** An automated worker that processes pending MergeProposals before they reach the human Editor.
* **Intelligence:** Uses a "Medium LLM" (Gemma-3-12B-IT) to perform Event Co-reference Resolution. It compares the candidate article against the target event to determine if they refer to the exact same real-world incident.
* **Logic:**
    * **High Confidence Match:** Auto-merges the article.
    * **High Confidence Mismatch:** Auto-rejects the proposal (triggers "New Event").
    * **Unsure:** Leaves the proposal for human review in the CLI.
* **Run:** `python news_reviewer.py`

## 🕹️ Part 2: The Human Loop (CLI)

These steps involve high-level analysis or human finalization.

### 5. news_enhancer.py (The Analyst)
* **Role:** Reads from ENHANCER queue. Writes the "Briefing Cards," Bias Analysis, and Stance Scoring.
* **Intelligence:** Uses a "Large LLM" (Gemma-3-27B-IT) to:
    * Summarize individual articles (bullets).
    * Calculate Stance (Float from -1.0 Critical to 1.0 Supportive).
    * Synthesize the "Ground News" style event summary (Left vs. Center vs. Right).
* **Run:** `python news_enhancer.py`

### 6. cli.py (The Control Room)
The central dashboard for the Editor.
* **[1] Review Merges:** Resolves "Ambiguous Clusters" that news_reviewer.py couldn't handle automatically.
* **[2] Queue Manager:** Retry failed jobs or inspect pipeline health.
* **[5] Find & Merge Duplicates:** Utility to clean up split events.
* **Run:** `python cli.py`

## 🛠️ Setup & Configuration

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
