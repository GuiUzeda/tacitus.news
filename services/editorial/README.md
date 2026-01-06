# **📰 Tacitus Editorial Pipeline**

**"Semi-Autonomous Intelligence for the Modern Editor."**

This service is the heart of Tacitus.news. It is an ETL (Extract, Transform, Load) pipeline designed to ingest thousands of news articles, filter out noise, cluster them into "Events," and prepare them for human review.

## **🧠 Philosophy**

The system is designed as a **Cyborg Pipeline**:

1. **Automated Grunt Work:** AI handles the fetching, reading, filtering, and initial clustering of news.  
2. **Human Command:** You (the Editor) use the CLI to resolve ambiguities and push the final "Publish" button.

*Goal: "The machine prepares the briefing; the human signs off on it."*

## **🏗️ Architecture**

The pipeline moves data through a series of **PostgreSQL Queues** (articles\_queue, events\_queue).

```mermaid
graph TD  
    A\[NewsGetter\] \--\>|Raw Articles| B(Filter Queue)  
    B \--\> C\[NewsFilter\]  
    C \--\>|Approved IDs| D(Cluster Queue)  
    D \--\> E\[NewsCluster\]  
    E \--\>|High Confidence| F(Enhancer Queue)  
    E \--\>|Ambiguous| G\[CLI: Review Merges\]  
    G \--\>|Manual Resolve| F  
    F \--\> H\[NewsEnhancer\]  
    H \--\>|Draft Event| I\[CLI: Publisher\]  
    I \--\>|Publish| J\[Front End DB\]
```

## **🔁 Part 1: The Automated Loop (Cron Jobs)**

These scripts are designed to run periodically (e.g., every hour) to build up the backlog of intelligence.

### **1\. news\_getter.py (The Harvester)**

* **Role:** Fetches raw HTML from RSS feeds and Sitemaps defined in data/feeds.json.  
* **Intelligence:**  
  * Extracts clean text using trafilatura.  
  * **Vectorizes** text immediately using nomic-embed-text-v1.5.  
  * **Extracts Entities** (PER, ORG) using SpaCy.  
* **Run:** `python news_getter.py`

### **2\. news\_filter.py (The Gatekeeper)**

* **Role:** Reads from FILTER queue. Filters out noise (Sports, Gossip, Horoscopes).  
* **Intelligence:** Uses a "Small LLM" (**Gemma-3-4B**) to classify headlines in batches of 50\.  
* **Run:** python news\_filter.py

### **3\. news\_cluster.py (The Organizer)**

* **Role:** Reads from CLUSTER queue. Groups related articles into **Events**.  
* **Intelligence:** Uses **Reciprocal Rank Fusion (RRF)**, combining:  
  * *Semantic Search* (Vector Cosine Distance)  
  * *Keyword Search* (Postgres TS\_RANK)  
* **Logic:**  
  * **Match:** Merges into existing event.  
  * **New:** Creates a new event.  
  * **Ambiguous:** Creates a MergeProposal for human review (see CLI).  
* **Run:** `python news_cluster.py`

## **🕹️ Part 2: The Human Loop (CLI)**

These steps are manual. You use the CLI to unblock the pipeline and perform final quality control.

### **4\. news\_enhancer.py (The Analyst)**

* **Role:** Reads from ENHANCER queue. Writes the "Briefing Cards" and Bias Analysis.  
* **Intelligence:** Uses a "Large LLM" (**Gemma-3-27B**) to:  
  * Summarize individual articles (bullets, stance).  
  * Synthesize the **"Ground News"** style comparison (Left vs. Center vs. Right).  
* **Note:** While this script *can* run automatically, it consumes expensive API tokens, so it is often triggered manually or on a slower cron.  
* **Run:** python news\_enhancer.py

### **5\. cli.py (The Control Room)**

The central dashboard for the Editor.

* **\[1\] Review Merges:**  
  * Resolves "Ambiguous Clusters" flagged by news\_cluster.py.  
  * *Example:* "Is 'Lula visits China' the same event as 'Lula signs bilateral treaty'?" \-\> You decide.  
* **\[6\] Publisher Dashboard:**  
  * Review "Enhanced" events that are ready to go live.  
  * **Actions:** \[P\] Publish, \[E\] Edit Title, \[K\] Kill/Archive.  
* **Run:** `python cli.py`

## **🛠️ Setup & Configuration**

1. Environment Variables:  
   Ensure .env contains:  

    ```bash
    DATABASE\_URL=postgresql://user:pass@localhost:5432/tacitus  
    GEMINI\_API\_KEY=xyz...
    ```

2. Feeds:  
   Edit data/feeds.json to add newspapers/sources. The feeds\_seeder.py script loads these into the DB.  
3. Local Models:  
   The system requires \~2GB RAM for local NLP models:  

    ```bash
    pip install spacy  
    python \-m spacy download pt\_core\_news\_lg  
    ```
