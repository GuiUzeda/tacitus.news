# 📰 Tacitus Editorial Pipeline

"Semi-Autonomous Intelligence for the Tacitus.news."

This service is the heart of Tacitus.news. It is an ETL (Extract, Transform, Load) pipeline designed to ingest thousands of news articles, filter out noise, cluster them into "Events," and prepare them for human review.

## 🧠 Philosophy

The system is designed as a Cyborg Pipeline:

* **Automated Grunt Work:** AI handles the fetching, reading, filtering, clustering, and initial verification, and publication of news.
* **Human + AI Command:** Monitor the queues, events, publishing relevance, a other problems that may arise.

## Architecture

### Workers and domains

The editorial is comprised of a set of workers with their relative domains. Each of these workers do one of the process parts separately. Specific documentation on each can be found in the workers folder.

    - **Harverster:** Responsible for the **Extract** phase. It monitors RSS feeds, news APIs, and web sources to ingest raw articles and metadata into the system for downstream processing.
    - **Filter:** Responsible for pre filtering with a very light llm model the relevant articles harvested.
    - **Enricher:** Responsible to scrape and enrich each of the articles.
    - **Analyzer:** Responsible for the summaryzation and data extraction using llm model.
    - **Cluster:** Responsible to find matches between new articles and existing events
    - **Merger:** Responsible to scout the db and find matches between events and events.
    - **Reviewer:** Responsible to disambiguate possible matches between events and articles usins llm.
    - **Enhancer:** Responsible to summarize the data from all the articles into their parent event.
    -_**Publisher:** Resonsible to score and decide which events are going to be published.
    - **Gardener:** Routines.

### Queues

The system hasa message-driven architecture. Each worker listens to a specific queue, processes the incoming message, and then publishes a new message to the next queue in the sequence. This decoupled architecture allows each worker to scale independently, ensures fault tolerance, and provides clear visibility into the state of the system.

There are 3 separate queues domains and each of them have their queues:

    - **Articles Queues**
        -   Filer Queue
        -   Enricher Queue
        -   Analyzer Queue
        -   Cluster Queue
    - **Events Queues**
        -   Enhancer Queue
        -   Publisher Queue
    - **Merge Proposals** (Has no queue separation)

#### Queue logic

    Harvester creates Article
                            -> Filter Queue -> Enricher Queue
                                            -> Analyzer Queue (Only if already enriched)
                            -> Enricher Queue (Articles Without title come directly)
                                            -> Filter Queue (Only if Skipped Filter)
                                            -> Analyzer Queue
                                                             -> Cluster Queue
    Cluster create Event
                        -> Enhancer Queue -> Publisher Queue

    If the cluster or merger find a match that is ambiguous, they will send a Merge Proposal to the reviewer. The reviewer ALLWAYS send new or merged events to the enhancer!

    **EVERY TIME A EVENT IS MODIFIED IT NEEDS TO GO THROUGH ENHANCER AN THEN PUBLISHER**

## CLI

The Editorial CLI is a command-line interface for managing events, merges, and system maintenance. It shares the same service layer as the MCP server and the Dashboard.

Available command groups:
- `events`: List, show, boost, kill, and recalculate events.
- `merges`: Interactive or automated merge proposal review.
- `queue`: Monitor and manage processing queues (retry, reset, clear).
- `system`: Health checks and backfill utilities.

Usage:
```bash
python services/editorial/app/cli/cli.py --help
```

## MCP Server

The MCP (Model Context Protocol) server exposes the editorial tools to AI agents. It allows for automated remediation and querying of the system state using the same standardized services.

To start the server:
```bash
python services/editorial/app/mcp_server.py
```

## Dashboard (Experimental)

The system includes a Streamlit-based dashboard for real-time monitoring and editorial control.

> **Note:** This is a simple AI-generated unpublished version used for internal development and monitoring via VPN/Tailscale.

### Features
- **Control Room:** High-level metrics (Daily Volume, Heartbeat), system status, and AI usage vitality.
- **Live Desk:** Interactive table of published events with drill-down into analysis, topics, and linked articles. Includes manual actions like Boost, Kill, and Re-Enhance.
- **Queue Inspector:** Detailed investigation of Articles, Events, and Proposals with status filtering and pagination.
- **System Health:** Detailed source ingestion monitoring and database statistics.

To run the dashboard:
```bash
streamlit run services/editorial/app/dashboard.py
```

### TODO
- [ ] Polish dashboard UI and operational documentation.
- [ ] Implement Docker configurations for all services.
- [ ] Expand LLM parsing documentation.
