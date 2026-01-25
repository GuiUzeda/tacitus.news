import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Literal, Optional, Type, TypeVar

from app.config import Settings
from app.services.analytics_service import AnalyticsService
from app.services.article_service import ArticleService
from app.services.audit_service import AuditService
from app.services.backfill import BackfillService
from app.services.event_service import EventService
from app.services.filters.filters import (
    ArticleFilter,
    ArticleQueueFilter,
    AuditFilter,
    EventFilter,
    EventQueueFilter,
    NewspaperFilter,
    ProposalQueueFilter,
)
from app.services.health_service import HealthService
from app.services.maintenance import MaintenanceService
from app.services.newspaper_service import NewspaperService
from app.services.queue_service import QueueService
from app.services.rate_limit_service import RateLimitService
from app.services.review_service import ReviewService
from fastmcp import FastMCP
from news_events_lib.audit import receive_after_flush  # noqa: F401
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

settings = Settings()
engine = create_engine(str(settings.pg_dsn))
print(f"Connecting to {settings.pg_dsn}")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

mcp = FastMCP("Tacitus Editorial")

T = TypeVar("T")


@contextmanager
def get_service(service_class: Type[T]):
    """Context manager for service injection with DB session handling."""
    with SessionLocal() as session:
        yield service_class(session)


@mcp.tool
def execute_sql(query: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Executes raw SQL queries against the database.
    Use this for complex analysis, reporting, or ad-hoc data fixes.
    All SELECT queries return results. UPDATE/DELETE/INSERT queries return rowcount and require explicit approval.
    """
    with SessionLocal() as session:
        # Use text() for safe SQL execution
        stmt = text(query)
        result = session.execute(stmt, params or {})

        if query.strip().upper().startswith("SELECT"):
            # Handle results
            rows = result.mappings().all()
            serialized_rows = []
            for row in rows:
                d = dict(row)
                for k, v in d.items():
                    if hasattr(v, "isoformat"):
                        d[k] = v.isoformat()
                    elif isinstance(v, uuid.UUID):
                        d[k] = str(v)
                serialized_rows.append(d)
            return serialized_rows
        else:
            session.commit()
            return {"status": "success", "rowcount": result.rowcount}


@mcp.tool
def check_db_connection() -> Dict[str, str]:
    """
    Checks the connection status and connection string
    """
    status = {"status": "ok", "connection": str(settings.pg_dsn)}
    try:
        engine.connect()
    except Exception as e:
        status["status"] = "error"
        status["message"] = str(e)
    return status


@mcp.tool
def queue_overview(
    queue_filter: (
        Literal["all"] | Literal["articles"] | Literal["events"] | Literal["proposals"]
    ) = "all",
) -> Dict[str, Any]:
    """
    Summarizes states and Queue stats across all domains.
    """
    with get_service(AuditService) as audit_service:
        return audit_service.get_queue_overview(queue_filter)


@mcp.tool
def list_article_queue(
    limit: int = 20, offset: int = 0, filters: Optional[ArticleQueueFilter] = None
):
    """
    Explore items in the Article processing queue.
    """
    with get_service(ArticleService) as article_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return article_service.list_queue(filter_dict, limit, offset)


@mcp.tool
def list_event_queue(
    limit: int = 20, offset: int = 0, filters: Optional[EventQueueFilter] = None
):
    """
    Explore items in the Event processing queue.
    """
    with get_service(EventService) as event_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return event_service.list_queue(filter_dict, limit, offset)


@mcp.tool
def list_proposals(
    limit: int = 20, offset: int = 0, filters: Optional[ProposalQueueFilter] = None
):
    """
    Explore merge proposals.
    """
    with get_service(EventService) as event_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return event_service.list_proposals(filter_dict, limit, offset)


@mcp.tool
def search_articles(
    limit: int = 20, offset: int = 0, filters: Optional[ArticleFilter] = None
):
    """
    Search for articles in the database.
    """
    with get_service(ArticleService) as article_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return article_service.search(filter_dict, limit, offset)


@mcp.tool
def search_events(
    limit: int = 20, offset: int = 0, filters: Optional[EventFilter] = None
):
    """
    Search for events in the database.
    """
    with get_service(EventService) as event_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return event_service.search(filter_dict, limit, offset)


@mcp.tool
def update_event(
    event_id: str, title: Optional[str] = None, subtitle: Optional[str] = None
) -> Dict[str, Any]:
    """
    Updates basic event fields like title and subtitle.
    """
    with get_service(EventService) as event_service:
        return event_service.update_event(event_id, title, subtitle)


@mcp.tool
def search_newspapers(
    limit: int = 20, offset: int = 0, filters: Optional[NewspaperFilter] = None
):
    """
    Search for newspapers/sources in the database.
    """
    with get_service(NewspaperService) as newspaper_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return newspaper_service.search(filter_dict, limit, offset)


@mcp.tool
def search_audit_logs(
    limit: int = 20, offset: int = 0, filters: Optional[AuditFilter] = None
):
    """
    Search system audit logs.
    """
    with get_service(AuditService) as audit_service:
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        return audit_service.search_logs(filter_dict, limit, offset)


@mcp.tool
def recalculate_all_event_metrics(only_active: bool = True) -> Dict[str, Any]:
    """
    Force a full recalculation of all aggregate metrics (Stance, Bias, Centroids) for events.
    Use this if you suspect data corruption or after significant algorithm updates.
    """
    with get_service(EventService) as event_service:
        return event_service.recalculate_all_events(only_active)


@mcp.tool
def recalculate_event_metrics(event_id: str) -> str:
    """
    Force a full recalculation of all aggregate metrics (Stance, Bias, Centroids) for a single event.
    """
    with get_service(EventService) as event_service:
        return event_service.recalculate_event_metrics(event_id)


@mcp.tool
def refresh_all_rankings() -> Dict[str, Any]:
    """
    Recalculates hot_scores and insights (Blind Spots, Controversy) for all published events.
    Use this to refresh the front-page rankings.
    """
    with get_service(EventService) as event_service:
        return event_service.refresh_all_rankings()


@mcp.tool
def get_article_bias_distribution() -> List[Dict[str, Any]]:
    """
    Get the global distribution of articles by bias (e.g., center-left, right, etc.).
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_article_bias_distribution()


@mcp.tool
def get_source_volume_stats(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get the top news sources by number of articles.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_source_volume_stats(limit)


@mcp.tool
def get_global_news_stats() -> Dict[str, Any]:
    """
    Get high-level statistics about total articles and events.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_global_event_stats()


@mcp.tool
def get_last_ingestion_time() -> Dict[str, Any]:
    """
    Returns the timestamp of the most recently created article and status (healthy/warning/critical).
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_last_ingestion_time()


@mcp.tool
def get_blind_spot_stats() -> Dict[str, Any]:
    """
    Returns counts of active blind spots (events ignored by one side).
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_blind_spot_stats()


@mcp.tool
def get_stance_distribution() -> List[Dict[str, Any]]:
    """
    Returns stance distribution histogram.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_stance_distribution()


@mcp.tool
def get_clickbait_distribution() -> List[Dict[str, Any]]:
    """
    Returns clickbait score distribution histogram.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_clickbait_distribution()


@mcp.tool
def get_queue_volume_history(queue_type: str, hours: int = 24) -> List[Dict[str, Any]]:
    """
    Returns volume history (CREATED vs terminal statuses) for a queue type.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_queue_volume_history(queue_type, hours)


@mcp.tool
def get_rate_limit_status() -> List[Dict[str, Any]]:
    """
    Get the current usage and limits for all LLM models.
    """
    with get_service(RateLimitService) as rate_service:
        return rate_service.get_status()


@mcp.tool
def requeue_all_stale_items(minutes_stale: int = 10) -> Dict[str, Any]:
    """
    Identifies and re-queues items (Articles, Events, Proposals) that have been stuck
    in 'PROCESSING' for longer than the specified minutes.
    """
    with get_service(QueueService) as queue_service:
        return queue_service.requeue_stale_items(minutes_stale)


@mcp.tool
def clear_completed_queue_items() -> Dict[str, Any]:
    """
    Deletes ALL completed, approved, or rejected items from all queues.
    Use this to free up database space and reduce noise.
    """
    with get_service(MaintenanceService) as maintenance_service:
        return maintenance_service.clear_completed_queues()


@mcp.tool
def backfill_missing_topics(limit: int = 50) -> str:
    """
    Backfills 'main_topic_counts' for events where it is missing.
    """
    with get_service(BackfillService) as backfill_service:
        return backfill_service.backfill_missing_topics_batch(limit)


@mcp.tool
def backfill_bias_distribution(limit: int = 50) -> str:
    """
    Backfills 'bias_distribution' for events where it is missing.
    """
    with get_service(BackfillService) as backfill_service:
        return backfill_service.backfill_bias_distribution_batch(limit)


@mcp.tool
def backfill_interests(limit: int = 20) -> str:
    """
    Updates article entities and event interest counts using spaCy.
    """
    with get_service(BackfillService) as backfill_service:
        return backfill_service.backfill_interests_batch(limit)


@mcp.tool
def backfill_stance(limit: int = 50) -> str:
    """
    Recalculates stance distribution for events.
    """
    with get_service(BackfillService) as backfill_service:
        return backfill_service.backfill_stance_batch(limit)


@mcp.tool
def requeue_incomplete_events() -> int:
    """
    Resets events that finished enhancement but still lack a subtitle or summary.
    """
    with get_service(QueueService) as queue_service:
        return queue_service.requeue_incomplete_events()


# --- New Tools for CLI/MCP Parity ---


@mcp.tool
def get_top_topics(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get the top trending topics across all active events.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_top_topics(limit)


@mcp.tool
def get_ingestion_stats_by_source(days: int = 7) -> List[Dict[str, Any]]:
    """
    Get daily article ingestion counts grouped by source.
    """
    with get_service(AnalyticsService) as analytics_service:
        return analytics_service.get_ingestion_stats_by_source(days)


@mcp.tool
def search_events_hybrid(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Semantic search for events using hybrid (keyword + vector) scoring.
    """
    with get_service(EventService) as event_service:
        return event_service.search_events_hybrid(query, limit)


@mcp.tool
def get_top_events(
    limit: int = 50,
    offset: int = 0,
    min_score: Optional[float] = None,
    topic: Optional[str] = None,
    only_blind_spots: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get top published events by hot_score. (Live Desk)
    """
    with get_service(EventService) as event_service:
        return event_service.get_top_events(
            limit, offset, min_score, topic, only_blind_spots
        )


@mcp.tool
def get_article_details(article_id: str) -> Optional[Dict[str, Any]]:
    """
    Get full details (including content) of an article.
    """
    with get_service(ArticleService) as article_service:
        return article_service.get_article_details(article_id)


@mcp.tool
def get_waiting_events() -> List[Dict[str, Any]]:
    """
    Get events waiting for approval.
    """
    with get_service(EventService) as event_service:
        return event_service.get_waiting_events()


@mcp.tool
def boost_event(event_id: str, amount: int = 10) -> str:
    """
    Boosts an event's editorial score.
    """
    with get_service(EventService) as event_service:
        return event_service.boost_event(event_id, amount)


@mcp.tool
def archive_event(event_id: str) -> str:
    """
    Archives (kills) an event.
    """
    with get_service(EventService) as event_service:
        return event_service.archive_event(event_id)


@mcp.tool
def dissolve_event(event_id: str) -> str:
    """
    Dissolves an event, unlinking its articles and re-queueing them for enrichment.
    """
    with get_service(EventService) as event_service:
        return event_service.dissolve_event(event_id)


@mcp.tool
def requeue_articles_for_enrichment(event_id: str) -> str:
    """
    Forces all articles for an event back to the Enrichment queue.
    """
    with get_service(EventService) as event_service:
        return event_service.requeue_articles_for_enrichment(event_id)


@mcp.tool
def requeue_event_for_enhancement(event_id: str) -> str:
    """
    Re-queues an Event for Enhancement (Summary generation).
    """
    with get_service(EventService) as event_service:
        return event_service.requeue_event_for_enhancement(event_id)


@mcp.tool
def requeue_article_for_analysis(article_id: str) -> str:
    """
    Re-queues a single Article for Enrichment/Analysis.
    """
    with get_service(ArticleService) as article_service:
        return article_service.requeue_article_for_analysis(article_id)


@mcp.tool
def approve_event(event_id: str) -> str:
    """
    Approves a waiting event, publishing it.
    """
    with get_service(EventService) as event_service:
        return event_service.approve_event(event_id)


@mcp.tool
def get_next_merge_proposal(type: str = "article") -> Dict[str, Any]:
    """
    Gets the next pending merge proposal. Type: 'article' or 'event'.
    """
    with get_service(ReviewService) as review_service:
        prop = review_service.get_next_merge_proposal(type)
        if not prop:
            return {}
        # Simple serialization
        return {
            "id": str(prop.id),
            "distance_score": prop.distance_score,
            "reasoning": prop.reasoning,
            "source_title": (
                prop.source_article.title
                if prop.source_article
                else (prop.source_event.title if prop.source_event else "Unknown")
            ),
            "target_title": prop.target_event.title if prop.target_event else "Unknown",
        }


@mcp.tool
def process_merge_proposal(
    proposal_id: str, action: Literal["merge", "reject", "new_event"]
) -> str:
    """
    Process a merge proposal. Action: 'merge', 'reject', 'new_event'.
    """
    with get_service(ReviewService) as review_service:
        return review_service.process_merge_proposal(proposal_id, action)


@mcp.tool
async def refetch_article(article_id: str) -> str:
    """
    Refetches article HTML and updates date/score.
    """
    # Note: FastMCP handles async
    with get_service(ArticleService) as article_service:
        return await article_service.refetch_article(article_id)


@mcp.tool
def check_system_blocks() -> str:
    """
    Checks for HTTP 403/429 blocking signatures in queues.
    """
    with get_service(HealthService) as health_service:
        return health_service.check_blocks()


if __name__ == "__main__":
    mcp.run()
