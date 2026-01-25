import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.services.base_service import BaseService
from app.services.queue_service import QueueService
from app.utils.event_manager import EventManager
from app.workers.publisher.domain import NewsPublisherDomain
from news_events_lib.models import (
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    EventsQueueName,
    EventStatus,
    JobStatus,
    MergeProposalModel,
    NewsEventModel,
)
from sqlalchemy import delete, desc, select
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


class EventService(BaseService, QueueService):
    """
    Service for the Event domain.
    Domain Model: NewsEventModel
    Queue Model: EventsQueueModel
    """

    def search(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(NewsEventModel, filters, limit, offset)

    def list_queue(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        if "queue_name" in filters and isinstance(filters["queue_name"], str):
            for q in EventsQueueName:
                if q.value.lower() == filters["queue_name"].lower():
                    filters["queue_name"] = q
                    break
        return self._list_queue(EventsQueueModel, filters, limit, offset)

    def list_proposals(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(MergeProposalModel, filters, limit, offset)

    def retry_queue(self, queue_name: Optional[str] = None) -> int:
        q_enum = None
        if queue_name:
            for q in EventsQueueName:
                if q.value.lower() == queue_name.lower():
                    q_enum = q
                    break
        return self._retry_failed(EventsQueueModel, q_enum)

    def recalculate_all_events(self, only_active: bool = True) -> Dict[str, Any]:
        """
        Force a full recalculation of all aggregate metrics for events in batches.
        """
        stmt = select(NewsEventModel)
        if only_active:
            stmt = stmt.where(NewsEventModel.is_active)

        events = self.session.scalars(stmt).all()
        count = 0
        batch_size = 50

        for event in events:
            try:
                EventManager.recalculate_event_metrics(
                    self.session, event, commit=False
                )
                count += 1

                if count % batch_size == 0:
                    self.session.commit()
                    logger.info(f"Recalculated {count} events...")
            except Exception as e:
                self.session.rollback()
                logger.error(f"Error recalculating event {event.id}: {e}")
                continue

        self.session.commit()
        return {"status": "success", "recalculated_count": count}

    def recalculate_event_metrics(self, event_id: str) -> str:
        """
        Force a full recalculation of aggregate metrics for a single event.
        """
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        try:
            EventManager.recalculate_event_metrics(self.session, event, commit=True)
            return f"✅ Recalculated metrics for event: {event.title}"
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error recalculating event {event.id}: {e}")
            return f"❌ Error: {str(e)}"

    def update_event(
        self, event_id: str, title: Optional[str] = None, subtitle: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Updates basic event fields (title, subtitle).
        """
        # Handle UUID or str
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return {"status": "error", "message": "Invalid UUID format"}

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return {"status": "error", "message": f"Event {event_id} not found"}

        if title is not None:
            event.title = title
        if subtitle is not None:
            event.subtitle = subtitle

        self.session.commit()
        return {"status": "success", "event_id": str(event.id)}

    def refresh_all_rankings(self) -> Dict[str, Any]:
        """
        Recalculates hot_scores and insights in batches.
        """
        publisher = NewsPublisherDomain()
        stmt = select(NewsEventModel).where(
            NewsEventModel.status == EventStatus.PUBLISHED,
            NewsEventModel.is_active,
        )

        events = self.session.scalars(stmt).all()
        count = 0
        batch_size = 50

        for event in events:
            try:
                topics = (
                    list(event.main_topic_counts.keys())
                    if event.main_topic_counts
                    else []
                )
                score, insights = publisher.calculate_spectrum_score(event, topics)
                event.hot_score = score
                event.publisher_insights = insights
                count += 1

                if count % batch_size == 0:
                    self.session.commit()
            except Exception as e:
                self.session.rollback()
                logger.error(f"Error refreshing ranking for event {event.id}: {e}")
                continue

        self.session.commit()
        return {"status": "success", "refreshed_count": count}

    def get_top_events(
        self,
        limit: int = 50,
        offset: int = 0,
        min_score: Optional[float] = None,
        topic: Optional[str] = None,
        only_blind_spots: bool = False,
    ) -> List[dict]:
        """Returns top published events for the dashboard."""
        stmt = select(NewsEventModel).where(
            NewsEventModel.status == EventStatus.PUBLISHED
        )

        if only_blind_spots:
            stmt = stmt.where((NewsEventModel.is_blind_spot.is_(True)))

        if min_score is not None:
            stmt = stmt.where(NewsEventModel.hot_score >= min_score)

        if topic:
            # Check if topic key exists in JSONB
            # SQLAlchemy has jsonb_has_key but text is safer for portable logic if we stick to Postgres
            from sqlalchemy import text

            stmt = stmt.where(text("main_topic_counts ? :topic")).params(topic=topic)

        stmt = stmt.order_by(desc(NewsEventModel.hot_score)).limit(limit).offset(offset)

        events = self.session.scalars(stmt).all()

        return [
            {
                "id": str(e.id),
                "title": e.title,
                "hot_score": e.hot_score,
                "editorial_score": e.editorial_score,
                "created_at": e.created_at.isoformat() if e.created_at else None,
                "article_count": e.article_count,
                "is_blind_spot": e.is_blind_spot,
                "blind_spot_side": e.blind_spot_side,
            }
            for e in events
        ]

    def search_events_hybrid(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Performs a hybrid (keyword + vector) search for events.
        Lazily loads NewsCluster to avoid overhead if not used.
        """
        from app.workers.cluster.domain import NewsCluster

        # We can't cache this easily on the instance without managing lifecycle,
        # so we instantiate. NewsCluster initialization is somewhat heavy (loads models).
        # Optimization: In a persistent app (Streamlit/FastAPI), this should be a singleton.
        cluster = NewsCluster()

        # This returns [(event, score, explanation), ...]
        results = cluster.search_news_events_hybrid(
            self.session,
            query_text=query,
            query_vector=None,  # It will calculate it
            target_date=datetime.now(
                timezone.utc
            ),  # Scope to recent? Or allow all? Domain default is 7 days.
            limit=limit,
        )

        return [
            {
                "id": str(e.id),
                "title": e.title,
                "hot_score": e.hot_score,
                "search_score": score,
                "article_count": e.article_count,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e, score, _ in results
        ]

    def get_event_details(self, event_id: str) -> Optional[dict]:
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return None

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return None

        # Helper to safely render summary
        summary_text = ""
        if event.summary:
            if isinstance(event.summary, dict):
                # Try to find a standard summary field
                summary_text = (
                    event.summary.get("content")
                    or event.summary.get("text")
                    or str(event.summary)
                )
            else:
                summary_text = str(event.summary)

        # Get Articles
        articles_data = []
        for art in event.articles:
            articles_data.append(
                {
                    "id": str(art.id),
                    "title": art.title,
                    "source": art.newspaper.name if art.newspaper else "Unknown",
                    "bias": art.newspaper.bias if art.newspaper else "Unknown",
                    "url": art.original_url,
                    "published_date": (
                        art.published_date.isoformat() if art.published_date else None
                    ),
                    "stance": art.stance,
                    "clickbait_score": art.clickbait_score,
                }
            )

        # Check Active Queue
        from sqlalchemy import or_

        q_stmt = (
            select(EventsQueueModel)
            .where(
                EventsQueueModel.event_id == event.id,
                EventsQueueModel.status.in_(
                    [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.WAITING]
                ),
            )
            .limit(1)
        )
        active_job = self.session.scalar(q_stmt)
        queue_tag = (
            f"{active_job.queue_name.value} ({active_job.status.value})"
            if active_job
            else None
        )

        # Check Active Proposals
        p_stmt = (
            select(MergeProposalModel)
            .where(
                or_(
                    MergeProposalModel.source_event_id == event.id,
                    MergeProposalModel.target_event_id == event.id,
                ),
                MergeProposalModel.status == JobStatus.PENDING,
            )
            .limit(1)
        )
        active_prop = self.session.scalar(p_stmt)
        prop_tag = "Merge Proposed" if active_prop else None

        return {
            "id": str(event.id),
            "title": event.title,
            "subtitle": event.subtitle,
            "status": event.status.value,
            "hot_score": event.hot_score,
            "editorial_score": event.editorial_score,
            "ai_impact_score": event.ai_impact_score,
            "insights": event.publisher_insights,
            "summary": summary_text,
            "topics": event.main_topic_counts,
            "stance_dist": event.stance_distribution,
            "clickbait_dist": event.clickbait_distribution,
            "articles": sorted(
                articles_data, key=lambda x: x["published_date"] or "", reverse=True
            ),
            "last_summarized": (
                event.last_summarized_at.isoformat()
                if event.last_summarized_at
                else None
            ),
            "queue_tag": queue_tag,
            "proposal_tag": prop_tag,
        }

    def boost_event(self, event_id: str, amount: int = 10) -> str:
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        event.editorial_score = (event.editorial_score or 0) + amount

        # Recalc
        publisher = NewsPublisherDomain()
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        new_score, insights, _ = publisher.calculate_spectrum_score(event, topics)
        event.hot_score = new_score
        event.publisher_insights = insights

        self.session.commit()
        return f"✅ Event boosted by {amount}. New Score: {new_score:.2f}"

    def archive_event(self, event_id: str) -> str:
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        event.status = EventStatus.ARCHIVED
        event.is_active = False
        self.session.commit()
        return "✅ Event killed (Archived)."

    def get_waiting_events(self) -> List[dict]:
        """Returns events waiting for approval."""
        events = self.session.scalars(
            select(NewsEventModel)
            .join(EventsQueueModel, NewsEventModel.id == EventsQueueModel.event_id)
            .where(EventsQueueModel.status == JobStatus.WAITING)
            .order_by(desc(NewsEventModel.created_at))
        ).all()

        return [
            {
                "id": str(e.id),
                "title": e.title,
                "article_count": e.article_count,
                "reason": "Topic/Niche" if e.article_count > 5 else "Low Volume",
            }
            for e in events
        ]

    def approve_event(self, event_id: str) -> str:
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        event.status = EventStatus.PUBLISHED
        event.published_at = datetime.now(timezone.utc)

        # Recalc Score
        publisher = NewsPublisherDomain()
        topics = list(event.main_topic_counts.keys()) if event.main_topic_counts else []
        new_score, insights, _ = publisher.calculate_spectrum_score(event, topics)
        event.hot_score = new_score
        event.publisher_insights = insights

        # Update queue item to COMPLETED
        q_item = self.session.scalar(
            select(EventsQueueModel).where(
                EventsQueueModel.event_id == event.id,
                EventsQueueModel.status == JobStatus.WAITING,
            )
        )
        if q_item:
            q_item.status = JobStatus.COMPLETED
            q_item.msg = "Approved via MCP/CLI"

        self.session.commit()
        return f"✅ Event '{event.title}' Published!"

    def dissolve_event(self, event_id: str) -> str:
        """Unlinks articles, resets them for enrichment, and deletes the event."""
        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        # 1. Unlink Articles & Re-queue
        articles = event.articles
        count = len(articles)

        if articles:
            queue_data = []
            now = datetime.now(timezone.utc)

            for art in articles:
                art.event_id = None  # Unlink

                # Strip Article Info (Reset for Re-Enrichment)
                art.title = "Unknown"
                art.subtitle = None
                art.summary = None
                art.published_date = None
                art.stance = None
                art.stance_reasoning = None
                art.clickbait_score = None
                art.clickbait_reasoning = None
                art.main_topics = None
                art.key_points = None
                art.entities = []
                art.interests = {}
                art.embedding = None
                art.summary_status = JobStatus.PENDING

                queue_data.append(
                    {
                        "article_id": art.id,
                        "status": JobStatus.PENDING,
                        "queue_name": ArticlesQueueName.ENRICHER,
                        "created_at": now,
                        "updated_at": now,
                        "attempts": 0,
                        "msg": f"Dissolved from Event {event.id}",
                    }
                )

            self.session.add_all(articles)

            if queue_data:
                stmt = insert(ArticlesQueueModel).values(queue_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["article_id"],
                    set_={
                        "status": JobStatus.PENDING,
                        "queue_name": ArticlesQueueName.ENRICHER,
                        "updated_at": now,
                        "msg": stmt.excluded.msg,
                    },
                )
                self.session.execute(stmt)

        # 2. Cleanup Dependencies
        self.session.execute(
            delete(MergeProposalModel).where(
                (MergeProposalModel.source_event_id == event.id)
                | (MergeProposalModel.target_event_id == event.id)
            )
        )
        self.session.execute(
            delete(EventsQueueModel).where(EventsQueueModel.event_id == event.id)
        )

        # 3. Delete Event
        self.session.delete(event)
        self.session.commit()
        return f"✅ Event Dissolved! {count} articles re-queued."

    def requeue_articles_for_enrichment(self, event_id: str) -> str:
        """
        Forces all articles linked to this event back to the ENRICHER queue.
        Does NOT unlink them or delete the event (unlike dissolve).
        """
        from sqlalchemy import update

        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        articles = event.articles
        if not articles:
            return "⚠️ No articles to requeue."

        now = datetime.now(timezone.utc)
        article_ids = [a.id for a in articles]
        msg = f"Forced Requeue from Event {event.id}"

        # 1. Update existing active jobs
        updated_ids = []
        if article_ids:
            # We reset any job that isn't COMPLETED (so PENDING, PROCESSING, FAILED)
            # Actually, even COMPLETED ones should be reset if we are forcing re-enrichment?
            # Yes, if we want to re-enrich, we should probably grab ANY job.
            # But the table might have duplicates.
            # Ideally we want ONE active job.
            # Strategy: Update ALL jobs for these articles to PENDING.
            # But wait, if an article has 5 old completed jobs, we don't want to make them all pending.
            # We should insert a NEW job if no PENDING/PROCESSING job exists.

            # Let's try to update OPEN jobs first.
            stmt = (
                update(ArticlesQueueModel)
                .where(
                    ArticlesQueueModel.article_id.in_(article_ids),
                    ArticlesQueueModel.status.in_(
                        [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.FAILED]
                    ),
                )
                .values(
                    status=JobStatus.PENDING,
                    queue_name=ArticlesQueueName.ENRICHER,
                    updated_at=now,
                    msg=msg,
                    attempts=0,
                )
                .returning(ArticlesQueueModel.article_id)
            )

            result = self.session.execute(stmt)
            updated_ids = [r[0] for r in result.all()]

        # 2. Insert for articles that didn't have an open job
        missing_ids = set(article_ids) - set(updated_ids)
        if missing_ids:
            new_jobs = [
                {
                    "article_id": aid,
                    "status": JobStatus.PENDING,
                    "queue_name": ArticlesQueueName.ENRICHER,
                    "created_at": now,
                    "updated_at": now,
                    "attempts": 0,
                    "msg": msg,
                }
                for aid in missing_ids
            ]
            self.session.execute(insert(ArticlesQueueModel), new_jobs)

        self.session.commit()
        return f"✅ Re-queued {len(articles)} articles for Enrichment."

    def requeue_event_for_enhancement(self, event_id: str) -> str:
        """
        Re-queues the Event itself for Enhancement (Summary/Score update).
        """
        from sqlalchemy import select

        try:
            uid = uuid.UUID(event_id)
        except ValueError:
            return "❌ Invalid UUID"

        event = self.session.get(NewsEventModel, uid)
        if not event:
            return "❌ Event not found."

        now = datetime.now(timezone.utc)

        # Check for existing job
        stmt = select(EventsQueueModel).where(
            EventsQueueModel.event_id == event.id,
            EventsQueueModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )
        existing_job = self.session.scalar(stmt)

        if existing_job:
            existing_job.status = JobStatus.PENDING
            existing_job.updated_at = now
            existing_job.attempts = 0
            existing_job.msg = "Manual Re-Enhance"
            existing_job.queue_name = EventsQueueName.ENHANCER
        else:
            new_job = EventsQueueModel(
                event_id=event.id,
                status=JobStatus.PENDING,
                queue_name=EventsQueueName.ENHANCER,
                created_at=now,
                updated_at=now,
                attempts=0,
                msg="Manual Re-Enhance",
            )
            self.session.add(new_job)

        self.session.commit()
        return f"✅ Event '{event.title}' queued for Enhancement."
