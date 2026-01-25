from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

from app.services.sqlalchemy_filter import SQLAlchemyFilter
from news_events_lib.models import (
    EventsQueueModel,
    EventsQueueName,
    JobStatus,
    NewsEventModel,
)
from sqlalchemy import asc, desc, or_, select, text, update
from sqlalchemy.orm import Session


class QueueService:
    """Base logic for managing processing queues."""

    def __init__(self, session: Session):
        self.session = session

    def requeue_incomplete_events(self) -> int:
        """
        Resets events that finished enhancement but still lack a subtitle or summary.
        """
        stmt = (
            select(NewsEventModel)
            .join(EventsQueueModel, NewsEventModel.id == EventsQueueModel.event_id)
            .where(
                or_(
                    NewsEventModel.subtitle.is_(None),
                    NewsEventModel.subtitle == "",
                    NewsEventModel.summary == {},
                ),
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                EventsQueueModel.status == JobStatus.COMPLETED,
            )
        )

        events = self.session.scalars(stmt).all()
        count = 0
        for event in events:
            # We use a simple update or add approach
            q_stmt = select(EventsQueueModel).where(
                EventsQueueModel.event_id == event.id,
                EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
            )
            q_item = self.session.scalars(q_stmt).first()

            if q_item:
                q_item.status = JobStatus.PENDING
                q_item.attempts = 0
                q_item.msg = "Re-queued: missing data"
                count += 1

        self.session.commit()
        return count

    def requeue_stale_items(self, minutes_stale: int = 10) -> Dict[str, int]:
        """
        Resets jobs that have been stuck in PROCESSING for too long.
        """
        # 1. Proposals
        q_proposals = text("""
            UPDATE merge_proposals
            SET status = 'PENDING',
                updated_at = NOW(),
                reasoning = 'Re-queued stale processing proposal'
            WHERE status = 'PROCESSING'
              AND updated_at < NOW() - make_interval(mins => :mins)
        """)
        r_proposals = self.session.execute(q_proposals, {"mins": minutes_stale})

        # 2. Articles Queue
        q_articles = text("""
            UPDATE articles_queue
            SET status = 'PENDING',
                updated_at = NOW(),
                msg = 'Re-queued stale processing job'
            WHERE status = 'PROCESSING'
              AND updated_at < NOW() - make_interval(mins => :mins)
        """)
        r_articles = self.session.execute(q_articles, {"mins": minutes_stale})

        # 3. Events Queue
        q_events = text("""
            UPDATE events_queue
            SET status = 'PENDING',
                updated_at = NOW(),
                msg = 'Re-queued stale processing job'
            WHERE status = 'PROCESSING'
              AND updated_at < NOW() - make_interval(mins => :mins)
        """)
        r_events = self.session.execute(q_events, {"mins": minutes_stale})

        self.session.commit()

        return {
            "proposals": r_proposals.rowcount,
            "articles": r_articles.rowcount,
            "events": r_events.rowcount,
        }

    def _list_queue(
        self,
        model_class: Type,
        filters: Dict[str, Any],
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        stmt = select(model_class)
        order_by = filters.pop("order_by", None)

        if filters:
            f = SQLAlchemyFilter(model_class, stmt)
            stmt = f.apply(filters)

        if order_by:
            if order_by.startswith("-"):
                field_name = order_by[1:]
                if hasattr(model_class, field_name):
                    stmt = stmt.order_by(desc(getattr(model_class, field_name)))
            else:
                field_name = order_by
                if hasattr(model_class, field_name):
                    stmt = stmt.order_by(asc(getattr(model_class, field_name)))
        else:
            stmt = stmt.order_by(model_class.updated_at.desc())

        stmt = stmt.limit(limit).offset(offset)
        rows = self.session.scalars(stmt).all()
        return [self._serialize(row) for row in rows]

    def _retry_failed(
        self, model_class: Type, queue_name_filter: Optional[Any] = None
    ) -> int:
        now = datetime.now(timezone.utc)
        stmt = (
            update(model_class)
            .where(model_class.status == JobStatus.FAILED)
            .values(status=JobStatus.PENDING, msg="Manual Retry", updated_at=now)
        )
        if queue_name_filter:
            stmt = stmt.where(model_class.queue_name == queue_name_filter)

        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount

    def _serialize(self, row: Any) -> Dict[str, Any]:
        data = {}
        # Columns to exclude from serialization (e.g. large embeddings)
        exclude_columns = {"embedding", "embedding_centroid"}

        for column in row.__table__.columns:
            if column.name in exclude_columns:
                continue

            val = getattr(row, column.name)
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            elif hasattr(val, "value") and hasattr(val, "__str__"):
                val = val.value
            data[column.name] = val
        return data
