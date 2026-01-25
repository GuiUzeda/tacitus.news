from datetime import datetime
from typing import Optional

from news_events_lib.models import EventStatus, JobStatus
from pydantic import BaseModel, ConfigDict, Field

# --- Base Components ---


class TimeRangeFilter(BaseModel):
    """Common time-based filtering."""

    created_at__gte: Optional[datetime] = Field(None, description="Created after")
    created_at__lte: Optional[datetime] = Field(None, description="Created before")
    updated_at__gte: Optional[datetime] = Field(None, description="Updated after")
    updated_at__lte: Optional[datetime] = Field(None, description="Updated before")


class QueueBaseFilter(TimeRangeFilter):
    """Common fields for all queue items."""

    status: Optional[JobStatus] = None
    attempts: Optional[int] = None
    attempts__gt: Optional[int] = Field(None, description="Attempts greater than")
    attempts__lt: Optional[int] = Field(None, description="Attempts less than")
    msg__ilike: Optional[str] = Field(
        None, description="Case-insensitive search in message/error"
    )
    order_by: Optional[str] = Field(
        None, description="Field to order by (prefix with - for descending)"
    )


# --- Specific Queue Filters ---


class ArticleQueueFilter(QueueBaseFilter):
    """Filter criteria for the Articles Queue."""

    article__title__ilike: Optional[str] = Field(
        None, description="Search article title"
    )
    article__original_url__ilike: Optional[str] = Field(None, description="Search URL")
    article__newspaper__name__ilike: Optional[str] = Field(
        None, description="Search newspaper name"
    )

    model_config = ConfigDict(extra="allow")


class EventQueueFilter(QueueBaseFilter):
    """Filter criteria for the Events Queue (Enhancer, Publisher, etc.)."""

    event__title__ilike: Optional[str] = Field(None, description="Search event title")
    event__status: Optional[str] = Field(None, description="Event status")

    model_config = ConfigDict(extra="allow")


class ProposalQueueFilter(QueueBaseFilter):
    """Filter criteria for Merge Proposals."""

    proposal_type: Optional[str] = Field(None, description="Type of proposal")
    reasoning__ilike: Optional[str] = Field(None, description="Search reasoning")
    distance_score__lt: Optional[float] = Field(
        None, description="Distance score less than"
    )

    model_config = ConfigDict(extra="allow")


# --- Entity Filters (Direct DB) ---


class EntityBaseFilter(TimeRangeFilter):
    """Common fields for database entities."""

    id: Optional[str] = None
    order_by: Optional[str] = Field(
        None, description="Field to order by (prefix with - for descending)"
    )


class ArticleFilter(EntityBaseFilter):
    """Filters for searching Articles."""

    title__ilike: Optional[str] = Field(None, description="Search title")
    content__ilike: Optional[str] = Field(None, description="Search content body")
    url_hash: Optional[str] = Field(None, description="Exact URL hash")
    original_url__ilike: Optional[str] = Field(None, description="Search URL")
    published_date__gte: Optional[datetime] = Field(None, description="Published after")
    newspaper__name__ilike: Optional[str] = Field(None, description="Newspaper name")

    model_config = ConfigDict(extra="allow")


class EventFilter(EntityBaseFilter):
    """Filters for searching Events."""

    status: Optional[EventStatus] = Field(None, description="Lifecycle status")
    is_active: Optional[bool] = Field(None, description="Active status")
    hot_score__gt: Optional[float] = Field(None, description="Hot score greater than")
    article_count__gt: Optional[int] = Field(
        None, description="Article count greater than"
    )
    title__ilike: Optional[str] = Field(None, description="Search title")

    model_config = ConfigDict(extra="allow")


class NewspaperFilter(EntityBaseFilter):
    """Filters for searching Newspapers."""

    name__ilike: Optional[str] = Field(None, description="Name search")
    bias: Optional[str] = Field(None, description="Bias rating")

    model_config = ConfigDict(extra="allow")


class AuditFilter(EntityBaseFilter):
    """Filters for searching Audit logs."""

    target_table: Optional[str] = Field(None, description="Name of the table affected")
    target_id: Optional[str] = Field(None, description="ID of the affected entity")
    action: Optional[str] = Field(
        None, description="Action taken (INSERT, UPDATE, DELETE)"
    )
    actor_id: Optional[str] = Field(
        None, description="ID of the actor who performed the action"
    )

    model_config = ConfigDict(extra="allow")
