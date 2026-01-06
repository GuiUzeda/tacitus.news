# models.py
import enum
import queue
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from news_events_lib.models import ArticleModel, BaseModel, NewsEventModel
from sqlalchemy import (Column, DateTime, Enum, ForeignKey, Index, Integer,
                        String, Text)
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship


@dataclass
class ClusterResult:
    action: str  # 'MERGE', 'PROPOSE', 'NEW', 'ORPHAN'
    event_id: Optional[uuid.UUID]
    candidates: List[dict] # For the CLI menu
    reason: str
    
class JobStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class ArticlesQueueName(enum.Enum):
    FILTER = "filter"
    CLUSTER = "cluster"


class EventsQueueName(enum.Enum):
    ENHANCER = "enhancer"
    PUBLISHER = "publisher"

    
    

class ArticlesQueueModel(BaseModel):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return 'articles_queue'

    id = Column(Integer, primary_key=True, index=True)
    article_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("articles.id"), unique=True)
    article: Mapped["ArticleModel"] = relationship()


    # Intelligence
    estimated_tokens = Column(Integer) # Calculated during filter stage
    
    # Worker Control
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.PENDING, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    msg: Mapped[str] = mapped_column(Text, nullable=True)
    queue_name: Mapped[ArticlesQueueName] = mapped_column(Enum(ArticlesQueueName), default=ArticlesQueueName.FILTER, index=True)
    
    
    __table_args__ = (
        Index(
            'ix_queue_pending_fifo', 
            'updated_at', 
            'queue_name' ,
            postgresql_where=(status == JobStatus.PENDING)
        ),
    )
    
class EventsQueueModel(BaseModel):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return 'events_queue'

    id = Column(Integer, primary_key=True, index=True)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("news_events.id"), unique=True)
    event: Mapped["NewsEventModel"] = relationship()

    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.PENDING, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    msg: Mapped[str] = mapped_column(Text, nullable=True)
    queue_name: Mapped[EventsQueueName] = mapped_column(Enum(EventsQueueName), default=EventsQueueName.ENHANCER, index=True)
    
    __table_args__ = (
        Index(
            'ix_events_queue_pending_fifo', 
            'updated_at', 
            'queue_name' ,
            postgresql_where=(status == JobStatus.PENDING)
        ),
    )