import uuid
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class NewsArticleBaseSchema(BaseModel):
    url_hash: str
    original_url: str
    title: str
    subtitle: Optional[str] = None
    summary: Optional[str] = None
    published_date: datetime

    stance: Optional[float] = None
    stance_reasoning: Optional[str] = None
    clickbait_score: Optional[float] = None
    clickbait_reasoning: Optional[str] = None

    main_topics: Optional[List[str]] = None
    key_points: Optional[List[str]] = None
    entities: Optional[List[str]] = None
    interests: Optional[Dict[str, List[str]]] = None
    source_rank: Optional[int] = None

    embedding: Optional[List[float]] = None


class NewsArticleCreateSchema(NewsArticleBaseSchema):
    newspaper_id: uuid.UUID
    event_id: Optional[uuid.UUID] = None


class NewsArticleReadSchema(NewsArticleBaseSchema):
    id: uuid.UUID
    newspaper_id: uuid.UUID
    event_id: Optional[uuid.UUID] = None

    model_config = ConfigDict(from_attributes=True)


class NewsArticleContentBaseSchema(BaseModel):
    content: str


class NewsArticleContentCreateSchema(NewsArticleContentBaseSchema):
    article_id: uuid.UUID


class NewsArticleContentReadSchema(NewsArticleContentBaseSchema):
    id: uuid.UUID
    article_id: uuid.UUID
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)
