import uuid
from datetime import datetime
from typing import List, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# --- Newspaper Schemas ---
class NewspaperBaseSchema(BaseModel):
    name: str
    bias: str
    description: str
    icon_url: str
    logo_url: str
    ownership_type: Optional[str] = None

class NewspaperCreateSchema(NewspaperBaseSchema):
    pass

class NewspaperReadSchema(NewspaperBaseSchema):
    id: uuid.UUID
    
    model_config = ConfigDict(from_attributes=True)


# --- Feed Schemas ---
class FeedBaseSchema(BaseModel):
    url: str
    is_active: bool = True
    allowed_sections: Optional[str] = None
    blocklist: Optional[str] = None
    url_pattern: Optional[str] = None
    feed_type: str = "sitemap"
    is_ranked: bool = False
    use_browser_render: bool = False
    scroll_depth: int = 1


class FeedCreateSchema(FeedBaseSchema):
    newspaper_id: uuid.UUID

class FeedReadSchema(FeedBaseSchema):
    id: uuid.UUID
    newspaper_id: uuid.UUID
    
    model_config = ConfigDict(from_attributes=True)


# --- NewsEvent Schemas ---
class NewsEventBaseSchema(BaseModel):
    title: str
    subtitle: Optional[str] = None
    summary: Dict[str, str] = Field(
        default_factory=dict, 
        description='Structure: {"left": "txt_markdown", "right": "txt_markdown", "center": "txt_markdown", "bias": "txt_markdown"}'
    )
    is_active: bool = True
    status: str = "draft"
    article_count: int = 1
    
    bias_distribution: Dict[str, List[str]] = Field(
        default_factory=dict, 
        description='Structure: {"left": ["Source A"], "right": ["Source B"]}'
    )
    article_counts_by_bias: Dict[str, int] = Field(
        default_factory=dict,
        description='Structure: {"left": 4, "right": 2, "center": 1}'
    )
    
    stance_distribution: Dict[str, Dict[str, int]] = Field(
        default_factory=dict, 
        description='Structure: {"left": {"critical": 5...}, ...}'
    )
    
    main_topic_counts: Dict[str, int] = Field(default_factory=dict)
    interest_counts: Dict[str, Dict[str, int]] = Field(default_factory=dict)
    ownership_stats: Dict[str, int] = Field(default_factory=dict)
    clickbait_distribution: Dict[str, float] = Field(default_factory=dict)
    
    hot_score: float = 0.0
    editorial_score: float = 0.0
    ai_impact_score: Optional[int] = None
    category_tag: Optional[str] = None
    
    embedding_centroid: List[float]

    @field_validator('embedding_centroid')
    @classmethod
    def validate_embedding_dimensions(cls, v: List[float]) -> List[float]:
        if len(v) != 768:
            raise ValueError('embedding_centroid must have exactly 768 dimensions')
        return v

class NewsEventCreateSchema(NewsEventBaseSchema):
    pass

class NewsEventReadSchema(NewsEventBaseSchema):
    id: uuid.UUID
    created_at: datetime
    last_updated_at: datetime
    published_at: Optional[datetime] = None
    first_article_date: Optional[datetime] = None
    last_article_date: Optional[datetime] = None
    ai_impact_reasoning: Optional[str] = None
    merged_into_id: Optional[uuid.UUID] = None
    best_source_rank: Optional[int] = None
    
    model_config = ConfigDict(from_attributes=True)


# --- NewsArticle Schemas ---
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


# --- NewsArticleContent Schemas ---
class NewsArticleContentBaseSchema(BaseModel):
    content: str

class NewsArticleContentCreateSchema(NewsArticleContentBaseSchema):
    article_id: uuid.UUID

class NewsArticleContentReadSchema(NewsArticleContentBaseSchema):
    id: uuid.UUID
    article_id: uuid.UUID
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)