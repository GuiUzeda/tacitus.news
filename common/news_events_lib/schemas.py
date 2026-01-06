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


class FeedCreateSchema(FeedBaseSchema):
    newspaper_id: uuid.UUID

class FeedReadSchema(FeedBaseSchema):
    id: uuid.UUID
    newspaper_id: uuid.UUID
    
    model_config = ConfigDict(from_attributes=True)


# --- NewsEvent Schemas ---
class NewsEventBaseSchema(BaseModel):
    title: str
    summary: Dict[str, str] = Field(
        default_factory=dict, 
        description='Structure: {"left": "txt_markdown", "right": "txt_markdown", "center": "txt_markdown", "bias": "txt_markdown"}'
    )
    is_active: bool = True
    article_count: int = 1
    bias_distribution: Dict[str, int] = Field(
        default_factory=dict, 
        description='Structure: {"left": 4, "right": 2, "center": 1}'
    )
    stance_distribution: Dict[str, Dict[str, int]] = Field(
        default_factory=dict, 
        description='Structure: {"left": {"critical": 5...}, ...}'
    )
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
    
    model_config = ConfigDict(from_attributes=True)


# --- NewsArticle Schemas ---
class NewsArticleBaseSchema(BaseModel):
    url_hash: str
    original_url: str
    title: str
    subtitle: str
    summary: str
    published_date: datetime
    stance_label: str
    stance_reasoning: str
    main_topics: str
    embedding: List[float]

class NewsArticleCreateSchema(NewsArticleBaseSchema):
    newspaper_id: uuid.UUID
    event_id: uuid.UUID

class NewsArticleReadSchema(NewsArticleBaseSchema):
    id: uuid.UUID
    newspaper_id: uuid.UUID
    event_id: uuid.UUID
    
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