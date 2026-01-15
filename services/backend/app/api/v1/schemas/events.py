import uuid
from datetime import datetime
from typing import List, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class NewsEventBaseSchema(BaseModel):
    title: str
    subtitle: Optional[str] = None
    summary: Dict[str, list | str] = Field(
        default_factory=dict,
        description='Structure: {"left": "txt_markdown", "right": "txt_markdown", "center": "txt_markdown", "bias": "txt_markdown"}',
    )
    is_active: bool = True
    status: str = "draft"
    article_count: int = 1

    bias_distribution: Dict[str, List[str]] = Field(
        default_factory=dict,
        description='Structure: {"left": ["Source A"], "right": ["Source B"]}',
    )
    article_counts_by_bias: Dict[str, int] = Field(
        default_factory=dict,
        description='Structure: {"left": 4, "right": 2, "center": 1}',
    )

    stance_distribution: Dict[str, Dict[str, int]] = Field(
        default_factory=dict, description='Structure: {"left": {"critical": 5...}, ...}'
    )

    main_topic_counts: Optional[Dict[str, int]] = Field(default_factory=dict)
    interest_counts: Optional[Dict[str, Dict[str, int]]] = Field(default_factory=dict)
    ownership_stats: Optional[Dict[str, int]] = Field(default_factory=dict)
    clickbait_distribution: Optional[Dict[str, float]] = Field(default_factory=dict)

    hot_score: float = 0.0
    editorial_score: float = 0.0
    ai_impact_score: Optional[int] = None
    category_tag: Optional[str] = None
class NewsEventListSchema(BaseModel):
    """
    NewsEventPrimitiveSchema Simple for listing

    This is a simpler model for simple listing in the FE

    """
    title: str
    subtitle: Optional[str] = None
    bias_distribution: Dict[str, List[str]] = Field(
        default_factory=dict,
        description='Structure: {"left": ["Source A"], "right": ["Source B"]}',
    )
    article_counts_by_bias: Dict[str, int] = Field(
        default_factory=dict,
        description='Structure: {"left": 4, "right": 2, "center": 1}',
    )

    stance_distribution: Dict[str, Dict[str, int]] = Field(
        default_factory=dict, description='Structure: {"left": {"critical": 5...}, ...}'
    )
    ownership_stats: Optional[Dict[str, int]] = Field(default_factory=dict)
    clickbait_distribution: Optional[Dict[str, float]] = Field(default_factory=dict)
    hot_score: float = 0.0
    category_tag: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
    
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