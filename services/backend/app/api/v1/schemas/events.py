import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# --- BASE (Keep existing) ---
class NewsEventBaseSchema(BaseModel):
    title: str
    subtitle: Optional[str] = None
    summary: Dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True
    status: str = "draft"
    article_count: int = 1
    bias_distribution: Dict[str, List[str]] = Field(default_factory=dict)
    article_counts_by_bias: Dict[str, int] = Field(default_factory=dict)
    stance_distribution: Dict[str, Dict[str, int]] = Field(default_factory=dict)
    main_topic_counts: Optional[Dict[str, int]] = Field(default_factory=dict)
    interest_counts: Optional[Dict[str, Dict[str, int]]] = Field(default_factory=dict)
    ownership_stats: Optional[Dict[str, int]] = Field(default_factory=dict)
    clickbait_distribution: Optional[Dict[str, float]] = Field(default_factory=dict)
    hot_score: float = 0.0
    editorial_score: float = 0.0
    ai_impact_score: Optional[int] = None
    category_tag: Optional[str] = None
    is_blind_spot: Optional[bool] = False
    blind_spot_side: Optional[str] = None


# --- LIST SCHEMA (UPDATE THIS!) ---
class NewsEventListSchema(BaseModel):
    """Lighter list schema"""

    id: uuid.UUID
    title: str
    subtitle: Optional[str] = None
    category_tag: Optional[str] = Field(default="GENERAL")
    created_at: datetime
    last_article_date: Optional[datetime] = Field(default_factory=datetime.now)
    last_updated_at: datetime
    ai_impact_score: Optional[int] = Field(default=0)
    article_count: Optional[int] = Field(default=0)
    article_counts_by_bias: Dict[str, int] = Field(
        default_factory=lambda: {"left": 0, "center": 0, "right": 0},
    )
    stance_distribution: Dict[str, Dict[str, float]] = Field(default_factory=dict)
    clickbait_distribution: Dict[str, float] = Field(default_factory=dict)
    publisher_insights: Optional[List[str]] = Field(default_factory=list)
    sources_snapshot: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    stance: float = Field(default=0.0)
    is_blind_spot: Optional[bool] = Field(default=False)
    blind_spot_side: Optional[str] = None
    is_international: bool
    main_topic_counts: Optional[Dict[str, int]] = Field(default_factory=dict)

    model_config = ConfigDict(from_attributes=True)


# --- DETAIL SCHEMA (Keep existing) ---
class NewsEventReadSchema(NewsEventBaseSchema):
    id: uuid.UUID
    created_at: datetime
    last_updated_at: datetime
    published_at: Optional[datetime] = None
    ai_impact_reasoning: Optional[str] = None
    merged_into_id: Optional[uuid.UUID] = None
    best_source_rank: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)
