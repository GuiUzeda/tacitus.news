import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationInfo

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

# --- LIST SCHEMA (UPDATE THIS!) ---
class NewsEventListSchema(BaseModel):
    id: uuid.UUID
    title: str
    subtitle: Optional[str] = None
    
    category: Optional[str] = Field(default="GENERAL", validation_alias="category_tag")
    time: datetime = Field(..., validation_alias="last_updated_at")
    
    # 1. Map 'ai_impact_score' -> 'impact'
    impact: Optional[int] = Field(default=0, validation_alias="ai_impact_score")

    # 2. Map 'article_count' -> 'sourceCount'
    sourceCount: Optional[int] = Field(default=0, validation_alias="article_count")

    # 3. Map 'article_counts_by_bias' -> 'biasDistribution'
    biasDistribution: Dict[str, int] = Field(
        default_factory=lambda: {"left": 0, "center": 0, "right": 0}, 
        validation_alias="article_counts_by_bias"
    )

    # 4. Computed Fields
    clickbait: float = 0.0
    summary: str = "" 
    isBlindspot: bool = False

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @field_validator("summary", mode="before")
    @classmethod
    def flatten_summary(cls, v: Any) -> str:
        if isinstance(v, dict):
            # Prioritize the narrative bias summary
            return v.get("bias") or v.get("center") or v.get("subtitle") or ""
        return str(v or "")

    @field_validator("clickbait", mode="before")
    @classmethod
    def calc_clickbait(cls, v: Any) -> float:
        # v is likely the clickbait_distribution dict passed from the ORM
        if isinstance(v, dict) and v:
            return sum(v.values()) / len(v)
        return 0.0

    @field_validator("isBlindspot", mode="before")
    @classmethod
    def check_blindspot(cls, v: Any, info: ValidationInfo) -> bool:
        # Check logic here if you have specific flags, otherwise default False
        return False

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