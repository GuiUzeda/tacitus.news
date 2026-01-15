import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationInfo

# --- BASE ---
class NewsEventBaseSchema(BaseModel):
    title: str
    subtitle: Optional[str] = None
    summary: Dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True
    status: str = "draft"
    article_count: int = 1
    
    # Raw Data Fields
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

# --- LIST SCHEMA (Optimized for Feed) ---
class NewsEventListSchema(BaseModel):
    id: uuid.UUID
    title: str
    category: Optional[str] = Field(default="GENERAL", validation_alias="category_tag")
    time: datetime = Field(..., validation_alias="published_at")
    
    # 1. IMPACT SCORE (The Ring)
    # Backend has 'ai_impact_score', Frontend wants 'impact'
    impact: int = Field(default=0, validation_alias="ai_impact_score")

    # 2. SOURCE COUNT
    sourceCount: int = Field(default=0, validation_alias="article_count")

    # 3. BIAS DOTS
    # Backend has 'article_counts_by_bias', Frontend wants 'biasDistribution' (numbers)
    biasDistribution: Dict[str, int] = Field(
        default_factory=lambda: {"left": 0, "center": 0, "right": 0}, 
        validation_alias="article_counts_by_bias"
    )

    # 4. CLICKBAIT
    clickbait: float = 0.0

    # 5. COMPUTED FIELDS
    summary: str = "" # Flattened string
    isBlindspot: bool = False

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @field_validator("summary", mode="before")
    @classmethod
    def flatten_summary(cls, v: Any) -> str:
        # Extract the narrative summary from the JSON object
        if isinstance(v, dict):
            return v.get("bias") or v.get("center") or v.get("subtitle") or ""
        return str(v or "")

    @field_validator("isBlindspot", mode="before")
    @classmethod
    def check_blindspot(cls, v: Any, info: ValidationInfo) -> bool:
        # We need to look at the 'summary' field of the ORM object to find insights
        # Since pydantic v2 validation context is tricky with from_attributes,
        # we might need to rely on the backend logic populating a transient field 
        # or check 'summary.insights' if available in the dict.
        
        # Simplified approach: Pass explicitly or check if 'BLIND_SPOT' is in summary tags
        # Note: In a real Pydantic v2 ORM scenario, accessing sibling fields during validation 
        # can be complex. For now, defaulting to False unless explicit.
        return False 
    
    @field_validator("clickbait", mode="before")
    @classmethod
    def calc_clickbait(cls, v: Any, info: ValidationInfo) -> float:
        # Calculate average from distribution dict if needed, or pass pre-calculated
        if isinstance(v, dict) and v:
            return sum(v.values()) / len(v)
        return 0.0

# --- DETAIL SCHEMA ---
class NewsEventReadSchema(NewsEventBaseSchema):
    id: uuid.UUID
    created_at: datetime
    last_updated_at: datetime
    published_at: Optional[datetime] = None
    ai_impact_reasoning: Optional[str] = None
    merged_into_id: Optional[uuid.UUID] = None
    best_source_rank: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)