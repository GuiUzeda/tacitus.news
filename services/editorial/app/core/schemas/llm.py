from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class CategoryEnum(str, Enum):
    # The "Big 5" for UI Badges
    POLITICS = "Politics"
    ECONOMY = "Economy"
    WORLD = "World"  # Use only if no other fit or purely geopolitical
    SOCIETY = "Society"  # Includes Crime, Health, Education
    TECH_SCIENCE = "Tech & Science"
    ENTERTAINMENT = "Entertainment"
    SPORTS = "Sports"
    HEALTH = "Health"
    CRIME = "Crime"
    OTHER = "Other"


class LLMNewsOutputSchema(BaseModel):
    status: Literal["valid", "error", "irrelevant"] = Field(
        default="valid",
        description="Indicates if the article was successfully analyzed, is irrelevant (gossip/ads), or caused an error (block/login wall).",
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Description of the error or reason for irrelevance if status is not 'valid'.",
    )
    summary: str | None = Field(
        default="",
        description="A concise two-sentence summary of the article in Portuguese.",
    )
    key_points: List[str] = Field(
        default_factory=list,
        description="4 to 5 fact-dense bullet points covering Who, What, Where, When, and Context in Portuguese.",
    )
    stance_reasoning: str | None = Field(
        default="", description="Short explanation for the assigned stance score."
    )
    stance: float | None = Field(
        default=0.0,
        ge=-1.0,
        le=1.0,
        description="Sentiment/Support score: -1.0 (Critical/Negative) to 1.0 (Supportive/Positive). 0.0 is neutral.",
    )
    clickbait_reasoning: str | None = Field(
        default="", description="Reasoning for the clickbait score."
    )
    clickbait_score: float | None = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Clickbait probability: 0.0 (Factual headline) to 1.0 (Highly misleading/clickbait).",
    )
    entities: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Named entities extracted from the text grouped by type: person, place, org.",
    )
    main_topics: List[str] = Field(
        default_factory=list,
        description="List of primary themes discussed in the article (e.g., 'Tax Reform', 'Corruption').",
    )
    title: str = Field(
        default="",
        description="The original or a cleaned version of the article title in Portuguese.",
    )
    subtitle: str | None = Field(
        default="", description="Article subtitle or lead excerpt in Portuguese."
    )

    @model_validator(mode="before")
    @classmethod
    def drop_fields_on_error(cls, data: Any) -> Any:
        if isinstance(data, dict):
            status = data.get("status")
            if status in ["error", "irrelevant"]:
                safe_data = {
                    "status": status,
                    "error_message": data.get("error_message"),
                    "title": (
                        data.get("title") if isinstance(data.get("title"), str) else ""
                    ),
                }
                return safe_data
        return data


class LLMBatchNewsOutputSchema(BaseModel):
    results: List[LLMNewsOutputSchema] = Field(
        description="List of analysis results, one for each input article."
    )


class EventMatchSchema(BaseModel):
    """
    Schema for the Event Co-reference Resolution.
    """

    reasoning: str = Field(
        description="Concise logical explanation for the matching decision."
    )
    supporting_quote: Optional[str] = Field(
        default=None,
        description="Verbatim quote from the Candidate text that supports the decision.",
    )
    same_event: bool = Field(
        description="True if the candidate refers to the exact same real-world incident as the reference."
    )
    confidence_score: float = Field(
        ge=0.0,
        le=1.0,
        description="Confidence in the verdict: 1.0 is absolutely sure, 0.0 is complete guess.",
    )
    key_matches: Dict[str, str] = Field(
        default_factory=dict,
        description="Comparison of key dimensions like actors, location, and time (e.g., {'actors': 'Match', 'time': 'Mismatch'}).",
    )
    discrepancies: Optional[str] = Field(
        default=None,
        description="Description of any conflicting information between the two sources.",
    )

    @field_validator("discrepancies", mode="before")
    @classmethod
    def parse_discrepancies(cls, v):
        if isinstance(v, list):
            return "; ".join(map(str, v))
        return v

    @field_validator("key_matches", mode="before")
    @classmethod
    def validate_key_matches(cls, v):
        if v is None:
            return {}
        if isinstance(v, dict):
            return {k: (str(val) if val is not None else "N/A") for k, val in v.items()}
        return v


class BatchMatchResult(BaseModel):
    proposal_id: str = Field(description="Unique identifier of the merge proposal.")
    reasoning: str = Field(description="Explanation for the decision.")
    same_event: bool = Field(description="Verdict: True if it's the same event.")
    confidence_score: float = Field(
        ge=0.0, le=1.0, description="Confidence in the verdict."
    )


class BatchMatchResponse(BaseModel):
    results: List[BatchMatchResult] = Field(
        description="List of verdicts for the batch of candidates."
    )


class EventSummaryInput(BaseModel):
    title: str = Field(description="Factually descriptive headline of the event.")
    subtitle: str = Field(description="Current contextual subtitle of the event.")
    summary: Dict[str, str] = Field(
        description="Consolidated summary sections: 'left', 'right', 'center', and 'bias' analysis. What they are talking about? What is the focus?"
    )
    impact_reasoning: str = Field(
        description="Explanation for the impact score choice."
    )
    impact_score: int = Field(
        ge=0,
        le=100,
        description="Societal impact score from 0 (Irrelevant) to 100 (Historic/Global).",
    )

    @field_validator("summary", mode="before")
    @classmethod
    def parse_summary(cls, v):
        if isinstance(v, dict):
            return {
                k: ("\n".join(val) if isinstance(val, list) else val)
                for k, val in v.items()
            }
        return v


class EventSummaryOutput(EventSummaryInput):
    is_international: bool | None = Field(
        default=None,
        description="True if the event takes place primarily outside of Brazil.",
    )
    primary_category: CategoryEnum | None = Field(
        default=None, description="The high-level category that best fits the event."
    )

    @field_validator("primary_category", mode="before")
    @classmethod
    def parse_category(cls, v):
        if isinstance(v, str):
            for member in CategoryEnum:
                if member.value.lower() == v.lower():
                    return member
        return CategoryEnum.OTHER


class FilterResponse(BaseModel):
    ids: List[int] = Field(
        description="List of integer indices of the articles that passed the filter."
    )


class MergeSummaryResponse(BaseModel):
    title: str = Field(description="Final consolidated headline.")
    subtitle: Optional[str] = Field(
        default=None, description="Final consolidated subtitle."
    )
    summary: Dict[str, Any] = Field(
        description="Final consolidated multi-perspective summary."
    )
