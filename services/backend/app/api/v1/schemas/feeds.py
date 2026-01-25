import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict


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
