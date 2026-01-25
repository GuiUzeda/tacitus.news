import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict


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
