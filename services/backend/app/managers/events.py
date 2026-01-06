from typing import Optional
from uuid import UUID
from app.managers.base import BaseManager

from fastapi_filter.contrib.sqlalchemy import Filter
from news_events_lib.models import NewsEventModel
from sqlalchemy.orm import Session
from sqlalchemy import select
from fastapi_pagination.types import (
    AsyncItemsTransformer,
)

class EventsManager(BaseManager):
    def __init__(self, db_session, **kwargs):
        super().__init__(db_session, **kwargs)
        
    async def get_events(self, filter: Filter, transformer: Optional[AsyncItemsTransformer] = None,):
        stmt = select(NewsEventModel)
        stmt = filter.filter(stmt)
        stmt = filter.sort(stmt)

        return self.aget_paginated(stmt, transformer)
        
        
        
