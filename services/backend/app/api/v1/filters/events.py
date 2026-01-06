from typing import Optional
from uuid import UUID
from fastapi import Query
from fastapi_filter.contrib.sqlalchemy import Filter
from news_events_lib.models import NewsEventModel, ArticleModel, NewspaperModel, FeedModel, BaseModel

from datetime import datetime 

class EventsFilter(Filter):
    id: Optional[UUID] = Query(None)
    search: Optional[str] = Query(None)
    is_active: Optional[bool] = Query(None)
    created_at__lte: Optional[datetime] = Query(None)
    created_at__gte: Optional[datetime] = Query(None)
    order_by: Optional[list[str]] = Query(None)
    
    
    class Constants(Filter.Constants):
        model= NewsEventModel
        search_model_fields = ["title", "summary"]