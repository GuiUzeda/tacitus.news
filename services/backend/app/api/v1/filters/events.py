from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import Query
from fastapi_filter.contrib.sqlalchemy import Filter
from news_events_lib.models import EventStatus, NewsEventModel


class EventsFilter(Filter):
    id: Optional[UUID] = Query(None)
    search: Optional[str] = Query(None)
    is_active: Optional[bool] = Query(None)
    status: Optional[EventStatus] = Query(None)
    article_count: Optional[int] = Query(None)
    last_updated_at__lte: Optional[datetime] = Query(None)
    last_updated_at__gte: Optional[datetime] = Query(None)
    created_at__lte: Optional[datetime] = Query(None)
    created_at__gte: Optional[datetime] = Query(None)
    is_blind_spot: Optional[bool] = Query(None)
    blind_spot_side: Optional[str] = Query(None)
    order_by: Optional[list[str]] = Query(None)

    class Constants(Filter.Constants):
        model = NewsEventModel
        search_model_fields = ["title", "summary"]
