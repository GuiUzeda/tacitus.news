from typing import Optional
from uuid import UUID
from fastapi import Query
from fastapi_filter.contrib.sqlalchemy import Filter
from news_events_lib.models import ArticleModel

from datetime import datetime

from app.api.v1.filters.newspapers import NewspaperFilter 

class ArticlesFilter(Filter):

    
    published_date__lte: Optional[datetime] = Query(None)
    published_date__gte: Optional[datetime] = Query(None)
    newspaper: Optional[NewspaperFilter] = Query(None)

    
    
    class Constants(Filter.Constants):
        model= ArticleModel
        search_model_fields = ["title", "summary"]