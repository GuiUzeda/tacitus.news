from functools import partial
from typing import Any, List, Optional

from app.api.v1.schemas.articles import NewsArticleReadSchema
from app.managers.base import BaseManager
from fastapi_filter.contrib.sqlalchemy import Filter
from fastapi_pagination import Page
from news_events_lib.models import ArticleModel
from sqlalchemy import select


class ArticlesManager(BaseManager):
    def __init__(self, db_session, **kwargs):
        super().__init__(db_session, **kwargs)

    async def get_articles(
        self,
        event_id: str,
        filter: Filter,
        transformer: Optional[partial[List[Any]]] = None,
    ) -> Page[NewsArticleReadSchema]:

        stmt = select(ArticleModel).where(ArticleModel.event_id == event_id)
        stmt = filter.filter(stmt)
        stmt = filter.sort(stmt)

        return await self.aget_paginated(stmt, transformer=transformer)
