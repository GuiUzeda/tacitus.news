from functools import partial
from typing import Any, List, Optional

from fastapi_pagination import Page

from app.common.mixins import DBSessionMixin
from app.managers.articles import ArticlesManager
from fastapi_filter.contrib.sqlalchemy import Filter
from fastapi_pagination.types import AsyncItemsTransformer
from sqlalchemy.orm import Session

from news_events_lib.schemas import NewsArticleReadSchema


class ArticlesService(DBSessionMixin):
    """Manages article related operations."""

    def __init__(self, db_session: Session) -> None:
        super().__init__(db_session=db_session)

    async def get_articles(
        self,
        event_id: str,
        filter: Filter,
        transformer: Optional[partial[List[Any]]] = None,
    ) -> Page[NewsArticleReadSchema]:
        articles_manager = ArticlesManager(self.db_session)
        return await articles_manager.get_articles(event_id, filter, transformer=transformer)
