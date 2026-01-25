from functools import partial
from typing import Any, List, Optional

from app.api.v1.schemas.articles import NewsArticleReadSchema
from app.common.mixins import DBSessionMixin
from app.managers.articles import ArticlesManager
from fastapi_filter.contrib.sqlalchemy import Filter
from fastapi_pagination import Page
from sqlalchemy.orm import Session


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
        return await articles_manager.get_articles(
            event_id, filter, transformer=transformer
        )
