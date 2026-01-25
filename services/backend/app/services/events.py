from functools import partial
from typing import Any, List, Optional

from app.api.v1.schemas.events import NewsEventListSchema, NewsEventReadSchema
from app.common.mixins import DBSessionMixin
from app.managers.events import EventsManager
from fastapi_filter.contrib.sqlalchemy import Filter
from fastapi_pagination import Page
from sqlalchemy.orm import Session


class EventsService(DBSessionMixin):
    """Manages event related operations."""

    def __init__(self, db_session: Session) -> None:
        super().__init__(db_session=db_session)

    def get_events(
        self,
        filter: Filter,
        transformer: Optional[partial[List[Any]]] = None,
    ) -> Page[NewsEventListSchema]:
        events_manager = EventsManager(self.db_session)
        return events_manager.get_events(filter, transformer)

    def get_event(self, event_id: str) -> NewsEventReadSchema:
        events_manager = EventsManager(self.db_session)
        return events_manager.get_event(event_id)

    def search_events(
        self,
        query: str,
        transformer: Optional[partial[List[Any]]] = None,
    ) -> Page[NewsEventReadSchema]:
        events_manager = EventsManager(self.db_session)
        return events_manager.search_events(query=query, transformer=transformer)
