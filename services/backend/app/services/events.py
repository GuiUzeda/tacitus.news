from typing import Optional
from app.common.mixins import DBSessionMixin
from sqlalchemy.orm import Session
from fastapi_filter.contrib.sqlalchemy import Filter
from fastapi_pagination.types import (
    AsyncItemsTransformer,
)


from app.managers.events import EventsManager


class EventsService(DBSessionMixin):
    """Manages event related operations."""

    def __init__(self, db_session: Session) -> None:
        super().__init__(db_session=db_session)

    async def get_events(
        self,
        filter: Filter,
        transformer: Optional[AsyncItemsTransformer] = None,
    ):
        events_manager = EventsManager(self.db_session)
        return await events_manager.get_events(filter, transformer)
