from functools import partial
from typing import Any, List, Optional

from app.exc import raise_with_log
from app.managers.base import BaseManager
from fastapi_filter.contrib.sqlalchemy import Filter
from news_events_lib.models import NewsEventModel
from sqlalchemy import func, select


class EventsManager(BaseManager):
    def __init__(self, db_session, **kwargs):
        super().__init__(db_session, **kwargs)

    def get_events(
        self,
        filter: Filter,
        transformer: Optional[partial[List[Any]]] = None,
    ):
        stmt = select(NewsEventModel)
        stmt = filter.filter(stmt)
        stmt = filter.sort(stmt)

        return self.get_paginated(stmt, transformer=transformer)

    def get_event(self, event_id: str):
        stmt = select(NewsEventModel).where(NewsEventModel.id == event_id)
        event = self.get_one(stmt)
        if not event:

            raise raise_with_log(404, "Event not found")

        return event

    def search_events(
        self,
        query: str,
        transformer: Optional[partial[List[Any]]] = None,
    ):

        stmt = (
            select(NewsEventModel)
            .filter(
                NewsEventModel.search_vector_ts.op("@@")(
                    func.websearch_to_tsquery("portuguese", query)
                )
            )
            .order_by(
                func.ts_rank_cd(
                    NewsEventModel.search_vector_ts,
                    func.websearch_to_tsquery("portuguese", query),
                ).desc(),
                NewsEventModel.last_updated_at.desc(),
            )
        )

        return self.get_paginated(stmt, transformer=transformer)
