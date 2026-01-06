from functools import partial
from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi_filter import FilterDepends
from fastapi_pagination import Page

from news_events_lib.schemas import NewsEventReadSchema
from sqlalchemy.orm import Session
from app.core.session import db_session
from app.api.v1.filters.events import EventsFilter
from app.services.events import EventsService
from app.common.utils import aschema_transformer

router = APIRouter()


@router.get("/", response_model=Page[NewsEventReadSchema])
async def get_events(
    session: Annotated[Session, Depends(db_session)],
    events_filter: Annotated[EventsFilter, FilterDepends(EventsFilter)],
) -> Page[NewsEventReadSchema]:
    events_service = EventsService(db_session=session)
    news_events = await events_service.get_events(events_filter, transformer=partial(aschema_transformer, model_type=NewsEventReadSchema))
    

    return news_events
