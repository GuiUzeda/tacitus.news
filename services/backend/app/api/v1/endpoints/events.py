from functools import partial
from typing import Annotated

from app.api.v1.filters.events import EventsFilter
from app.api.v1.schemas.events import NewsEventListSchema, NewsEventReadSchema
from app.common.utils import schema_transformer
from app.core.session import db_session
from app.services.events import EventsService
from fastapi import APIRouter, Depends
from fastapi_filter import FilterDepends
from fastapi_pagination import Page
from sqlalchemy.orm import Session

router = APIRouter()


@router.get("/", response_model=Page[NewsEventListSchema])
async def get_events(
    session: Annotated[Session, Depends(db_session)],
    filter: Annotated[EventsFilter, FilterDepends(EventsFilter)],
) -> Page[NewsEventListSchema]:
    events_service = EventsService(db_session=session)
    news_events = events_service.get_events(
        filter,
        transformer=partial(schema_transformer, model_type=NewsEventListSchema),
    )

    return news_events


@router.get("/{event_id}", response_model=NewsEventReadSchema)
async def get_event(
    event_id: str,
    session: Annotated[Session, Depends(db_session)],
) -> NewsEventReadSchema:
    events_service = EventsService(db_session=session)
    news_event = events_service.get_event(event_id=event_id)

    return news_event


@router.get("/search", response_model=Page[NewsEventReadSchema])
async def search_events(
    session: Annotated[Session, Depends(db_session)],
    query: str,
) -> Page[NewsEventReadSchema]:
    events_service = EventsService(db_session=session)
    news_events = await events_service.search_events(
        query=query,
        transformer=partial(schema_transformer, model_type=NewsEventReadSchema),
    )

    return news_events
