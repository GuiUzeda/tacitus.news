from functools import partial
from fastapi import APIRouter

from fastapi_filter import FilterDepends
from fastapi_pagination import Page
from app.api.v1.schemas.articles import NewsArticleReadSchema

from app.common.utils import schema_transformer
from app.services.articles import ArticlesService
from functools import partial
from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi_pagination import Page

from sqlalchemy.orm import Session
from app.core.session import db_session
from app.common.utils import  schema_transformer
from app.api.v1.filters.articles import ArticlesFilter


router = APIRouter()


@router.get("/", response_model=Page[NewsArticleReadSchema])
async def get_articles(
    session: Annotated[Session, Depends(db_session)],
    event_id: str,
     filter: Annotated[ArticlesFilter, FilterDepends(ArticlesFilter)],
) -> Page[NewsArticleReadSchema]:
    articles_service = ArticlesService(session)
    articles = await articles_service.get_articles(event_id, filter,
        transformer=partial(schema_transformer, model_type=NewsArticleReadSchema)
    )

    return articles
