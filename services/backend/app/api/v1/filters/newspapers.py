from typing import Optional

from fastapi import Query
from fastapi_filter.contrib.sqlalchemy import Filter
from news_events_lib.models import NewspaperModel


class NewspaperFilter(Filter):
    bias: Optional[str] = Query(None)

    class Constants(Filter.Constants):
        model = NewspaperModel
        search_model_fields = ["name", "description"]
