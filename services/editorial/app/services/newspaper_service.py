from typing import Any, Dict, List

from app.services.base_service import BaseService
from news_events_lib.models import AuthorModel, FeedModel, NewspaperModel


class NewspaperService(BaseService):
    """
    Service for Newspapers, Feeds, and Authors.
    Domain Model: NewspaperModel
    """

    def search(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(NewspaperModel, filters, limit, offset)

    def search_feeds(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(FeedModel, filters, limit, offset)

    def search_authors(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(AuthorModel, filters, limit, offset)
