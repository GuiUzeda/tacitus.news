from .base import BaseHarvester
from functools import partial
from datetime import timedelta, datetime


class MetropolesHarvester(BaseHarvester):
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=24),
    ):
        super().__init__(cutoff)

    async def fetch_feed_articles(self, session, source, ignore_hashes: set = set()) -> list[dict]:
        url = source["url"]
        if url == "https://www.metropoles.com/sitemap/noticias-{{today}}.xml":
            today = datetime.now().strftime("%Y-%m-%d")
            today_url = url.format(
                today=today
            )
            
            source["url"] = today_url
            return await super().fetch_feed_articles(session, source, ignore_hashes)
        return await super().fetch_feed_articles(session, source, ignore_hashes)