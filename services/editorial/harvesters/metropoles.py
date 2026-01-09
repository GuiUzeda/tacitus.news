from .base import BaseHarvester
from functools import partial
from datetime import timedelta

class MetropolesHarvester(BaseHarvester):
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=24),
    ):
        super().__init__(cutoff)
    async def harvest(self, session, sources)-> list[dict]:
        url_harvesters = {
            "https://www.metropoles.com/sitemap.xml": partial(
                self.harvest_latest_date,
                date_pattern=r"https://www\.metropoles\.com/sitemap/noticias-(\d{4}-\d{2}-\d{2})\.xml",
            ),
        }


        articles = []
        for source in sources:
            harvester = url_harvesters.get(source["url"], super()._fetch)
            articles.extend(
                await harvester(
                    session=session,
                    url=source["url"],
                    blocklist=source.get("blocklist"),
                    allowed_sections=source.get("allowed_sections"),
                )
            )
        return articles
