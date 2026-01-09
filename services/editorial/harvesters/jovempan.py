from functools import partial
import re
from datetime import datetime, timedelta
from typing import Optional

from bs4 import BeautifulSoup
from .base import BaseHarvester
from loguru import logger
import feedparser


class JovemPanHarvester(BaseHarvester):
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=24),
    ):
        super().__init__(cutoff)

    async def harvest(self, session, sources) -> list[dict]:

        url_harvesters = {
            "https://jovempan.com.br/feed": partial(
                self.harvest_rss,
            ),
        }
        articles = []
        for source in sources:
            harvester = url_harvesters.get(source["url"], super()._fetch)
            articles.extend(
                await harvester(
                    session= session,
                    url=source["url"],
                    blocklist=source.get("blocklist"),
                    allowed_sections=source.get("allowed_sections"),
                )
            )
        return articles
