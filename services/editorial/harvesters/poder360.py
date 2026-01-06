from functools import partial
import re
from datetime import datetime, timedelta
from typing import Optional

from bs4 import BeautifulSoup
from .base import BaseHarvester
from loguru import logger


class Poder360Harvester(BaseHarvester):
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=24),
    ):
        super().__init__(cutoff)

    async def harvest(
        self, session, sources
    ) -> list[dict]:

        url_harvesters = {
            "https://www.poder360.com.br/sitemap_index.xml": partial(
                self.harvest_latest_id,
                id_pattern=r"https://www\.poder360\.com\.br/post-sitemap(\d*)\.xml",
            ),
        }
        articles =[]
        for source in sources:
            harvester = url_harvesters.get(source["url"], super()._fetch)
            articles.extend(await harvester(session, source["url"], blocklist=source["blocklist"], allowed_sections=source["allowed_sections"]))
        return articles