import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp
from app.config import Settings
from app.harvesters.base import HarvestResult
from app.harvesters.factory import HarvesterFactory
from dateutil import parser
from loguru import logger

# Project Imports

# --- DOMAIN CLASS ---


class HarvestingDomain:

    def __init__(self):
        self.settings = Settings()

        self.factory = HarvesterFactory()

    async def process_newspaper(
        self,
        http_session: aiohttp.ClientSession,
        newspaper_data: dict,
    ) -> HarvestResult:
        """
        Orchestrates the pipeline for a SINGLE newspaper.
        """
        try:
            name = newspaper_data["name"]
            # UPDATED: Pass the full feed objects (with patterns/flags)
            raw_feeds = newspaper_data["feeds"]

            # Validate Regex Patterns to prevent crashes in Harvester Base
            feeds = []
            for feed in raw_feeds:
                pattern = feed.get("url_pattern")
                if pattern:
                    try:
                        re.compile(pattern)
                        feeds.append(feed)
                    except re.error as e:
                        logger.error(
                            f"[{name}] Skipping Feed {feed.get('url')} due to Invalid Regex: {e}"
                        )
                else:
                    feeds.append(feed)

            # logger.info(f"[{name}] Harvesting {len(feeds)} feeds: {feed_list}")

            harvester_instance = self.factory.get_harvester(name)
            # 1. Pipeline Execution

            result = await self._run_pipeline(
                http_session,
                feeds,
                harvester_instance,
                name,
                newspaper_data["id"],
                newspaper_data["article_hashes"],
            )

            # logger.info(f"[{name}] Extracted {len(result.articles)} items from feeds.")
            return result

        except Exception as e:
            logger.error(f"[{newspaper_data['name']}] Pipeline Failed: {e}")
            # Return an empty result with the error
            res = HarvestResult()
            res.errors.append(str(e))
            return res

    # --- INTERNAL PIPELINE STEPS ---

    async def _run_pipeline(
        self, http_session, feeds, harvester, name, np_id, ignore_hashes
    ) -> HarvestResult:
        # A. Fetch Links (Uses the new BaseHarvester with Browser/Regex support)
        try:
            # Pass ignore_hashes to filter at source (BaseHarvester)
            # This returns a HarvestResult object
            harvest_res = await harvester.harvest(http_session, feeds, ignore_hashes)
        except Exception as e:
            logger.error(f"[{name}] Link fetch failed: {e}")
            res = HarvestResult()
            res.errors.append(str(e))
            return res

        if not harvest_res.articles:
            return harvest_res

        results = []
        # B. Yield entries for saving (Enrichment happens in a separate worker now)
        for entry in harvest_res.articles:
            clean_link = entry["link"]
            link_hash = entry.get("hash")
            if not link_hash:
                link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()

            results.append(
                {
                    "title": entry.get("title"),  # Might be "Unknown"
                    "link": clean_link,
                    "hash": link_hash,
                    "published": self._parse_date(
                        entry.get("published")
                    ),  # Might be None
                    "summary": entry.get("summary"),  # Might be None
                    "content": entry.get(
                        "content"
                    ),  # Might be "Unknown" or full text from RSS
                    "newspaper_id": np_id,
                    "rank": entry.get("rank"),
                }
            )

        # Replace raw articles with processed ones
        harvest_res.articles = results
        return harvest_res

    def _sanitize_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Ensures entry data is pickle-safe for multiprocessing"""
        clean = entry.copy()
        pub = clean.get("published")
        if pub and hasattr(pub, "group"):  # Fix re.Match objects from Regex
            clean["published"] = pub.group(0)
        return clean

    def _parse_date(self, pub_data) -> Optional[datetime]:
        now = datetime.now(timezone.utc)

        try:
            dt = parser.parse(pub_data, dayfirst=True)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            # Future Guard: If date is > 1 hour in future, clamp to now
            if dt > now:
                return None
            return dt
        except Exception:
            return None
