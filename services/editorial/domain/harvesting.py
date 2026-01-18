import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from config import Settings
from core.models import ArticlesQueueModel, ArticlesQueueName
from harvesters.factory import HarvesterFactory
from loguru import logger

# Project Imports
from news_events_lib.models import ArticleModel, JobStatus
from sqlalchemy.dialects.postgresql import insert
from dateutil import parser

# --- DOMAIN CLASS ---


class HarvestingDomain:

    def __init__(self):
        self.settings = Settings()

        self.factory = HarvesterFactory()

    async def process_newspaper(
        self,
        http_session: aiohttp.ClientSession,
        newspaper_data: dict,
    ):
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

            feed_list = ", ".join([f.get("url") for f in feeds])
            logger.info(f"[{name}] 🚀 Harvesting {len(feeds)} feeds: {feed_list}")

            harvester_instance = self.factory.get_harvester(name)
            # 1. Pipeline Execution

            all_articles = await self._run_pipeline(
                http_session,
                feeds,
                harvester_instance,
                name,
                newspaper_data["id"],
                newspaper_data["article_hashes"],
            )

            logger.info(f"[{name}] Extracted {len(all_articles)} items from feeds.")
            return all_articles

        except Exception as e:
            logger.error(f"[{newspaper_data['name']}] Pipeline Failed: {e}")
            raise e

    # --- INTERNAL PIPELINE STEPS ---

    async def _run_pipeline(
        self, http_session, feeds, harvester, name, np_id, ignore_hashes
    ) -> List[Dict[str, Any]]:
        # A. Fetch Links (Uses the new BaseHarvester with Browser/Regex support)
        try:
            # Pass ignore_hashes to filter at source (BaseHarvester)
            raw_entries = await harvester.harvest(http_session, feeds, ignore_hashes)
        except Exception as e:
            logger.error(f"[{name}] Link fetch failed: {e}")
            return []

        if not raw_entries:
            return []

        results = []
        # B. Yield entries for saving (Enrichment happens in a separate worker now)
        for entry in raw_entries:
            clean_link = entry["link"]
            link_hash = entry.get("hash")
            if not link_hash:
                link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()

            results.append(
                {
                    "title": entry.get("title"),
                    "link": clean_link,
                    "hash": link_hash,
                    "published": self._parse_date(entry.get("published")),
                    "summary": entry.get("summary"),  # Might be None
                    "content": entry.get(
                        "content"
                    ),  # Might be "Unknown" or full text from RSS
                    "newspaper_id": np_id,
                    "rank": entry.get("rank"),
                }
            )

        return results

    def _sanitize_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Ensures entry data is pickle-safe for multiprocessing"""
        clean = entry.copy()
        pub = clean.get("published")
        if pub and hasattr(pub, "group"):  # Fix re.Match objects from Regex
            clean["published"] = pub.group(0)
        return clean

    def queue_articles_bulk(self, session, articles: List[ArticleModel]):
        if not articles:
            return
        now = datetime.now(timezone.utc)
        queue_data = []
        for a in articles:
            if not a.id:
                continue

            # If title is missing, skip Filter and go straight to Enricher
            target_queue = (
                ArticlesQueueName.ENRICH
                if a.title.lower() in ["no title", "unknown"]
                else ArticlesQueueName.FILTER
            )

            queue_data.append(
                {
                    "article_id": a.id,
                    "status": JobStatus.PENDING,
                    "queue_name": target_queue,
                    "created_at": now,
                    "updated_at": now,
                    "attempts": 0,
                }
            )

        if queue_data:
            stmt = (
                insert(ArticlesQueueModel).values(queue_data).on_conflict_do_nothing()
            )
            session.execute(stmt)

    def _parse_date(self, pub_data) -> Optional[datetime]:
        now = datetime.now(timezone.utc)

        try:
            dt = parser.parse(pub_data, dayfirst=True)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            # Future Guard: If date is > 1 hour in future, clamp to now
            if dt > now :
                return None
            return dt
        except:
            return None
