import asyncio
import hashlib
from os import wait
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, AsyncGenerator

import aiohttp
from loguru import logger
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import select

# Project Imports
from harvesters.base import BaseHarvester
from news_events_lib.models import (
    ArticleContentModel,
    ArticleModel,
    JobStatus,
)
from core.models import ArticlesQueueModel, ArticlesQueueName
from harvesters.factory import HarvesterFactory
from config import Settings

# --- DOMAIN CLASS ---


class HarvestingDomain:

    def __init__(self):
        self.settings = Settings()

        self.factory = HarvesterFactory()

    async def process_newspaper(
        self,
        session: Session,
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
            total_saved = 0
            all_articles = await self._run_pipeline(
                http_session,
                feeds,
                harvester_instance,
                name,
                newspaper_data["id"],
                newspaper_data["article_hashes"],
            )

            logger.info(f"[{name}] Extracted {len(all_articles)} items from feeds.")

            if all_articles:
                saved_objs = self._bulk_save_articles(session, all_articles)
                if saved_objs:
                    self._queue_articles_bulk(session, saved_objs)
                    session.commit()
                    total_saved = len(saved_objs)

            return total_saved

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

            results.append({
                "title": entry.get("title"),
                "link": clean_link,
                "hash": link_hash,
                "published": entry.get("published"),
                "summary": entry.get("summary"),  # Might be None
                "content": entry.get(
                    "content"
                ),  # Might be "Unknown" or full text from RSS
                "newspaper_id": np_id,
                "rank": entry.get("rank"),
            })

        return results

    def _sanitize_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Ensures entry data is pickle-safe for multiprocessing"""
        clean = entry.copy()
        pub = clean.get("published")
        if pub and hasattr(pub, "group"):  # Fix re.Match objects from Regex
            clean["published"] = pub.group(0)
        return clean

    def _bulk_save_articles(
        self, session, articles_data: List[dict]
    ) -> List[ArticleModel]:
        if not articles_data:
            return []

        # Pre-fetch existing hashes to filter duplicates
        hashes = [d["hash"] for d in articles_data if d.get("hash")]
        existing_hashes = set()
        if hashes:
            existing_hashes = set(session.scalars(
                select(ArticleModel.url_hash).where(ArticleModel.url_hash.in_(hashes))
            ).all())

        valid_models = []
        skipped_count = 0
        for data in articles_data:
            h = data.get("hash")
            if h in existing_hashes:
                skipped_count += 1
                continue

            # If content is "Unknown" or empty, we save empty list to be enriched later
            # If RSS provided content, we save it.
            contents = []
            if data.get("content") and data["content"] != "Unknown":
                contents = [ArticleContentModel(content=data["content"])]

            dt = self._parse_date(data.get("published"))
            article = ArticleModel(
                title=data["title"][:500] if data["title"] else "Unknown",
                original_url=data["link"],
                url_hash=data["hash"],
                summary=data["summary"],
                published_date=dt,
                newspaper_id=data["newspaper_id"],
                authors=[],
                contents=contents,
                entities=[],
                interests={},
                embedding=None,
                summary_status=JobStatus.PENDING,
                summary_date=datetime.now(timezone.utc),
                # UPDATED: Save the Editorial Rank
                source_rank=data.get("rank"),
            )
            valid_models.append(article)

        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} duplicates (pre-check).")

        if not valid_models:
            return []

        try:
            session.add_all(valid_models)
            session.flush()
            return valid_models
        except IntegrityError as e:
            logger.warning(f"Bulk save failed (IntegrityError). Retrying one-by-one... {e}")
            session.rollback()
            # Fallback: Save one by one to isolate duplicates
            saved = []
            for model in valid_models:
                try:
                    session.add(model)
                    session.commit()
                    saved.append(model)
                except IntegrityError:
                    session.rollback()
            logger.info(f"Recovered {len(saved)} articles via fallback.")
            return saved
        except Exception as e:
            logger.error(f"Save Error: {e}")
            session.rollback()
            return []

    def _queue_articles_bulk(self, session, articles: List[ArticleModel]):
        if not articles:
            return
        now = datetime.now(timezone.utc)
        queue_data = []
        for a in articles:
            if not a.id:
                continue

            # If title is missing, skip Filter and go straight to Enricher
            target_queue = ArticlesQueueName.ENRICH if a.title.lower() in [ "no title", "unknown"] else ArticlesQueueName.FILTER

            queue_data.append({
                "article_id": a.id,
                "status": JobStatus.PENDING,
                "queue_name": target_queue,
                "created_at": now,
                "updated_at": now,
                "attempts": 0,
            })

        if queue_data:
            stmt = (
                insert(ArticlesQueueModel).values(queue_data).on_conflict_do_nothing()
            )
            session.execute(stmt)

    def _parse_date(self, pub_data):
        if not pub_data:
            return datetime.now(timezone.utc)
        try:
            if isinstance(pub_data, str):
                return datetime.fromisoformat(pub_data.replace("Z", "+00:00"))
            return pub_data
        except:
            return datetime.now(timezone.utc)
