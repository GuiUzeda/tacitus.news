import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import aiohttp

# Imports
from app.config import Settings
from app.workers.harvester.domain import HarvestingDomain
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    AuditLogModel,
    FeedModel,
    JobStatus,
    NewspaperModel,
)
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


class HarvesterWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(
            str(self.settings.pg_dsn), pool_size=10, pool_timeout=30
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # Instantiate Domain (manages ProcessPool)
        self.domain = HarvestingDomain()
        self.worker_id = str(uuid.uuid4())[:8]

    async def run(self):
        logger.info(f"🌾 Harvester Worker Started. ID: {self.worker_id}")

        while True:
            cycle_start_time = datetime.now()
            try:
                # 1. Fetch Configuration (IO Bound)
                newspapers = self._get_active_newspapers()
                if not newspapers:
                    logger.warning(
                        f"[{self.worker_id}] No active newspapers found. Sleeping..."
                    )
                    await asyncio.sleep(300)
                    continue

                logger.info(
                    f"[{self.worker_id}] Starting Cycle for {len(newspapers)} newspapers..."
                )

                # 2. Shared HTTP Session for the cycle
                async with aiohttp.ClientSession() as http_session:

                    # 3. Process Parallelly (IO Bound)
                    # We limit concurrent newspapers to avoid overloading DB connections or network
                    sem = asyncio.Semaphore(5)

                    tasks = [
                        self._process_wrapper(sem, http_session, np)
                        for np in newspapers
                    ]
                    await asyncio.gather(*tasks)

                logger.success(f"[{self.worker_id}] Cycle Finished.")

                # Sleep to align with hourly schedule (1 hour after START)
                elapsed = (datetime.now() - cycle_start_time).total_seconds()
                sleep_duration = max(0, 3600 - elapsed)
                logger.info(
                    f"[{self.worker_id}] Sleeping {sleep_duration:.0f}s to align with hourly schedule..."
                )
                await asyncio.sleep(sleep_duration)

            except Exception as e:
                logger.critical(f"[{self.worker_id}] Harvester Cycle Crashed: {e}")
                await asyncio.sleep(60)

    async def _process_wrapper(self, sem, http_session, np_data):
        async with sem:
            logger.info(
                f"[{self.worker_id}] [{np_data['name']}]  Processing Newspaper..."
            )

            # We open a NEW DB session per newspaper to keep transactions short
            # and avoid "idle in transaction" issues during long http requests

            # Domain now returns a HarvestResult object
            harvest_result = await self.domain.process_newspaper(http_session, np_data)
            articles = harvest_result.articles

            with self.SessionLocal() as session:
                try:
                    saved_articles = self._bulk_save_articles(session, articles)

                    # --- METRICS LOGGING ---
                    saved_count = len(saved_articles)
                    fetched = harvest_result.total_fetched
                    old = harvest_result.filtered_date
                    dupes = harvest_result.filtered_hash
                    blocked = harvest_result.filtered_block
                    err_count = len(harvest_result.errors)

                    log_msg = (
                        f"[{self.worker_id}] [{np_data['name']}] Report: "
                        f"Fetched={fetched} | Old={old} | Pre-Dupes={dupes} | "
                        f"Blocked={blocked} | Errors={err_count} | Saved={saved_count}"
                    )

                    if err_count > 0:
                        logger.warning(
                            f"[{self.worker_id}] [{np_data['name']}] Errors encountered: {harvest_result.errors}"
                        )

                    if saved_count > 0:
                        self._queue_articles_bulk(session, saved_articles)
                        session.commit()
                        logger.success(log_msg)
                    else:
                        session.rollback()
                        # Differentiate between "Empty/Failed" and "Just no new content"
                        if fetched == 0 and err_count > 0:
                            logger.error(f"{log_msg} (FAILED)")
                        elif fetched == 0:
                            logger.warning(f"{log_msg} (EMPTY FEED)")
                        else:
                            logger.info(f"{log_msg} (NO NEW CONTENT)")

                except Exception as e:
                    logger.error(
                        f"[{self.worker_id}] [{np_data['name']}] Error during bulk save or queue: {e}"
                    )
                    session.rollback()

                    # Log to Audit Table
                    try:
                        audit_log = AuditLogModel(
                            id=uuid.uuid4(),
                            source="Harvester",
                            event="Batch Failed",
                            object_id=None,
                            details=f"Newspaper: {np_data['name']} - Error: {str(e)[:250]}",
                            created_at=datetime.now(timezone.utc),
                        )
                        session.add(audit_log)
                        session.commit()
                    except Exception as audit_err:
                        logger.error(f"Failed to save audit log: {audit_err}")

    def _get_active_newspapers(self) -> List[dict]:
        with self.SessionLocal() as session:
            cutoff = datetime.now() - timedelta(days=60)

            # 1. Get Newspapers
            q = (
                select(NewspaperModel)
                .join(FeedModel)
                .where(FeedModel.is_active)
                .distinct()
            )
            newspapers = session.execute(q).scalars().all()

            if not newspapers:
                return []
            ids = [n.id for n in newspapers]

            # 2. Get Feeds
            feeds = (
                session.execute(
                    select(FeedModel).where(
                        FeedModel.newspaper_id.in_(ids), FeedModel.is_active
                    )
                )
                .scalars()
                .all()
            )

            # 3. Get Hashes
            hashes = session.execute(
                select(ArticleModel.newspaper_id, ArticleModel.url_hash).where(
                    ArticleModel.newspaper_id.in_(ids),
                    ArticleModel.published_date >= cutoff,
                )
            ).all()

            feed_map = {i: [] for i in ids}
            for f in feeds:
                feed_map[f.newspaper_id].append(
                    {
                        "url": f.url,
                        "feed_type": f.feed_type,
                        "url_pattern": f.url_pattern,
                        "blocklist": f.blocklist,
                        "allowed_sections": f.allowed_sections,
                        # Pass the flags down
                        "is_ranked": f.is_ranked,
                        "use_browser_render": f.use_browser_render,
                        "scroll_depth": f.scroll_depth,
                    }
                )

            hash_map = {i: set() for i in ids}
            for nid, h in hashes:
                hash_map[nid].add(h)

            return [
                {
                    "id": n.id,
                    "name": n.name,
                    "feeds": feed_map[n.id],
                    "article_hashes": hash_map[n.id],
                }
                for n in newspapers
            ]

    def _bulk_save_articles(
        self, session, articles_data: List[dict]
    ) -> List[ArticleModel]:
        if not articles_data:
            return []

        # Pre-fetch existing hashes to filter duplicates
        hashes = [d["hash"] for d in articles_data if d.get("hash")]
        existing_hashes = set()
        if hashes:
            existing_hashes = set(
                session.scalars(
                    select(ArticleModel.url_hash).where(
                        ArticleModel.url_hash.in_(hashes)
                    )
                ).all()
            )

        valid_models = []
        skipped_count = 0
        for data in articles_data:
            h = data.get("hash")
            if h in existing_hashes:
                skipped_count += 1
                continue

            # If content is "Unknown" or empty, we save empty list to be enriched later
            # If RSS provided content, we save it.

            article = ArticleModel(
                title=data["title"][:500] if data["title"] else "Unknown",
                original_url=data["link"],
                url_hash=data["hash"],
                summary=data["summary"],
                published_date=data.get("published"),
                newspaper_id=data["newspaper_id"],
                authors=[],
                entities=[],
                interests={},
                embedding=None,
                summary_status=JobStatus.PENDING,
                summary_date=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                source_rank=data.get("rank"),
                content=data.get("content"),
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
            logger.warning(
                f"Bulk save failed (IntegrityError). Retrying one-by-one... {e}"
            )
            session.rollback()
            # Fallback: Save one by one to isolate duplicates
            saved = []
            for model in valid_models:
                try:
                    session.add(model)
                    session.commit()
                    session.refresh(
                        model
                    )  # Ensure model is fresh and usable for queueing
                    saved.append(model)
                except IntegrityError as ie:
                    session.rollback()
                    if hasattr(ie.orig, "pgcode") and ie.orig.pgcode == "23505":
                        # 🔇 IGNORE: It's just a duplicate, no need to error log
                        logger.info(f"Skipped duplicate: {model.title[:20]}")
                    else:
                        # 🚨 REAL ERROR: Log this one
                        logger.error(f"Failed to save article: {ie}")
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
            target_queue = (
                ArticlesQueueName.ENRICHER
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


if __name__ == "__main__":
    from multiprocessing import freeze_support

    freeze_support()

    worker = HarvesterWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
