from itertools import count
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime, timezone
from typing import List, Tuple, Dict
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, selectinload

# Models
from news_events_lib.models import ArticleModel, JobStatus, ArticleContentModel
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT DOMAIN LOGIC
from domain.enriching import EnrichingDomain


class NewsEnricherWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # Instantiate Domain Logic (Manages ProcessPool)
        self.domain = EnrichingDomain(max_cpu_workers=2, http_concurrency=20)
        self.TARGET_LLM_BATCH = 10  # We want exactly 10 items per LLM call

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.ENRICH,
            batch_size=20,  # Smaller batch size due to heavy processing
            pending_status=JobStatus.PENDING,
        )

    async def run(self):
        logger.info(f"🚀 Smart-Batch Worker started.")
        self.domain.warmup()

        pending_writes = []
        llm_buffer = []

        while True:
            try:
                with self.SessionLocal() as session:
                    # --- 1. FETCH & CPU PHASE ---
                    if len(llm_buffer) < self.TARGET_LLM_BATCH:
                        needed = self.TARGET_LLM_BATCH - len(llm_buffer)
                        fetch_size = max(10, needed * 2)

                        jobs = self._fetch_jobs(session, limit=fetch_size)

                        if jobs:
                            results = await self.domain.run_cpu_enrichment(jobs)

                            for job, res in results:
                                if res["status"] == "success":

                                    if res["status"] == "boomerang":
                                        # Mark for _save_results
                                        pending_writes.append((job, res))
                                    else:
                                        # ✅ Standard Gold: Queue for LLM
                                        llm_buffer.append((job, res))
                                else:
                                    # ❌ Garbage (Old/Blocked): Save immediately
                                    pending_writes.append((job, res))

                        elif not jobs and not llm_buffer and not pending_writes:
                            logger.info("💤 Queue empty. Sleeping...")
                            await asyncio.sleep(30)
                            continue

                    # --- 2. FLUSH TRIGGER ---
                    should_flush_llm = len(llm_buffer) >= self.TARGET_LLM_BATCH
                    if not jobs and llm_buffer:  # Flush leftovers if queue is empty
                        should_flush_llm = True

                    if should_flush_llm:
                        batch_to_process = llm_buffer[: self.TARGET_LLM_BATCH]
                        llm_buffer = llm_buffer[self.TARGET_LLM_BATCH :]

                        # Run LLM (Updates dicts in-place)
                        await self.domain.run_llm_enrichment(batch_to_process)
                        pending_writes.extend(batch_to_process)

                    # --- 3. WRITE PHASE ---
                    if pending_writes:
                        self._save_results(session, pending_writes)
                        pending_writes = []

            except Exception as e:
                logger.critical(f"Worker Loop Error: {e}")
                await asyncio.sleep(5)

    def _fetch_jobs(self, session, limit=None):
        fetch_limit = limit or self.batch_size
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .options(selectinload(ArticleModel.contents))
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .order_by(ArticlesQueueModel.created_at.asc())
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(
            stmt.where(
                ArticleModel.title.in_(["unknown", "no title", "Unknown", "No Title"])
            ).limit(fetch_limit)
        ).all()
        if len(rows) < fetch_limit:
            rows.extend(
                session.execute(
                    stmt.where(
                        ArticleModel.title.not_in(
                            ["unknown", "no title", "Unknown", "No Title"]
                        )
                    ).limit(fetch_limit - len(rows))
                ).all()
            )

        jobs = []
        for queue_item, article in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.article = article
            jobs.append(queue_item)

        session.commit()
        return jobs

    def _save_results(self, session, items: List[Tuple[ArticlesQueueModel, Dict]]):
        """Single function to handle all status transitions."""
        if not items:
            return

        count_success = 0
        count_fail = 0
        count_drops = 0
        count_returns = 0

        for job, res in items:
            # We need to re-attach job to this session if it came from a previous fetch cycle
            # (Though if we keep session open, it's fine. If session recreates, we need session.merge)
            job = session.merge(job)

            status = res["status"]

            if status == "success":
                # Check LLM output
                if "llm_output" in res:
                    llm = res["llm_output"]
                    if llm.status == "valid":
                        self._apply_data(job, res)  # Map dict to model
                        job.status = JobStatus.COMPLETED
                        job.queue_name = ArticlesQueueName.CLUSTER
                        count_success += 1
                    else:
                        logger.debug(f"LLM: {llm.error_message}")
                        job.status = JobStatus.FAILED
                        job.msg = f"LLM: {llm.error_message}"
                        count_fail += 1
                else:
                    # Should not happen unless LLM crashed
                    logger.debug("LLM output missing")
                    job.status = JobStatus.FAILED
                    job.msg = "LLM output missing"
                    count_fail += 1

            elif status == "archived":
                job.status = JobStatus.COMPLETED
                job.msg = res.get("stop_reason")
                count_drops += 1  # Not a technical fail, but a drop

            elif status == "failed":
                logger.debug(res.get("stop_reason"))
                job.status = JobStatus.FAILED
                job.msg = res.get("stop_reason")
                count_fail += 1

            elif status == "boomerang":
                # Resend to filter
                count_returns += 1
                job.article.title = res["title"]
                job.status = JobStatus.PENDING
                job.queue_name = ArticlesQueueName.FILTER

        session.commit()
        logger.info(
            f"💾 Saved Batch: {count_success} enriched, {count_fail} failed, {count_drops} dropped, {count_returns} returned to filter"
        )

    def _apply_data(self, job, res):
        art = job.article

        if not art.contents:
            art.contents = [ArticleContentModel(content=res["content"])]

        if res.get("title") and (
            not art.title or art.title.lower() in ["unknown", "no title"]
        ):
            art.title = res["title"]

        if res.get("subtitle"):
            art.subtitle = res["subtitle"]

        if res["published_date"]:
            art.published_date = res["published_date"]

        art.embedding = res["embedding"]

        llm_out = res.get("llm_output")
        if llm_out:
            art.summary = llm_out.summary
            art.stance = llm_out.stance
            art.key_points = llm_out.key_points
            art.interests = llm_out.entities
            art.entities = [f for i in llm_out.entities.values() for f in i]
            art.main_topics = llm_out.main_topics
            art.stance_reasoning = llm_out.stance_reasoning
            art.clickbait_score = llm_out.clickbait_score
            art.clickbait_reasoning = llm_out.clickbait_reasoning
            art.title = llm_out.title
            art.subtitles = llm_out.subtitles


if __name__ == "__main__":
    from multiprocessing import freeze_support

    freeze_support()

    worker = NewsEnricherWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
        _ = worker.domain.shutdown()
