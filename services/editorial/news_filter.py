import asyncio
import hashlib
import re
from datetime import datetime
from time import mktime
from typing import List, Dict, Any
import uuid

import aiohttp
import feedparser
import trafilatura
from loguru import logger
from sqlalchemy import create_engine, select, update  # Added update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from news_events_lib.models import (
    ArticleModel,
    ArticleContentModel,
    AuthorModel,
    NewspaperModel,
    FeedModel,
    JobStatus,
)
from models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from llm_parser import CloudNewsFilter


class NewsFilter:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.news_filter = CloudNewsFilter()

    @staticmethod
    def estimate_tokens(text: str) -> int:
        if not text:
            return 0
        return len(text) // 4

    async def run(self):
        """Get from filter queue, filter and pass into cluster queue.

        If got approved -> change to cluster queue.
        If got rejected -> update status to error (FAILED).

        During process add status to processing
        """
        # 1. FETCH & LOCK JOBS
        with self.SessionLocal() as session:
            stmt = (
                select(ArticleModel, ArticlesQueueModel)
                .select_from(ArticleModel)
                .join(
                    ArticlesQueueModel, ArticlesQueueModel.article_id == ArticleModel.id
                )
                .where(ArticlesQueueModel.status == JobStatus.PENDING)
                .where(ArticlesQueueModel.queue_name == ArticlesQueueName.FILTER)
                .order_by(ArticlesQueueModel.created_at.asc())
                .limit(500)  # Safety limit to prevent memory explosion
                .with_for_update(skip_locked=True)
            )

            rows = session.execute(stmt).all()

            if not rows:
                logger.info("Filter queue is empty. Exiting...")
                return

            # Mark all as processing so other workers don't grab them
            result = []
            for article, queue in rows:
                queue.status = JobStatus.PROCESSING
                session.add(queue)
                result.append({"title": article.title, "queue_id": queue.id})

            session.commit()
            logger.info(f"Processing {len(result)} articles from the filter queue.")

        # 2. PROCESS IN BATCHES
        batch_size = 50
        for i in range(0, len(result), batch_size):
            batch = result[i : i + batch_size]

            # Extract just the titles for the AI
            titles = [item["title"] for item in batch]
            queue_ids = [item["queue_id"] for item in batch]

            approved_indices = []
            failed_batch = False

            max_retries = 5
            for attempt in range(max_retries):
                try:
                    logger.info(
                        f"Filtering batch {i}-{i+len(batch)} (Attempt {attempt+1})..."
                    )

                    # Call LLM
                    approved_indices = await self.news_filter.filter_batch(titles)

                    # Small sleep to be nice to the API
                    await asyncio.sleep(2)
                    break
                except Exception as e:
                    msg = str(e)
                    if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                        wait_time = 20.0
                        # Extract retryDelay if present, e.g. 'retryDelay': '3.66s'
                        match = re.search(r"['\"]retryDelay['\"]\s*:\s*['\"](\d+(?:\.\d+)?)s['\"]", msg)
                        if match:
                            try:
                                wait_time = float(match.group(1)) + 1.0  # Add 1s buffer
                            except ValueError:
                                pass

                        logger.warning(
                            f"⚠️ Quota Exceeded (429). Cooling down for {wait_time:.2f}s..."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Unexpected error on batch {i}: {e}")
                        # If we hit a non-rate-limit error, we might want to fail the batch
                        if attempt == max_retries - 1:
                            failed_batch = True
                        await asyncio.sleep(2)

            # 3. SORT RESULTS
            approved_ids = []
            rejected_ids = []
            error_ids = []

            if failed_batch:
                # If AI completely failed, mark whole batch as Error to retry later or inspect
                error_ids = queue_ids
            else:
                for idx, item in enumerate(batch):
                    if idx in approved_indices:
                        approved_ids.append(item["queue_id"])
                    else:
                        rejected_ids.append(item["queue_id"])

            # 4. UPDATE DB (Per Batch)
            # We open a NEW session here to commit this batch immediately
            with self.SessionLocal() as session:
                try:
                    # A. Move Approved to Cluster Queue
                    if approved_ids:
                        session.execute(
                            update(ArticlesQueueModel)
                            .where(ArticlesQueueModel.id.in_(approved_ids))
                            .values(
                                status=JobStatus.PENDING,  # Reset to Pending for next worker
                                queue_name=ArticlesQueueName.CLUSTER,  # Move to next stage
                                updated_at=datetime.utcnow(),
                            )
                        )
                        logger.success(
                            f"✅ Approved {len(approved_ids)} articles -> Cluster Queue"
                        )

                    # B. Mark Rejected as Failed (or filtered)
                    if rejected_ids:
                        session.execute(
                            update(ArticlesQueueModel)
                            .where(ArticlesQueueModel.id.in_(rejected_ids))
                            .values(
                                status=JobStatus.COMPLETED,
                                msg="Rejected by AI Filter",
                                updated_at=datetime.utcnow(),
                            )
                        )
                        logger.info(f"❌ Rejected {len(rejected_ids)} articles.")

                    # C. Handle AI Errors
                    if error_ids:
                        session.execute(
                            update(ArticlesQueueModel)
                            .where(ArticlesQueueModel.id.in_(error_ids))
                            .values(
                                status=JobStatus.FAILED,
                                msg="AI Processing Error (Max Retries)",
                                updated_at=datetime.utcnow(),
                            )
                        )
                        logger.error(
                            f"⚠️ Marked {len(error_ids)} articles as FAILED due to AI error."
                        )

                    session.commit()
                except Exception as e:
                    logger.error(f"Critical DB Error updating batch: {e}")
                    session.rollback()


if __name__ == "__main__":
    producer = NewsFilter()
    asyncio.run(producer.run())
