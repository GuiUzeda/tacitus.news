import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import asyncio
from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Models
from news_events_lib.models import ArticleModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT DOMAIN LOGIC
from domain.filtering import NewsFilterDomain

class NewsFilterWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        
        # Instantiate Domain Logic
        self.domain = NewsFilterDomain()
        
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.FILTER,
            batch_size=50, # Keep batch size aligned with LLM context limits
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        logger.info(f"🚀 Worker started for queue: {self.queue_name}")
        while True:
            try:
                count = await self.process_batch()
                if count == 0:
                    logger.info(f"Queue {self.queue_name} empty. Sleeping 60s...")
                    await asyncio.sleep(60)
                else:
                    # Brief pause between batches to be nice to the API
                    await asyncio.sleep(1)
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    def _fetch_jobs(self, session):
        # Override to join with ArticleModel efficiently
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .where(
                (ArticleModel.published_date >= datetime.now(timezone.utc) - timedelta(days=7)) | 
                (ArticleModel.published_date.is_(None))
            )
            .order_by(ArticlesQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()
        
        jobs = []
        for queue_item, article in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            # Attach article to queue_item for the domain logic to use
            queue_item.article = article 
            jobs.append(queue_item)
            
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        """
        Delegates the batch processing to the Domain Layer.
        """
        if not jobs: return

        try:
            approved, rejected = await self.domain.execute_batch_filtering(session, jobs)
            logger.success(f"✅ Batch Processed: {approved} Approved | ❌ {rejected} Rejected")
        except Exception as e:
            logger.error(f"Critical error in batch processing: {e}")
            # Fallback: Mark all as failed if domain logic crashes unhandled
            for job in jobs:
                job.status = JobStatus.FAILED
                job.msg = f"Worker Critical: {str(e)}"

if __name__ == "__main__":
    worker = NewsFilterWorker()
    asyncio.run(worker.run())