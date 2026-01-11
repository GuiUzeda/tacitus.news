import asyncio
import sys
import os
from datetime import datetime, timezone
from typing import List

from base_worker import BaseQueueWorker

from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from news_events_lib.models import (
    ArticleModel,
    JobStatus,
)
from models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from llm_parser import CloudNewsFilter

class NewsFilterWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.news_filter = CloudNewsFilter()
        
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.FILTER,
            batch_size=50,
            pending_status=JobStatus.PENDING
        )

    def _fetch_jobs(self, session):
        # Override to join with ArticleModel
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .order_by(ArticlesQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()
        
        jobs = []
        for queue_item, article in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            # Attach article to queue_item temporarily for processing
            queue_item.article = article 
            jobs.append(queue_item)
            
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        if not jobs: return

        # Extract titles
        titles = [job.article.title for job in jobs]
        
        approved_indices = []
        failed_batch = False
        
        try:
            logger.info(f"Filtering batch of {len(titles)} articles...")
            # Retries are now handled by the decorator in llm_parser
            approved_indices = await self.news_filter.filter_batch(titles)
            await asyncio.sleep(1) # Brief pause between batches
        except Exception as e:
            logger.error(f"Batch failed after retries: {e}")
            failed_batch = True

        # Process Results
        if failed_batch:
            for job in jobs:
                job.status = JobStatus.FAILED
                job.msg = "AI Processing Error (Max Retries)"
                job.updated_at = datetime.now(timezone.utc)
            logger.error(f"⚠️ Marked {len(jobs)} articles as FAILED due to AI error.")
        else:
            approved_count = 0
            rejected_count = 0
            
            for idx, job in enumerate(jobs):
                if idx in approved_indices:
                    # Approved -> Move to Cluster Queue
                    job.status = JobStatus.PENDING
                    job.queue_name = ArticlesQueueName.CLUSTER
                    job.updated_at = datetime.now(timezone.utc)
                    approved_count += 1
                else:
                    # Rejected -> Completed
                    job.status = JobStatus.COMPLETED
                    job.msg = "Rejected by AI Filter"
                    job.updated_at = datetime.now(timezone.utc)
                    rejected_count += 1
            
            logger.success(f"✅ Approved {approved_count} | ❌ Rejected {rejected_count}")

if __name__ == "__main__":
    worker = NewsFilterWorker()
    asyncio.run(worker.run())