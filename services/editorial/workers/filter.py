import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import asyncio
from datetime import datetime, timedelta, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import joinedload, sessionmaker

# Models
from news_events_lib.models import (
    ArticleModel,
    JobStatus,
    ArticlesQueueModel,
    ArticlesQueueName,
)

from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT DOMAIN LOGIC
from domain.filtering import NewsFilterDomain
from news_events_lib.audit import receive_after_flush # audit

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
            batch_size=50,  # Keep batch size aligned with LLM context limits
            pending_status=JobStatus.PENDING,
        )

    async def run(self):
        logger.info(f"Worker started for queue: {self.queue_name}")
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
        stmt = (
            select(ArticlesQueueModel)
            .join(ArticlesQueueModel.article)
            .options(joinedload(ArticlesQueueModel.article)) 
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .where(
                (ArticleModel.published_date >= datetime.now(timezone.utc) - self.settings.cutoff_period)
                | (ArticleModel.published_date.is_(None))
            )
            .order_by(ArticlesQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        
        # ✅ FIX 2: Use scalars() to get Objects, not Tuples
        jobs = session.execute(stmt).scalars().all()

        # Mark as PROCESSING immediately so other workers skip them
        for job in jobs:
            job.status = JobStatus.PROCESSING
            job.updated_at = datetime.now(timezone.utc)

        session.commit() # Save the "Lock" (PROCESSING state)
        return jobs
    async def process_items(self, session, jobs):
        if not jobs:
            return

        try:
            # 1. Run Domain Logic
            approved_payloads, rejected_jobs = await self.domain.execute_batch_filtering(jobs)
            
            # 2. ✅ FIX 3: Save the results!
            self._save_changes(session, approved_payloads, rejected_jobs)
            
            logger.success(
                f"✅ Batch Processed: {len(approved_payloads)} Approved | ❌ {len(rejected_jobs)} Rejected"
            )

        except Exception as e:
            logger.error(f"Critical error in batch processing: {e}")
            # Fallback: Mark original jobs as FAILED
            for job in jobs:
                job.status = JobStatus.FAILED
                job.msg = f"Worker Critical: {str(e)}"
            session.commit()

    def _save_changes(self, session, approved_payloads, rejected_jobs):
        """
        Saves the batch with "Bad Apple" isolation.
        If one item fails to save, it is marked as FAILED (DLQ) without breaking the batch.
        """
        
        # 1. Process APPROVED Items
        for item in approved_payloads:
            old_job = item["approved"] # The 'FILTER' job (COMPLETED)
            new_job = item["forward"]  # The 'ENRICH' job (PENDING)
            
            try:
                with session.begin_nested():
                    session.add(old_job)
                    session.add(new_job)
            except Exception as e:
                logger.error(f"Error (Approval) {old_job.id}: {e}")
                old_job.status = JobStatus.FAILED
                old_job.msg = f"Save Error: {str(e)[:100]}"
                
                session.add(old_job) 


        for job in rejected_jobs:
            try:
                with session.begin_nested():
                    session.add(job)
            except Exception as e:
                logger.error(f"Error (Rejection) {job.id}: {e}")
                job.status = JobStatus.FAILED
                job.msg = f"Save Error: {str(e)[:100]}"
                session.add(job)


        try:
            session.commit()
        except Exception as e:
            # This handles the catastrophic case where the DB is down or connection lost
            logger.critical(f"Catastrophic Batch Failure: {e}")
            session.rollback()


        
if __name__ == "__main__":
    worker = NewsFilterWorker()
    asyncio.run(worker.run())
