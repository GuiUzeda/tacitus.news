import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, selectinload

# Models
from news_events_lib.models import ArticleModel, JobStatus
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
        self.domain = EnrichingDomain(max_cpu_workers=1, http_concurrency=10)

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.ENRICH,
            batch_size=10, # Smaller batch size due to heavy processing
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        logger.info(f"🚀 Worker started for queue: {self.queue_name}")
        # Warmup CPU workers
        await asyncio.to_thread(self.domain.warmup)
        
        while True:
            try:
                count = await self.process_batch()
                if count == 0:
                    logger.info(f"Queue {self.queue_name} empty. Sleeping 60s...")
                    await asyncio.sleep(60)
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    def _fetch_jobs(self, session):
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .options(selectinload(ArticleModel.contents))
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
            queue_item.article = article 
            jobs.append(queue_item)
            
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        await self.domain.process_batch(session, jobs)

if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()
    
    worker = NewsEnricherWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
        worker.domain.shutdown()