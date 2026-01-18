import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, selectinload
from loguru import logger

# Models
from news_events_lib.models import ArticleModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# Domain
from domain.analyzing import ContentAnalystDomain

class NewsAnalystWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.domain = ContentAnalystDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.ANALYZE,
            batch_size=10, # IMPORTANT: Matches the optimal LLM batch size
            pending_status=JobStatus.PENDING,
        )

    async def run(self):
        logger.info(f"🧠 Analyst Worker started.")
        await super().run()

    # Override fetch to ensure we load the Article data the Domain needs
    def _fetch_jobs(self, session):
        from sqlalchemy import select
        
        stmt = (
            select(self.QueueModel)
            .options(
                selectinload(self.QueueModel.article).selectinload(ArticleModel.contents)
            )
            .where(
                self.QueueModel.status == self.pending_status, 
                self.QueueModel.queue_name == self.queue_name
            )
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        jobs = session.execute(stmt).scalars().all()
        
        # If we don't have a full batch, you might want to wait (Cost Optimization)
        # But for now, we process what we have to keep latency low.
        # If strict batching is required, you would check len(jobs) here.
        
        for job in jobs: 
            job.status = self.processing_status
        session.commit()
        return jobs

    async def process_items(self, session, jobs):
        if not jobs:
            return

        # 1. Run Domain Logic
        # The domain updates the ArticleModel objects in memory
        await self.domain.analyze_batch(jobs)

        # 2. Handle Transitions
        count_success = 0
        count_drop = 0
        
        for job in jobs:
            # Check the flags set by the Domain
            llm_status = getattr(job, "_llm_status", "success")

            if llm_status == "irrelevant":
                # Article is valid but not interesting (e.g. "Sports", "Gossip")
                job.status = JobStatus.COMPLETED
                count_drop += 1
            
            elif llm_status == "failed":
                # Technical failure or empty content
                job.status = JobStatus.FAILED
                # job.msg is already set by domain
            
            elif job.article.summary: 
                # Success: Data is present
                job.status = JobStatus.PENDING
                job.queue_name = ArticlesQueueName.CLUSTER
                job.msg = "Enriched by LLM"
                count_success += 1
            
            else:
                # Fallback for unknown states
                job.status = JobStatus.FAILED
                job.msg = "Unknown Error in Analyst"

        logger.info(f"🧠 Batch Complete: {count_success} Enriched, {count_drop} Dropped")


if __name__ == "__main__":
    worker = NewsAnalystWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")