import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import asyncio
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload

# Imports from Service
from news_events_lib.models import JobStatus, ArticleModel
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT THE LOGIC
from domain.clustering import NewsCluster, ClusterResult

class NewsClusterWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine, expire_on_commit=False
        )
        
        # Instantiate the domain logic class
        self.cluster = NewsCluster()
        
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.CLUSTER,
            batch_size=100,
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
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    def _fetch_jobs(self, session):
        stmt = (
            select(ArticlesQueueModel, ArticleModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .options(joinedload(ArticleModel.newspaper, innerjoin=True))
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .order_by(ArticleModel.published_date.asc())
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

    async def process_item(self, session, job):
        try:
            article = job.article
            
            # Delegate logic to the domain class
            decision: ClusterResult = self.cluster.cluster_existing_article(
                session, article
            )

            if decision.action in ["MERGE", "NEW"]:
                if decision.event_id:
                    self.cluster._trigger_event_enhancement(session, decision.event_id)
                job.status = JobStatus.COMPLETED
                logger.success(f"Action {decision.action}: {article.title[:20]} -> ENHANCE")

            elif decision.action == "IGNORED":
                job.status = JobStatus.COMPLETED
                job.msg = decision.reason
                logger.info(f"Action {decision.action}: {article.title[:20]} -> SKIPPED")

            elif decision.action in ["PROPOSE", "PROPOSE_MULTI"]:
                job.status = JobStatus.COMPLETED
                job.msg = f"Parked: {decision.reason}"
                logger.warning(f"Action {decision.action}: {article.title} -> PARKED")

            else:
                job.status = JobStatus.FAILED
                job.msg = f"Unknown Action: {decision.action}"
        except Exception as e:
            logger.error(f"Error processing article {job.article_id}: {e}")
            session.rollback()
            raise e

if __name__ == "__main__":
    worker = NewsClusterWorker()
    asyncio.run(worker.run())