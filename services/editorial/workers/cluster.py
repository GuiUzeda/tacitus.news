import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import uuid
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload

# Imports from Service
from news_events_lib.models import JobStatus, ArticleModel
from core.models import ArticlesQueueModel, ArticlesQueueName, EventsQueueModel, EventsQueueName
from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT THE LOGIC
from domain.clustering import NewsCluster, ClusterResult

class NewsClusterWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        
        self.cluster = NewsCluster()
        
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=ArticlesQueueModel,
            target_queue_name=ArticlesQueueName.CLUSTER,
            batch_size=100, # Large batch for throughput
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        # We override run only to call warmup if needed, else super().run() is fine.
        logger.info(f"🚀 Cluster Worker started.")
        await super().run()

    def _fetch_jobs(self, session):
        """
        Override to eagerly load 'newspaper' and other relations needed for clustering.
        """
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
        """
        Called by BaseQueueWorker for each item in the batch.
        Runs inside the main worker transaction.
        """
        article = job.article
        
        # 1. Run Domain Logic
        decision: ClusterResult = self.cluster.cluster_existing_article(
            session, article
        )

        # 2. Handle Outcome & Transitions
        if decision.action in ["MERGE", "NEW"]:
            if decision.event_id:
                # Trigger Enhancer for the modified/new event
                self._trigger_event_job(session, decision.event_id, EventsQueueName.ENHANCER)
            
            job.status = JobStatus.COMPLETED
            logger.success(f"Action {decision.action}: {article.title[:20]}")

        elif decision.action == "IGNORED":
            job.status = JobStatus.COMPLETED
            job.msg = decision.reason
            logger.info(f"Action {decision.action}: {article.title[:20]} -> SKIPPED")

        elif decision.action in ["PROPOSE", "PROPOSE_MULTI"]:
            # Park the article until a human decides
            job.status = JobStatus.COMPLETED
            job.msg = f"Parked: {decision.reason}"
            logger.warning(f"Action {decision.action}: {article.title[:20]} -> PARKED")

        elif decision.action == "ERROR":
            job.status = JobStatus.FAILED
            job.msg = decision.reason
        
        else:
            job.status = JobStatus.FAILED
            job.msg = f"Unknown Action: {decision.action}"

    def _trigger_event_job(self, session, event_id: uuid.UUID, queue_name: EventsQueueName):
        """
        Helper to enqueue the event for the next stage (Enhancer/Publisher).
        Uses 'merge' to avoid duplicates in the same session.
        """
        # Check local session cache first
        for obj in session.new:
            if isinstance(obj, EventsQueueModel) and obj.event_id == event_id:
                return

        # Check DB
        existing = session.scalar(
            select(EventsQueueModel).where(EventsQueueModel.event_id == event_id)
        )

        if existing:
            if existing.status != JobStatus.PENDING or existing.queue_name != queue_name:
                existing.status = JobStatus.PENDING
                existing.queue_name = queue_name
                existing.updated_at = datetime.now(timezone.utc)
                session.add(existing)
        else:
            new_job = EventsQueueModel(
                event_id=event_id,
                queue_name=queue_name,
                status=JobStatus.PENDING,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            session.add(new_job)

if __name__ == "__main__":
    worker = NewsClusterWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")