import sys
import os
from typing import List

from requests import session




sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import uuid
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker, joinedload
from utils.article_manager import ArticleManager
from utils.event_manager import EventManager
# Imports from Service
from news_events_lib.models import (
    JobStatus,
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueModel,
    EventsQueueName,
    NewsEventModel,
)
from news_events_lib.audit import receive_after_flush

from config import Settings
from core.base_worker import BaseQueueWorker

# IMPORT THE LOGIC
from domain.clustering import Candidate, NewsCluster, ClusterResult


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
            batch_size=100,  # Large batch for throughput
            pending_status=JobStatus.PENDING,
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
            select(ArticlesQueueModel)
            .join(ArticleModel, ArticlesQueueModel.article_id == ArticleModel.id)
            .options(joinedload(ArticleModel.newspaper, innerjoin=True))
            .where(ArticlesQueueModel.status == self.pending_status)
            .where(ArticlesQueueModel.queue_name == self.queue_name)
            .order_by(ArticleModel.published_date.asc())
            .order_by(ArticlesQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()

        jobs = []
        for queue_item in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            jobs.append(queue_item)

        session.commit()
        return jobs

    async def process_item(self, session:Session, job:ArticlesQueueModel):
        """
        Called by BaseQueueWorker for each item in the batch.
        Runs inside the main worker transaction.
        """
        article = job.article

        # 1. Run Domain Logic
        decision: ClusterResult = self.cluster.cluster_existing_article(
            session, article
        )
        job.updated_at = datetime.now(timezone.utc)
        job.attempts += 1

        # 2. Handle Outcome & Transitions
        if decision.action in ["MERGE", "NEW"]:
            if decision.event:
                # Trigger Enhancer for the modified/new event

                EventManager.create_event_queue(
                    session,
                    decision.event.id,
                    EventsQueueName.ENHANCER,
                    reason="From Cluster: " + decision.reason,
                )
            job.status = JobStatus.COMPLETED
            job.msg = decision.reason
            logger.success(f"Action {decision.action}: {article.title[:20]}")

        elif decision.action == "IGNORED":
            job.status = JobStatus.COMPLETED
            job.msg = decision.reason
            logger.info(f"Action {decision.action}: {article.title[:20]} -> SKIPPED")

        elif decision.action in ["PROPOSE", "PROPOSE_MULTI"]:
            # Park the article until a human decides
            job.status = JobStatus.COMPLETED
            job.msg = f"Parked: {decision.reason}"
            if decision.event:
                self._create_proposals(
                    session, decision.event, decision.candidates, reason=decision.reason
                )
                logger.warning(f"Action {decision.action}: {article.title[:20]} -> PARKED")

        elif decision.action == "ERROR":
            job.status = JobStatus.FAILED
            job.msg = decision.reason

        else:
            job.status = JobStatus.FAILED
            job.msg = f"Unknown Action: {decision.action}"

    def _create_proposals(
        self,
        session: Session,
        event: NewsEventModel,
        candidates: List[Candidate],
        reason: str = "",
    ):
        for candidate in candidates:
            ArticleManager.create_merge_proposal(
                session, candidate.article, event, candidate.dist, reason=reason
            )


if __name__ == "__main__":
    worker = NewsClusterWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")
