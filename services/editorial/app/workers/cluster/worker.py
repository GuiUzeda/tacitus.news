import asyncio
from datetime import datetime, timezone
from typing import List

from app.config import Settings
from app.utils.article_manager import ArticleManager
from app.utils.event_manager import EventManager
from app.workers.base_worker import BaseArticleQueueWorker

# Logic & Utils
from app.workers.cluster.domain import (
    Candidate,
    ClusterAction,
    ClusterResult,
    NewsCluster,
)
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401

# Imports from Service
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    EventsQueueName,
    JobStatus,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, contains_eager, sessionmaker


class NewsClusterWorker(BaseArticleQueueWorker):
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
            batch_size=50,
            pending_status=JobStatus.PENDING,
        )

    def _fetch_jobs(self, session):
        stmt = (
            select(ArticlesQueueModel)
            .join(ArticlesQueueModel.article)
            .options(
                contains_eager(ArticlesQueueModel.article).joinedload(
                    ArticleModel.newspaper, innerjoin=True
                )
            )
            .where(
                ArticlesQueueModel.status == self.pending_status,
                ArticlesQueueModel.queue_name == self.queue_name,
            )
            .order_by(ArticleModel.published_date.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        jobs = session.execute(stmt).scalars().all()

        for job in jobs:
            job.status = self.processing_status

        session.flush()
        return list(jobs)

    async def process_item(self, session: Session, job: ArticlesQueueModel):
        article = job.article

        # 1. Run Domain Logic
        decision: ClusterResult = self.cluster.cluster_existing_article(
            session, article
        )

        job.updated_at = datetime.now(timezone.utc)
        job.attempts += 1

        # 2. Handle Outcome

        # --- MERGE or NEW ---
        if decision.action in [ClusterAction.MERGE, ClusterAction.NEW]:
            if decision.event:
                # IMPORTANT: Flush to ensure Event ID exists before queuing
                session.flush()

                EventManager.create_event_queue(
                    session,
                    decision.event.id,
                    EventsQueueName.ENHANCER,
                    reason=f"From Cluster: {decision.reason}",
                )
                logger.success(
                    f"{decision.action}: '{article.title[:30]}...' -> {decision.event.title[:30]}"
                )
                job.status = JobStatus.COMPLETED
                job.msg = decision.reason
            else:
                job.status = JobStatus.FAILED
                job.msg = "No event found in the response"
                raise Exception("No event found in the response")

        # --- IGNORED ---
        elif decision.action == ClusterAction.IGNORED:
            job.status = JobStatus.COMPLETED
            job.msg = decision.reason
            logger.info(f"IGNORED: '{article.title[:30]}...' ({decision.reason})")

        # --- PROPOSALS ---
        elif decision.action in [ClusterAction.PROPOSE, ClusterAction.PROPOSE_MULTI]:
            job.status = JobStatus.COMPLETED
            job.msg = f"Parked: {decision.reason}"

            self._create_proposals(session, decision.candidates, reason=decision.reason)
            logger.warning(
                f"PROPOSAL: '{article.title[:30]}...' vs {len(decision.candidates)} Candidates"
            )

        # --- ERROR ---
        elif decision.action == ClusterAction.ERROR:
            job.status = JobStatus.FAILED
            job.msg = decision.reason
            logger.error(f"ERROR: '{article.title[:30]}...' -> {decision.reason}")

    def _create_proposals(
        self,
        session: Session,
        candidates: List[Candidate],
        reason: str = "",
    ):
        for candidate in candidates:
            ArticleManager.create_merge_proposal(
                session,
                candidate.article,
                candidate.target_event,
                candidate.dist,
                reason=reason,
                status=JobStatus.PENDING,
            )


if __name__ == "__main__":
    worker = NewsClusterWorker()
    asyncio.run(worker.run())
