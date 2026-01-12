import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from itertools import groupby
from loguru import logger
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker

# Models
from news_events_lib.models import MergeProposalModel, JobStatus
from config import Settings
from core.base_worker import BaseQueueWorker

# Domain Logic
from domain.review import NewsReviewerDomain


class NewsReviewerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn), pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Domain
        self.domain = NewsReviewerDomain()

        # Configuration
        self.concurrency = 10

        # Initialize BaseWorker (Used mainly for structure, run() is overridden)
        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=MergeProposalModel,
            target_queue_name=None,
            batch_size=10,
            pending_status=JobStatus.PENDING,
        )

        self._reset_stuck_tasks()

    async def run(self):
        logger.info(f"🚀 Reviewer Worker started (Concurrency: {self.concurrency})")

        # Internal Queue for Producer-Consumer pattern
        self.internal_queue = asyncio.Queue(maxsize=15)

        # Start Consumers
        workers = []
        for i in range(self.concurrency):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(0.5)  # Stagger start

        # Producer Loop
        while True:
            try:
                if self.internal_queue.full():
                    await asyncio.sleep(1.0)
                    continue

                with self.SessionLocal() as session:
                    # 1. Fetch raw jobs
                    jobs = self._fetch_jobs_strategy(session)

                    if not jobs:
                        logger.debug("No jobs. Sleeping...")
                        await asyncio.sleep(30)
                        continue

                    # 2. Group by Source (Article OR Event)
                    # We sort by the composite key of source IDs
                    jobs.sort(
                        key=lambda x: str(x.source_article_id or x.source_event_id)
                    )

                    for source_id, group in groupby(
                        jobs, key=lambda x: x.source_article_id or x.source_event_id
                    ):
                        items = list(group)
                        if not items:
                            continue

                        is_event_merge = items[0].source_event_id is not None
                        proposal_ids = [p.id for p in items]

                        # Push to Consumers
                        await self.internal_queue.put(
                            (source_id, proposal_ids, is_event_merge)
                        )

            except Exception as e:
                logger.error(f"Producer Error: {e}")
                await asyncio.sleep(5)

    async def _consumer_loop(self, worker_id):
        while True:
            source_id, proposal_ids, is_event_merge = await self.internal_queue.get()
            try:
                # We create a fresh session for the domain logic thread
                with self.SessionLocal() as session:
                    await self.domain.review_proposals(
                        session, source_id, proposal_ids, is_event_merge
                    )
            except Exception as e:
                logger.error(f"Worker {worker_id} error on {source_id}: {e}")
            finally:
                self.internal_queue.task_done()

    def _fetch_jobs_strategy(self, session):
        """
        Prioritizes Articles, then Events.
        Marks fetched rows as 'processing' immediately to prevent double-reads.
        """
        # A. Articles
        subq_art = (
            select(MergeProposalModel.source_article_id)
            .where(
                MergeProposalModel.status == "pending",
                MergeProposalModel.source_article_id.is_not(None),
            )
            .distinct()
            .limit(self.batch_size)
        )
        article_ids = session.execute(subq_art).scalars().all()

        if article_ids:
            return self._lock_and_fetch(
                session, MergeProposalModel.source_article_id, article_ids
            )

        # B. Events
        subq_evt = (
            select(MergeProposalModel.source_event_id)
            .where(
                MergeProposalModel.status == "pending",
                MergeProposalModel.source_event_id.is_not(None),
            )
            .distinct()
            .limit(self.batch_size)
        )
        event_ids = session.execute(subq_evt).scalars().all()

        if event_ids:
            return self._lock_and_fetch(
                session, MergeProposalModel.source_event_id, event_ids
            )

        return []

    def _lock_and_fetch(self, session, column, ids):
        stmt = (
            select(MergeProposalModel)
            .where(column.in_(ids))
            .where(MergeProposalModel.status == "pending")
            .with_for_update(skip_locked=True)
        )
        proposals = session.execute(stmt).scalars().all()
        for p in proposals:
            p.status = "processing"
        session.commit()
        return proposals

    def _reset_stuck_tasks(self):
        with self.SessionLocal() as session:
            res = session.execute(
                update(MergeProposalModel)
                .where(MergeProposalModel.status == "processing")
                .values(status="pending")
            )
            session.commit()
            if res.rowcount:
                logger.info(f"🧹 Reset {res.rowcount} stuck proposals.")


if __name__ == "__main__":
    worker = NewsReviewerWorker()
    asyncio.run(worker.run())
