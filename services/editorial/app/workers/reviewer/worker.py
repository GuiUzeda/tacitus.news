import asyncio
from datetime import datetime, timezone
from itertools import groupby
from typing import List

from app.config import Settings
from app.workers.base_worker import BaseQueueWorker
from app.workers.reviewer.domain import NewsReviewerDomain, ReviewerAction
from loguru import logger
from news_events_lib.audit import receive_after_flush  # noqa: F401

# Models
from news_events_lib.models import JobStatus, MergeProposalModel
from sqlalchemy import create_engine, exists, func, or_, select
from sqlalchemy.orm import aliased, joinedload, sessionmaker


class NewsReviewerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        self.domain = NewsReviewerDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=MergeProposalModel,
            target_queue_name=None,
            batch_size=30,
            pending_status=JobStatus.PENDING,
        )

    async def process_batch(self):
        """
        1. Fetch proposals grouped by Source.
        2. Process each Group transactionally.
        """

        # 1. FETCH & CLAIM
        jobs = []
        with self.SessionLocal() as session:
            jobs = self._fetch_jobs_strategy(session)
            if jobs:
                session.commit()  # Lock them as PROCESSING

            if not jobs:
                return 0

            # 2. GROUP BY SOURCE
            # Sort required for groupby
            jobs.sort(key=lambda x: str(x.source_article_id or x.source_event_id))

        processed_count = 0

        for source_id, group in groupby(
            jobs, key=lambda x: x.source_article_id or x.source_event_id
        ):
            # Graceful Shutdown Check
            if self.should_exit:
                self._requeue_remaining_jobs(jobs[processed_count:])
                break

            items = list(group)
            if not items or not source_id:
                continue

            # 3. PROCESS GROUP (Isolated Transaction)
            with self.SessionLocal() as session:
                try:
                    # Re-attach items to this session (Domain needs DB access)
                    # We just need IDs, domain handles logic
                    proposal_ids = [p.id for p in items]
                    is_event_merge = items[0].source_event_id is not None

                    # Call Domain
                    result = await self.domain.review_proposals(
                        session, source_id, proposal_ids, is_event_merge
                    )

                    # Log Outcome
                    if result.action in [
                        ReviewerAction.MERGED,
                        ReviewerAction.NEW_EVENT,
                    ]:
                        logger.success(
                            f"✅ {result.action}: {result.reason} - source {source_id}"
                        )
                        # Note: Domain already pruned zombies/siblings in the DB.

                    elif result.action == ReviewerAction.REJECTED:
                        logger.info(f"🚫 REJECTED: {result.reason}")

                    elif result.action == ReviewerAction.SKIPPED:
                        logger.warning(f"⏩ SKIPPED (Lazy Cleanup): {result.reason}")

                    session.commit()
                    processed_count += len(items)

                except Exception as e:
                    logger.opt(exception=True).error(
                        f"❌ Failed Group {source_id}: {e}"
                    )
                    session.rollback()
                    self._mark_group_failed(items, str(e))
                    processed_count += len(items)

        return processed_count

    def _fetch_jobs_strategy(self, session) -> List[MergeProposalModel]:
        # Aliases
        Proposal = aliased(MergeProposalModel)
        Busy = aliased(MergeProposalModel)

        # 1. Normalized Keys
        source_key = func.coalesce(Proposal.source_article_id, Proposal.source_event_id)

        # 2. THE COLLISION SUBQUERY
        # We find any job currently PROCESSING that involves:
        # a) The same Source (someone else is handling this article/event)
        # b) The same Target (someone else is merging something else into our target event)
        # c) The Target of a busy job is our Source (rare, but keeps graph clean)
        busy_collision_subq = select(1).where(
            Busy.status == JobStatus.PROCESSING,
            or_(
                # Someone is already moving this source
                func.coalesce(Busy.source_article_id, Busy.source_event_id)
                == source_key,
                # Someone is already merging into our target
                Busy.target_event_id == Proposal.target_event_id,
                # Someone is using our target as their source (Chain prevention)
                func.coalesce(Busy.source_article_id, Busy.source_event_id)
                == Proposal.target_event_id,
                # Someone is using our source as their target
                Busy.target_event_id == source_key,
            ),
        )

        # 3. Step 1: Rank available groups that have ZERO collisions
        rank_subquery = (
            select(
                Proposal.id, func.dense_rank().over(order_by=source_key).label("rank")
            )
            .where(Proposal.status == JobStatus.PENDING, ~exists(busy_collision_subq))
            .subquery()
        )

        # 4. Get the Top 10 safe groups
        target_ids_stmt = select(rank_subquery.c.id).where(rank_subquery.c.rank <= 10)
        target_ids = session.execute(target_ids_stmt).scalars().all()

        if not target_ids:
            return []

        # 5. Step 2: Fetch and Lock the proposals
        # We still use 'of=MergeProposalModel' to avoid the Join-Lock error
        stmt = (
            select(MergeProposalModel)
            .options(
                joinedload(MergeProposalModel.target_event),
                joinedload(MergeProposalModel.source_article),
                joinedload(MergeProposalModel.source_event),
            )
            .where(
                MergeProposalModel.id.in_(target_ids),
                MergeProposalModel.status == JobStatus.PENDING,
            )
            # FIX: Reference the columns directly on the model class to avoid alias errors
            .order_by(
                func.coalesce(
                    MergeProposalModel.source_article_id,
                    MergeProposalModel.source_event_id,
                )
            )
            .with_for_update(skip_locked=True, of=MergeProposalModel)
        )

        jobs = session.execute(stmt).scalars().all()

        # 6. Mark as Processing
        for job in jobs:
            job.status = JobStatus.PROCESSING
            job.updated_at = datetime.now(timezone.utc)

        return list(jobs)

    def _requeue_remaining_jobs(self, jobs):
        if not jobs:
            return
        with self.SessionLocal() as session:
            for job in jobs:
                j = session.merge(job)
                j.status = JobStatus.PENDING
            session.commit()
            logger.warning(f"🛑 Re-queued {len(jobs)} jobs due to shutdown.")

    def _mark_group_failed(self, items, error_msg):
        try:
            with self.SessionLocal() as session:
                for item in items:
                    obj = session.merge(item)
                    obj.status = JobStatus.FAILED
                    obj.reasoning = f"Worker Crash: {error_msg[:100]}"
                session.commit()
        except Exception as e:
            logger.error(f"Failed to save error state: {e}")

    async def process_item(self, session, job):
        pass


if __name__ == "__main__":
    worker = NewsReviewerWorker()
    asyncio.run(worker.run())
