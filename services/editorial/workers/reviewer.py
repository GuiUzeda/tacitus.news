import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from itertools import groupby
from typing import List
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Models
from news_events_lib.models import MergeProposalModel, JobStatus, NewsEventModel, EventStatus
from config import Settings
from core.base_worker import BaseQueueWorker

# Domain
from domain.review import NewsReviewerDomain

class NewsReviewerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)

        self.domain = NewsReviewerDomain()

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=MergeProposalModel,
            target_queue_name=None, 
            batch_size=30, # Fetch enough to group effectively
            pending_status=JobStatus.PENDING,
            processing_status=JobStatus.PROCESSING
        )

    async def run(self):
        logger.info(f"🚀 Reviewer Worker started.")
        await super().run()

    # --- STRATEGY: GROUPED TRANSACTIONS ---
    
    async def process_batch(self):
        """
        Custom batch processor.
        We group proposals by 'Source' and commit each source INDEPENDENTLY.
        """
        # PHASE 1: FETCH & LOCK
        # We use a short transaction just to grab work and mark it PROCESSING
        jobs = []
        with self.SessionLocal() as session:
            session.expire_on_commit = False
            jobs = self._fetch_jobs_strategy(session)
            # Commit immediately to release "FOR UPDATE" locks and save PROCESSING state
            session.commit()

        if not jobs: 
            return 0

        # PHASE 2: PROCESS (New Session)
        # We process the *detached* job objects. We must re-attach them or fetch by ID.
        with self.SessionLocal() as session:
            # Re-fetch or Merge is needed because objects are detached. 
            # However, since we have IDs, we can just query or merge.
            # Grouping logic:
            jobs.sort(key=lambda x: str(x.source_article_id or x.source_event_id))
            
            for source_id, group in groupby(jobs, key=lambda x: x.source_article_id or x.source_event_id):
                items = list(group)
                if not items or not source_id: continue

                # --- TRANSACTION BOUNDARY START ---
                try:
                    # Using nested transaction (savepoint) so we can rollback JUST this group on error
                    with session.begin_nested():
                        is_event_merge = items[0].source_event_id is not None
                        proposal_ids = [p.id for p in items]

                        # Call Domain (Pure Logic, No Commit)
                        result = await self.domain.review_proposals(
                            session, source_id, proposal_ids, is_event_merge
                        )
                        
                        logger.info(f"Source {str(source_id)[:8]}: {result}")
                    
                    # If we reach here, this group is staged for commit.
                
                except Exception as e:
                    logger.error(f"❌ Failed group {source_id}: {e}")
                    # Since we were in begin_nested, the DB changes for this group are already rolled back.
                    # Now we just mark these jobs as FAILED in memory so the outer commit saves that state.
                    # We must re-fetch/merge them to update status in this session
                    for p in items:
                        failed_job = session.merge(p)
                        failed_job.status = JobStatus.FAILED
                        failed_job.reasoning = f"Worker Error: {str(e)[:50]}"
                        session.add(failed_job)
                        
                # --- TRANSACTION BOUNDARY END ---

            # PHASE 3: FINAL COMMIT
            # This commits all successful groups AND the FAILED statuses for failed groups
            session.commit()
            
        return len(jobs)

    def _fetch_jobs_strategy(self, session) -> List[MergeProposalModel]:
        # Simple priority fetch: Published Events First, then Articles
        
        # 1. Published Events (High Priority)
        urgent = session.scalars(
            select(MergeProposalModel)
            .join(NewsEventModel, MergeProposalModel.target_event_id == NewsEventModel.id)
            .where(
                MergeProposalModel.status == JobStatus.PENDING,
                NewsEventModel.status == EventStatus.PUBLISHED
            )
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        ).all()
        
        if urgent:
            for p in urgent: p.status = JobStatus.PROCESSING
            return urgent

        # 2. General Articles/Events
        general = session.scalars(
            select(MergeProposalModel)
            .where(MergeProposalModel.status == JobStatus.PENDING)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        ).all()

        for p in general: p.status = JobStatus.PROCESSING
        # Note: We do NOT commit here anymore, the caller does it.
        
        return general

if __name__ == "__main__":
    worker = NewsReviewerWorker()
    asyncio.run(worker.run())