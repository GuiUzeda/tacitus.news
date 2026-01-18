from datetime import datetime, timezone
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
        Refactored: Commits each source group immediately ("Commit-as-you-go").
        Prevents one failure from rolling back valid work from other groups.
        """
        # PHASE 1: FETCH & LOCK
        jobs = []
        with self.SessionLocal() as session:
            session.expire_on_commit = False
            jobs = self._fetch_jobs_strategy(session)
            # Commit immediately to release "FOR UPDATE" locks and save PROCESSING state
            session.commit()

        if not jobs: 
            return 0

        # PHASE 2: PROCESS & COMMIT PER GROUP
        with self.SessionLocal() as session:
            # Sort to ensure groupby works correctly
            jobs.sort(key=lambda x: str(x.source_article_id or x.source_event_id))
            
            for source_id, group in groupby(jobs, key=lambda x: x.source_article_id or x.source_event_id):
                items = list(group)
                if not items or not source_id: continue

                try:
                    # 1. Prepare Data
                    proposal_ids = [p.id for p in items]
                    is_event_merge = items[0].source_event_id is not None
                    
                    # 2. Call Domain Logic (Writes to Session, does NOT commit)
                    result = await self.domain.review_proposals(
                        session, source_id, proposal_ids, is_event_merge
                    )
                    
                    # 3. COMMIT IMMEDIATELY
                    # This saves the changes for this specific source and releases locks
                    session.commit()
                    logger.info(f"Source {str(source_id)}: {result} (Committed)")

                except Exception as e:
                    # 4. Handle Failure for this Group
                    session.rollback() # Undo pending changes for this group only
                    logger.error(f"❌ Failed group {source_id}: {e}")
                    
                    # 5. Mark items as FAILED in a fresh transaction
                    try:
                        for p in items:
                            # We must use merge() because 'p' is detached from the Phase 1 session
                            failed_job = session.merge(p)
                            failed_job.status = JobStatus.FAILED
                            failed_job.reasoning = f"Worker Error: {str(e)[:100]}"
                            session.add(failed_job)
                        session.commit()
                    except Exception as commit_err:
                        logger.critical(f"Failed to save error state: {commit_err}")
                        session.rollback()

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
            for p in urgent: 
                p.status = JobStatus.PROCESSING
                p.updated_at = datetime.now(timezone.utc) # <--- ADD THIS
            return urgent

        # 2. General Articles/Events
        general = session.scalars(
            select(MergeProposalModel)
            .where(MergeProposalModel.status == JobStatus.PENDING)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        ).all()

        for p in general:
            p.status = JobStatus.PROCESSING
            p.updated_at = datetime.now(timezone.utc) # <--- ADD THIS
        # Note: We do NOT commit here anymore, the caller does it.
        
        return general

if __name__ == "__main__":
    worker = NewsReviewerWorker()
    asyncio.run(worker.run())