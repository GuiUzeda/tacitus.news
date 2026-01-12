import asyncio
from datetime import datetime, timezone
from typing import List, Tuple
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

# Models
from news_events_lib.models import (
    ArticleModel,
    JobStatus,
)
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.llm_parser import CloudNewsFilter


class NewsFilterDomain:
    def __init__(self):
        self.settings = Settings()
        # Initialize the LLM Service (from Core)
        self.llm_filter = CloudNewsFilter()

    async def execute_batch_filtering(
        self, session: Session, jobs: List[ArticlesQueueModel]
    ) -> Tuple[int, int]:
        """
        Orchestrates the filtering process:
        1. Extracts titles from the batch.
        2. Calls the LLM to filter them.
        3. Updates the Database states based on the result.

        Returns: (approved_count, rejected_count)
        """
        if not jobs:
            return 0, 0

        # 1. Prepare Data
        # We assume jobs have 'article' attached or we fetch them.
        # The worker usually pre-loads them.
        titles = []
        valid_jobs = []

        for job in jobs:
            if hasattr(job, "article") and job.article:
                titles.append(job.article.title)
                valid_jobs.append(job)
            else:
                # Fallback if article missing (shouldn't happen with correct worker fetch)
                job.status = JobStatus.FAILED
                job.msg = "Article data missing"

        if not titles:
            return 0, 0

        # 2. Call AI (The "Gatekeeper")
        try:
            logger.info(f"🧠 Filtering batch of {len(titles)} articles...")
            approved_indices = await self.llm_filter.filter_batch(titles)
        except Exception as e:
            logger.error(f"AI Filter failed: {e}")
            # Mark whole batch as failed so it can be retried later
            for job in valid_jobs:
                job.status = JobStatus.FAILED
                job.msg = f"AI Error: {str(e)[:100]}"
                job.updated_at = datetime.now(timezone.utc)
            return 0, 0

        # 3. Apply Business Rules (State Transitions)
        approved_count = 0
        rejected_count = 0

        for idx, job in enumerate(valid_jobs):
            if idx in approved_indices:
                # RULE: Approved -> Move to CLUSTER queue
                job.status = JobStatus.PENDING
                job.queue_name = ArticlesQueueName.CLUSTER
                job.msg = None  # Clear any previous errors
                job.updated_at = datetime.now(timezone.utc)
                approved_count += 1
            else:
                # RULE: Rejected -> Mark COMPLETED (Dead End)
                job.status = JobStatus.COMPLETED
                job.msg = "Rejected by AI Filter"
                job.updated_at = datetime.now(timezone.utc)
                rejected_count += 1

        return approved_count, rejected_count
