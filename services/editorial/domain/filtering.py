from ast import Dict
from datetime import datetime, timezone
from typing import List, Tuple
from loguru import logger

# Models
from news_events_lib.models import (
    JobStatus,
    ArticlesQueueModel,
    ArticlesQueueName,
)

from config import Settings
from core.llm_parser import CloudNewsFilter
from services.backend import app



class NewsFilterDomain:
    def __init__(self):
        self.settings = Settings()
        # Initialize the LLM Service (from Core)
        self.llm_filter = CloudNewsFilter()

    async def execute_batch_filtering(
        self, jobs: List[ArticlesQueueModel]
    ) -> Tuple[List[Dict], List[ArticlesQueueModel]]:
        """
        Orchestrates the filtering process:
        1. Extracts titles from the batch.
        2. Calls the LLM to filter them.
        3. Updates the Database states based on the result.

        Returns: (approved_count, rejected_count)
        """
        if not jobs:
            return [], []

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
            return [], []

        # 2. Call AI (The "Gatekeeper")
        try:
            logger.info(f"🧠 Filtering batch of {len(titles)} articles...")
            approved_indices = await self.llm_filter.filter_batch(titles)
        except Exception as e:
            raise e

        # 3. Apply Business Rules (State Transitions)
        approved = []
        rejected = []

        for idx, job in enumerate(valid_jobs):
            now = datetime.now(timezone.utc)
            job.attempts += 1
            job.updated_at = now

            if idx in approved_indices:
                # RULE: Approved -> Move to ENRICH queue
                job.status = JobStatus.COMPLETED
                job.msg = "Approved by AI Filter"

                forward_job = ArticlesQueueModel(
                    article_id=job.article.id,
                    status=JobStatus.PENDING,
                    queue_name=ArticlesQueueName.ENRICHER,
                    created_at=now,
                    updated_at=now,
                    attempts=0,
                    msg="Approved by AI Filter"
                )

                approved.append({"approved": job, "forward": forward_job})

            else:
                # RULE: Rejected -> Mark COMPLETED (Dead End)
                job.status = JobStatus.COMPLETED
                job.msg = "Rejected by AI Filter"
                rejected.append(job)

        return approved, rejected
