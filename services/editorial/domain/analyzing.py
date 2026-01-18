import asyncio
from datetime import datetime, timezone
from typing import List, Optional
from loguru import logger

# Project Imports
from core.llm_parser import CloudNewsAnalyzer
from news_events_lib.models import ArticleModel, JobStatus
from core.models import ArticlesQueueModel
from config import Settings

class ContentAnalystDomain:
    def __init__(self):
        self.settings = Settings()
        self.llm = CloudNewsAnalyzer()

    async def analyze_batch(self, jobs: List[ArticlesQueueModel]):
        """
        Takes a batch of jobs (populated with data from the Miner),
        runs the LLM analysis, and updates the models in-place.
        """
        if not jobs:
            return

        # 1. Prepare Inputs from DB Models
        llm_inputs = []
        valid_jobs = []

        for job in jobs:
            article = job.article
            
            # Safety check: Ensure we have content to analyze
            content = article.contents[0].content if article.contents else ""
            if not content:
                job.msg = "Skipped: No Content"
                continue

            # Construct context for LLM
            # We assume the Miner has already cleaned and saved this data
            context_str = (
                f"Title: {article.title}\n"
                f"Date: {article.published_date}\n"
                f"Content: {content[:15000]}" # Cap context to prevent overflow
            )
            llm_inputs.append(context_str)
            valid_jobs.append(job)

        if not valid_jobs:
            return

        # 2. Call LLM (One Batch Request)
        logger.info(f"🧠 Analyst: Running LLM on {len(valid_jobs)} articles...")
        try:
            # This calls the method you already have in CloudNewsAnalyzer
            outputs = await self.llm.analyze_articles_batch(llm_inputs)

            # 3. Map Results Back to Models
            for job, output in zip(valid_jobs, outputs):
                if output.status == "valid":
                    self._apply_llm_data(job.article, output)
                    job.msg = "LLM Success" 
                elif output.status == "irrelevant":
                    job.msg = f"LLM Irrelevant: {output.error_message}"
                    # We flag it here so the worker knows to drop it
                    setattr(job, "_llm_status", "irrelevant") 
                else:
                    job.msg = f"LLM Failed: {output.error_message}"
                    setattr(job, "_llm_status", "failed")

        except Exception as e:
            logger.error(f"LLM Batch Failed: {e}")
            # Propagate error msg to all jobs in this batch so they can be retried or failed
            for job in valid_jobs:
                job.msg = f"LLM Batch Error: {str(e)[:100]}"
                setattr(job, "_llm_status", "failed")

    def _apply_llm_data(self, article: ArticleModel, llm_out):
        """Maps the Pydantic/JSON output from LLM to the SQLAlchemy Model."""
        article.summary = llm_out.summary
        article.stance = llm_out.stance
        article.key_points = llm_out.key_points
        article.summary_date = datetime.now(timezone.utc)
        article.summary_status = JobStatus.COMPLETED
        
        # Flatten entities dictionary to list if needed, or store as is
        # Adjust based on your ArticleModel definition for 'entities'
        if hasattr(llm_out, "entities") and isinstance(llm_out.entities, dict):
             article.entities = [f for i in llm_out.entities.values() for f in i]
        
        article.main_topics = llm_out.main_topics
        article.stance_reasoning = llm_out.stance_reasoning
        article.clickbait_score = llm_out.clickbait_score
        article.clickbait_reasoning = llm_out.clickbait_reasoning
        
        # Optional: LLM might suggest a better title
        if llm_out.title:
            article.title = llm_out.title
        if llm_out.subtitle:
            article.subtitle = llm_out.subtitle