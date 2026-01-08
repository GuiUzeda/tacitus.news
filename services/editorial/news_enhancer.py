import asyncio
import sys
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

# Add parent dir to path to find modules (similar to other scripts)
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "common",
    )
)

from sqlalchemy import create_engine, select, update, func
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger

# Project Imports
from config import Settings
from models import EventsQueueModel, EventsQueueName
from llm_parser import CloudNewsAnalyzer, LLMNewsOutputSchema

from news_events_lib.models import NewsEventModel, ArticleModel, JobStatus


class NewsEnhancerWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.enhancer = CloudNewsAnalyzer()

        self.BATCH_SIZE = 5
        self.semaphore = asyncio.Semaphore(1) 

    async def run(self):
        logger.info("🚀 News Enhancer Worker Started (Free Tier Mode)")
        while True:
            try:
                processed_count = await self.process_batch()
                if processed_count == 0:
                    logger.debug("Queue empty, sleeping...")
                    await asyncio.sleep(10)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)

    async def _analyze_with_retry(self, text: str) -> Optional[LLMNewsOutputSchema]:
        """
        Wraps the LLM call with a Semaphore and a robust Retry loop.
        """
        async with self.semaphore:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return await self.enhancer.analyze_article(text)
                except Exception as e:
                    error_msg = str(e).lower()
                    if "429" in error_msg or "resource_exhausted" in error_msg:
                        wait_time = 60 
                        logger.warning(f"⚠️ Quota Exceeded (Attempt {attempt+1}/{max_retries}). Sleeping {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(f"LLM Error: {e}")
                    return None
            return None
            
    def _map_stance_to_bucket(self, score: float) -> str:
        if score <= -0.35:
            return "critical"
        elif score >= 0.35:
            return "supportive"
        return "neutral"

    async def process_batch(self) -> int:
        processed = 0

        # 1. Fetch & Lock Batch
        with self.SessionLocal() as session:
            stmt = (
                select(EventsQueueModel, NewsEventModel)
                .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
                .where(
                    EventsQueueModel.status == JobStatus.PENDING,
                    EventsQueueModel.queue_name == EventsQueueName.ENHANCER,
                )
                .order_by(EventsQueueModel.created_at.asc())
                .limit(self.BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            jobs = session.execute(stmt).all()

            if not jobs:
                return 0

            for queue_item, _ in jobs:
                queue_item.status = JobStatus.PROCESSING
                queue_item.updated_at = datetime.utcnow()
            session.commit()
            
            logger.info(f"Processing batch of {len(jobs)} events")

        # 2. Process Jobs
        for queue_item, event_ref in jobs:
            with self.SessionLocal() as session:
                try:
                    job = session.get(EventsQueueModel, queue_item.id)
                    event = session.get(NewsEventModel, event_ref.id)

                    if not event or not job:
                        continue

                    articles = (
                        session.query(ArticleModel)
                        .filter(ArticleModel.event_id == event.id)
                        .options(joinedload(ArticleModel.contents))
                        .options(joinedload(ArticleModel.newspaper))
                        .filter(ArticleModel.summary_status == JobStatus.PENDING)
                        .all()
                    )

                    if not articles:
                        if event.article_count == 0:
                            job.status = JobStatus.FAILED
                            job.msg = "No articles found"
                        else:
                            job.status = JobStatus.COMPLETED
                        session.commit()
                        continue

                    logger.info(f"Enhancing '{event.title}' ({len(articles)} pending articles)...")

                    # --- CONCURRENCY STEP ---
                    tasks = []
                    for article in articles:
                        if not article.contents: 
                            tasks.append(asyncio.sleep(0))
                            continue
                        tasks.append(self._analyze_with_retry(article.contents[0].content))

                    results = await asyncio.gather(*tasks)

                    # 3. Process Results & Update DB
                    new_summaries_list = []
                    
                    # Prepare mutable dictionaries for event aggregation
                    # We create copies to ensure SQLAlchemy detects changes
                    event_interest_counts = dict(event.interest_counts or {})
                    event_stance_dist = dict(event.stance_distribution or {})
                    
                    for article, result in zip(articles, results):
                        if not result or isinstance(result, int): 
                            continue

                        # A. Update Article
                        article.main_topics = result.main_topics
                        article.interests = result.entities 
                        
                        # Flatten entities for legacy ARRAY column
                        all_entities = []
                        if result.entities:
                            for entity_list in result.entities.values():
                                all_entities.extend(entity_list)
                        article.entities = list(set(all_entities))

                        article.summary = result.summary
                        article.key_points = result.key_points
                        article.stance = result.stance # Now Float
                        article.stance_reasoning = result.stance_reasoning
                        
                        article.summary_status = JobStatus.COMPLETED
                        article.summary_date = datetime.utcnow()
                        session.add(article)

                        # B. Collect for Summary Generation
                        new_summaries_list.append({
                            "bias": article.newspaper.bias,
                            "key_points": result.key_points
                        })

                        # C. AGGREGATE TO EVENT (Mandatory Step)
                        # 1. Interests
                        if result.entities:
                            for category, items in result.entities.items():
                                if category not in event_interest_counts:
                                    event_interest_counts[category] = {}
                                for item in items:
                                    event_interest_counts[category][item] = event_interest_counts[category].get(item, 0) + 1
                        
                        # 2. Stance Distribution
                        # Map float stance to bucket
                        bias = article.newspaper.bias
                        bucket = self._map_stance_to_bucket(result.stance)
                        
                        if bias not in event_stance_dist:
                            event_stance_dist[bias] = {}
                        
                        event_stance_dist[bias][bucket] = event_stance_dist[bias].get(bucket, 0) + 1

                    # Apply aggregated stats to event
                    event.interest_counts = event_interest_counts
                    event.stance_distribution = event_stance_dist
                    session.add(event)
                    
                    session.flush()

                    # --- EVENT SUMMARY STEP ---
                    if new_summaries_list:
                        event_summary = await self.enhancer.summarize_event(
                            new_summaries_list, 
                            event.summary 
                        )
                        
                        if event_summary:
                            event.summary = event_summary
                            event.last_summarized_at = datetime.utcnow()
                            event.articles_at_last_summary = event.article_count 
                            session.add(event)

                    # Finish Job
                    job.status = JobStatus.PROCESSING
                    job.queue_name = EventsQueueName.PUBLISHER
                    job.updated_at = datetime.utcnow()
                    session.commit()
                    
                    processed += 1
                    logger.success(f"✅ Enhanced: {event.title} (+{len(new_summaries_list)} articles)")

                except Exception as e:
                    logger.error(f"Failed to enhance event {event_ref.id}: {e}")
                    with self.SessionLocal() as error_session:
                        failed_job = error_session.get(EventsQueueModel, queue_item.id)
                        if failed_job:
                            failed_job.status = JobStatus.FAILED
                            failed_job.msg = str(e)[:255]
                            error_session.commit()

        return processed

if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")