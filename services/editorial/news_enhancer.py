import asyncio
import sys
import os
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import create_engine, select, update, func
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger

# Add common to path if needed, though usually handled by env
from base_worker import BaseQueueWorker
from event_aggregator import EventAggregator

# Project Imports
from config import Settings
from models import EventsQueueModel, EventsQueueName
from llm_parser import CloudNewsAnalyzer, LLMNewsOutputSchema

from news_events_lib.models import NewsEventModel, ArticleModel, JobStatus


class NewsEnhancerWorker(BaseQueueWorker):
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.enhancer = CloudNewsAnalyzer()
        
        # Allow some concurrency for article processing within an event
        self.semaphore = asyncio.Semaphore(5) 

        super().__init__(
            session_maker=self.SessionLocal,
            queue_model=EventsQueueModel,
            target_queue_name=EventsQueueName.ENHANCER,
            batch_size=5,
            pending_status=JobStatus.PENDING
        )

    async def run(self):
        """
        Overrides BaseQueueWorker.run to implement a Streaming/Producer-Consumer pattern.
        """
        logger.info(f"🚀 Worker started (Streaming Mode)")
        self._reset_stuck_tasks()
        self.queue = asyncio.Queue(maxsize=1)
        
        # Start Consumers (Staggered)
        workers = []
        for i in range(1):
            workers.append(asyncio.create_task(self._consumer_loop(i)))
            await asyncio.sleep(1.0)
        
        while True:
            try:
                if self.queue.full():
                    await asyncio.sleep(1.0)
                    continue

                with self.SessionLocal() as session:
                    jobs = self._fetch_jobs(session)
                    
                    if not jobs:
                        logger.info('No more jobs found. Waiting for queue to drain...')
                        await self.queue.join()
                        logger.info('Queue drained. Stopping workers...')
                        for w in workers:
                            w.cancel()
                        await asyncio.gather(*workers, return_exceptions=True)
                        logger.info('All workers stopped. Exiting.')
                        return
                    
                    for job in jobs:
                        await self.queue.put(job.id)
                        
            except Exception as e:
                logger.error(f"Producer Error: {e}")
                await asyncio.sleep(5)

    def _reset_stuck_tasks(self):
        with self.SessionLocal() as session:
            result = session.execute(
                update(EventsQueueModel)
                .where(
                    EventsQueueModel.status == JobStatus.PROCESSING,
                    EventsQueueModel.queue_name == self.queue_name
                )
                .values(status=JobStatus.PENDING)
            )
            session.commit()
            if result.rowcount > 0:
                logger.info(f"🧹 Reset {result.rowcount} stuck 'processing' enhancer jobs.")

    async def _consumer_loop(self, worker_id):
        while True:
            job_id = await self.queue.get()
            try:
                with self.SessionLocal() as session:
                    job = session.get(EventsQueueModel, job_id)
                    if job:
                        await self.process_item(session, job)
                        session.commit()
            except Exception as e:
                logger.error(f"Worker {worker_id} error on job {job_id}: {e}")
            finally:
                self.queue.task_done()

    async def _analyze_wrapper(self, text: str) -> Optional[LLMNewsOutputSchema]:
        """
        Wraps the LLM call with a Semaphore for concurrency control.
        Retries are handled by the decorator in llm_parser.
        """
        async with self.semaphore:
            return await self.enhancer.analyze_article(text)
            
    def _fetch_jobs(self, session):
        # Override to join NewsEventModel
        stmt = (
            select(EventsQueueModel, NewsEventModel)
            .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
            .where(
                EventsQueueModel.status == self.pending_status,
                EventsQueueModel.queue_name == self.queue_name,
            )
            .order_by(EventsQueueModel.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = session.execute(stmt).all()

        jobs = []
        for queue_item, event in rows:
            queue_item.status = JobStatus.PROCESSING
            queue_item.updated_at = datetime.now(timezone.utc)
            queue_item.event = event # Attach event to job
            jobs.append(queue_item)
        
        session.commit()
        return jobs

    async def process_item(self, session, job):
        event = job.event
        
        # Limit articles per run to prevent API exhaustion on large events
        BATCH_LIMIT = 10

        # Fetch articles
        articles = (
            session.query(ArticleModel)
            .filter(ArticleModel.event_id == event.id)
            .options(joinedload(ArticleModel.contents))
            .options(joinedload(ArticleModel.newspaper))
            .filter(ArticleModel.summary_status == JobStatus.PENDING)
            .limit(BATCH_LIMIT)
            .all()
        )

        if not articles:
            if event.article_count == 0:
                job.status = JobStatus.FAILED
                job.msg = "No articles found"
            else:
                # Nothing new to enhance, move to publisher
                job.status = JobStatus.PENDING
                job.queue_name = EventsQueueName.PUBLISHER
            return

        logger.info(f"Enhancing '{event.title}' ({len(articles)} pending articles)...")

        # Process Articles
        tasks = []
        for article in articles:
            if not article.contents: 
                tasks.append(asyncio.sleep(0)) # No-op
                continue
            tasks.append(self._analyze_wrapper(article.contents[0].content))

        results = await asyncio.gather(*tasks)

        # Update DB
        new_summaries_list = []

        for article, result in zip(articles, results):
            if not result or isinstance(result, int):
                # Mark as failed to avoid infinite loops in partial processing
                article.summary_status = JobStatus.FAILED
                session.add(article)
                continue

            # Update Article
            article.main_topics = result.main_topics
            article.interests = result.entities 
            
            all_entities = []
            if result.entities:
                for entity_list in result.entities.values():
                    all_entities.extend(entity_list)
            article.entities = list(set(all_entities))

            article.summary = result.summary
            article.key_points = result.key_points
            article.stance = result.stance
            article.stance_reasoning = result.stance_reasoning
            article.clickbait_score = result.clickbait_score
            article.clickbait_reasoning = result.clickbait_reasoning
            
            article.summary_status = JobStatus.COMPLETED
            article.summary_date = datetime.now(timezone.utc)
            session.add(article)

            new_summaries_list.append({
                "bias": article.newspaper.bias,
                "key_points": result.key_points
            })

            # Aggregate Interests (using LLM entities)
            EventAggregator.aggregate_interests(event, result.entities)
            
            # Aggregate Stance
            EventAggregator.aggregate_stance(event, article.newspaper.bias, result.stance)

            # Aggregate Clickbait
            EventAggregator.aggregate_clickbait(event, article.newspaper.bias, result.clickbait_score)

        session.add(event)
        session.flush()

        # Event Summary
        if new_summaries_list:
            event_summary = await self.enhancer.summarize_event(
                new_summaries_list, 
                event.summary 
            )
            if event_summary:
                event.summary = event_summary["subtitle"]
                event.key_points = event_summary["summary"]
                event.title = event_summary["title"]
                event.last_summarized_at = datetime.now(timezone.utc)
                event.articles_at_last_summary = event.article_count 
                session.add(event)

        # Check if there are more pending articles for this event
        remaining_count = session.query(func.count(ArticleModel.id)).filter(
            ArticleModel.event_id == event.id,
            ArticleModel.summary_status == JobStatus.PENDING
        ).scalar()

        if remaining_count > 0:
            # Keep in Enhancer loop to process next batch
            job.status = JobStatus.PENDING
            job.queue_name = EventsQueueName.ENHANCER
            job.updated_at = datetime.now(timezone.utc)
            logger.info(f"🔄 Partial Enhancement: {event.title} ({remaining_count} remaining)")
        else:
            # All done, move to Publisher
            job.status = JobStatus.PENDING
            job.queue_name = EventsQueueName.PUBLISHER
            job.updated_at = datetime.now(timezone.utc)
            logger.success(f"✅ Enhanced: {event.title} (Complete)")

if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    asyncio.run(worker.run())