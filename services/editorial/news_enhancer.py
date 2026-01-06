import asyncio
import sys
import os
import time
from datetime import datetime
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

        # Batch size for processing events
        self.BATCH_SIZE = 5

    async def run(self):
        logger.info("🚀 News Enhancer Worker Started")

        while True:
            try:
                processed_count = await self.process_batch()

                if processed_count == 0:
                    # Sleep if queue is empty
                    logger.debug("Queue empty, sleeping...")
                    await asyncio.sleep(10)
                else:
                    # Small pause between batches
                    await asyncio.sleep(1)

            except Exception as e:
                logger.critical(f"Worker crashed: {e}")
                await asyncio.sleep(30)  # Cool down

    async def process_batch(self) -> int:
        processed = 0

        # 1. Fetch & Lock Batch
        with self.SessionLocal() as session:
            # Select pending jobs for ENHANCER queue
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

            # Mark as processing immediately
            for queue_item, _ in jobs:
                queue_item.status = JobStatus.PROCESSING
                queue_item.updated_at = datetime.utcnow()

            session.commit()

            logger.info(f"Processing batch of {len(jobs)} events")

        # 2. Process Each Job (Can be parallelized, but sequential for now to respect rate limits)
        for queue_item, event_ref in jobs:
            # Re-open session per job to keep transactions small
            with self.SessionLocal() as session:
                try:
                    # Re-fetch objects attached to this session
                    job = session.get(EventsQueueModel, queue_item.id)
                    event = session.get(NewsEventModel, event_ref.id)

                    if not event or not job:
                        continue

                    # Fetch articles linked to this event
                    # We need the content/summary to feed the LLM
                    # Optimization: Limit to top 20 articles to avoid context overflow?
                    # For now, let's grab all and let Enhancer truncate if needed.
                    articles = (
                        session.query(ArticleModel)
                        .filter(ArticleModel.event_id == event.id)
                        .options(joinedload(ArticleModel.contents))
                        .options(joinedload(ArticleModel.newspaper))
                        .all()
                    )

                    if not articles:
                        logger.warning(
                            f"Event {event.id} has no articles. Marking failed."
                        )
                        job.status = JobStatus.FAILED
                        job.msg = "No linked articles found"
                        session.commit()
                        continue

                    # ENHANCE
                    logger.info(
                        f"Enhancing '{event.title}' ({len(articles)} articles)..."
                    )
                    article_summaries = []
                    for article in articles:
                        article_summary = {
                            "bias": article.newspaper.bias, 
                            "key_points": [""]
                        }
                        article_summaries.append(article_summary)
                        if article.summary_status != JobStatus.PENDING:
                            article_summary["key_points"] = article.key_points
                     
                            continue

                        result: LLMNewsOutputSchema | None = (
                            await self.enhancer.analyze_article(
                                article.contents[0].content
                            )
                        )
                        if not result:
                            continue
                        article_summary["key_points"] = result.key_points
                        article.main_topics = result.main_topics
                        article.entities = result.entities
                        article.summary = result.summary
                        article.key_points = result.key_points
                        article.stance_label = result.stance
                        article.stance_reasoning = result.stance_reasoning
                        article.summary_status = JobStatus.COMPLETED
                        article.summary_date = datetime.utcnow()

                        session.add(article)
                        session.flush()

                    # UPDATE EVENT
                    event_summary = await self.enhancer.summarize_event(
                                article_summaries, event.summary
                            )
                    event.articles_at_last_summary = len(articles)
                    if  event_summary:
                        
                    
                        event.summary =event_summary
                        event.last_summarized_at = datetime.utcnow()
                    session.refresh(event)

                    # We might want to update the 'status' Enum on NewsEventModel too if it exists
                    # Assuming NewsEventModel has a 'status' field (e.g. 'enhanced', 'published')
                    if hasattr(event, "status"):
                        # event.status = "enhanced" # Or appropriate enum value
                        pass

                    # UPDATE JOB
                    job.status = JobStatus.COMPLETED
                    job.updated_at = datetime.utcnow()

                    session.commit()
                    processed += 1
                    logger.success(f"✅ Enhanced: {event.title}")

                except Exception as e:
                    logger.error(f"Failed to enhance event {event_ref.id}: {e}")
                    # Capture error in job
                    with self.SessionLocal() as error_session:
                        failed_job = error_session.get(EventsQueueModel, queue_item.id)
                        if failed_job:
                            failed_job.status = JobStatus.FAILED
                            failed_job.msg = str(e)[:255]
                            failed_job.updated_at = datetime.utcnow()
                            error_session.commit()

        return processed


if __name__ == "__main__":
    worker = NewsEnhancerWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
