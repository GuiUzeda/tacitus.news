import sys
import os
from datetime import datetime

# Add project root to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger
from tqdm import tqdm

from config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel
from domain.aggregator import EventAggregator

def recalculate_aggregations():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        logger.info("Fetching active events...")
        # Fetch events with articles and newspapers loaded to avoid N+1 queries
        stmt = (
            select(NewsEventModel)
            .where(NewsEventModel.is_active == True)
            .options(
                joinedload(NewsEventModel.articles).joinedload(ArticleModel.newspaper)
            )
        )
        events = session.scalars(stmt).unique().all()
        
        logger.info(f"Found {len(events)} active events. Starting recalculation...")
        
        for event in tqdm(events):
            # 1. Reset Stats to Zero/Empty
            event.article_count = 0
            event.editorial_score = 0.0
            event.interest_counts = {}
            event.main_topic_counts = {}
            event.article_counts_by_bias = {}
            event.sources_snapshot = {}
            event.ownership_stats = {}
            event.bias_distribution = {}
            event.stance = 0.0
            event.stance_distribution = {}
            event.clickbait_distribution = {}
            
            # Reset Dates & Rank to allow re-discovery from articles
            event.first_article_date = None
            event.last_article_date = None
            event.best_source_rank = None

            # 2. Re-process Articles
            # Sort by date to ensure consistent timeline reconstruction
            sorted_articles = sorted(
                event.articles, 
                key=lambda x: x.published_date or datetime.min
            )

            for article in sorted_articles:
                # Basic Stats (Count, Dates, Rank, Score)
                EventAggregator.aggregate_basic_stats(event, article)
                
                # Metadata Aggregations
                EventAggregator.aggregate_interests(event, article.interests)
                EventAggregator.aggregate_main_topics(event, article.main_topics)
                EventAggregator.aggregate_metadata(event, article)
                EventAggregator.aggregate_bias_counts(event, article)
                EventAggregator.aggregate_source_snapshot(event, article)
                
                # Stance & Clickbait (Requires Newspaper)
                if article.newspaper:
                    EventAggregator.aggregate_stance(
                        event, 
                        article.newspaper.bias, 
                        article.stance or 0.0
                    )
                    EventAggregator.aggregate_clickbait(
                        event, 
                        article.newspaper.bias, 
                        article.clickbait_score or 0.0
                    )

        logger.info("Committing changes...")
        session.commit()
        logger.success("Successfully recalculated aggregations for all active events.")

if __name__ == "__main__":
    recalculate_aggregations()