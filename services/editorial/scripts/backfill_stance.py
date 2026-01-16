import sys
import os

# Add project root to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger
from tqdm import tqdm

from config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel
from domain.aggregator import EventAggregator

def backfill_stance():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        logger.info("Fetching active events for stance backfill...")
        # Fetch events with articles and newspapers loaded
        stmt = (
            select(NewsEventModel)
            .where(NewsEventModel.is_active == True)
            .options(
                joinedload(NewsEventModel.articles).joinedload(ArticleModel.newspaper)
            )
        )
        events = session.scalars(stmt).unique().all()
        
        logger.info(f"Found {len(events)} active events. Starting backfill...")
        
        for event in tqdm(events):
            # 1. Reset Stance Stats
            event.stance = 0.0
            event.stance_distribution = {}
            
            # 2. Re-aggregate from articles
            for article in event.articles:
                if article.newspaper and article.stance is not None:
                    EventAggregator.aggregate_stance(
                        event, 
                        article.newspaper.bias, 
                        article.stance
                    )
        
        session.commit()
        logger.success("✅ Stance backfill complete.")

if __name__ == "__main__":
    backfill_stance()