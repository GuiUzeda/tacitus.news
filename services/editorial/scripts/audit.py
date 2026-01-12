import sys
import os
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from ..config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel

# Setup DB Connection
settings = Settings()
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(bind=engine)

def audit_largest_events():
    with SessionLocal() as session:
        # 1. Find Top 10 Largest Active Events
        print(f"{'ID':<38} | {'COUNT':<5} | {'TITLE'}")
        print("-" * 80)
        
        stmt = (
            select(NewsEventModel)
            .where(NewsEventModel.is_active == True)
            .order_by(NewsEventModel.article_count.desc())
            .limit(10)
        )
        events = session.scalars(stmt).all()
        
        if not events:
            print("No active events found.")
            return

        for ev in events:
            print(f"{str(ev.id):<38} | {ev.article_count:<5} | {ev.title}")

        # 2. Deep Dive into the #1 Largest Event
        largest = events[0]
        print(f"\n\n🔍 DEEP DIVE: '{largest.title}' (ID: {largest.id}) ) ({largest.created_at})")
        print("Sampling first 20 article titles inside this event:\n")
        
        articles = session.scalars(
            select(ArticleModel)
            .where(ArticleModel.event_id == largest.id)
            .order_by(ArticleModel.published_date.desc())
            .limit(100)
        ).all()
        
        for i, article in enumerate(articles):
            print(f"{i+1}. {article.title} ({article.published_date})")

if __name__ == "__main__":
    audit_largest_events()