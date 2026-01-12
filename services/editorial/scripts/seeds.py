import json
import os
import sys
import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Settings
from news_events_lib.models import NewspaperModel, FeedModel

def seed_feeds():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)
    
    json_path = os.path.join(os.path.dirname(__file__), "../data/feeds.json")
    with open(json_path, "r") as f:
        data = json.load(f)

    with SessionLocal() as session:
        print(f"🌱 Seeding {len(data)} newspapers...")
        
        for np_data in data:
            # 1. Upsert Newspaper
            stmt = select(NewspaperModel).where(NewspaperModel.name == np_data["name"])
            newspaper = session.execute(stmt).scalar_one_or_none()
            
            if not newspaper:
                newspaper = NewspaperModel(
                    name=np_data["name"],
                    bias=np_data.get("bias"),
                    ownership_type=np_data.get("ownership_type", ""),
                    icon_url=np_data.get("icon_url"),
                    logo_url=np_data.get("logo_url"),
                    description=np_data.get("description", ""),
             
                )
                session.add(newspaper)
                session.flush()
                print(f"   [+] Created Newspaper: {newspaper.name}")
            else:
                newspaper.bias = np_data.get("bias")
                newspaper.ownership_type = np_data.get("ownership_type", "")
                print(f"   [.] Updated Newspaper: {newspaper.name}")

            # 2. Upsert Feeds
            for feed_data in np_data.get("feeds", []):
                feed_url = feed_data["url"]
                
                stmt_feed = select(FeedModel).where(FeedModel.url == feed_url)
                feed = session.execute(stmt_feed).scalar_one_or_none()
                
                if not feed:
                    feed = FeedModel(
                        newspaper_id=newspaper.id,
                        url=feed_url,
                        feed_type=feed_data.get("feed_type", "sitemap"),
                        is_active=True
                    )
                    session.add(feed)
                
                # Update Configs
                feed.feed_type = feed_data.get("feed_type", "sitemap")
                feed.url_pattern = feed_data.get("url_pattern")
                feed.blocklist = feed_data.get("blocklist")
                feed.allowed_sections = feed_data.get("allowed_sections")
                
                # --- NEW FIELDS ---
                feed.is_ranked = feed_data.get("is_ranked", False)
                feed.use_browser_render = feed_data.get("use_browser_render", False)
                feed.scroll_depth = feed_data.get("scroll_depth", 1)
                
        session.commit()
        print("✅ Seeding complete.")

if __name__ == "__main__":
    seed_feeds()