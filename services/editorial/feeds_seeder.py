from config import Settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from news_events_lib.models import NewsEventModel, ArticleModel, NewspaperModel, FeedModel, BaseModel
from news_events_lib.schemas import NewspaperCreateSchema, FeedCreateSchema
import json
import os

settings = Settings()
feeds_path = settings.feeds_path

# Setup Database Connection
# Assuming 'settings' has a 'DATABASE_URL' attribute. 
# If your settings use a different name (e.g. SQLALCHEMY_DATABASE_URI), update it here.
engine = create_engine(str(settings.pg_dsn))
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def seed_newspapers_feeds(feeds_data):
    session = SessionLocal()
    try:
        for newspaper_data in feeds_data:
            # 1. Validate and Create Newspaper Schema
            newspaper_schema = NewspaperCreateSchema(
                name=newspaper_data["name"],
                bias=newspaper_data["bias"],
                icon_url=newspaper_data["icon_url"],
                logo_url=newspaper_data["logo_url"],
                description=newspaper_data["description"],
            )
            
            # 2. Create Newspaper DB Model
            # We use model_dump() (Pydantic v2) to unpack schema fields into the model
            newspaper_model = NewspaperModel(**newspaper_schema.model_dump())
            session.add(newspaper_model)
            session.flush()  # Flush to generate the ID for the newspaper so we can use it for feeds

            # 3. Create Feeds linked to the Newspaper
            for feed_data in newspaper_data.get("feeds", []):
                feed_schema = FeedCreateSchema(
                    url=feed_data["url"],
                    newspaper_id=newspaper_model.id,
                    blocklist=feed_data.get("blocklist"),
                    allowed_sections=feed_data.get("allowed_sections"),
                )
                feed_model = FeedModel(**feed_schema.model_dump())
                session.add(feed_model)
        
        session.commit()
        print("Seeding completed successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error seeding data: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    with open(feeds_path, "r") as f:
        feeds_data = json.load(f)
    seed_newspapers_feeds(feeds_data)
