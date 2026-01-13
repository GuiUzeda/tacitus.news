import sys
import os
from datetime import datetime, timezone
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Add service root to path to allow imports (services/editorial)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to add common to path if not present (assuming standard repo structure)
# project_root/services/editorial/scripts/script.py -> project_root/common
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
common_path = os.path.join(project_root, "common")
if common_path not in sys.path:
    sys.path.append(common_path)

from config import Settings
from news_events_lib.models import ArticleModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName

def main():
    print("🔧 Tacitus: Requeue Untitled Articles Script")
    
    try:
        settings = Settings()
    except Exception as e:
        print(f"❌ Could not load Settings: {e}")
        return

    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        print("🔍 Scanning for articles with 'No Title' or 'Unknown'...")
        
        # Case-insensitive check for placeholder titles
        stmt = select(ArticleModel.id).where(
            func.lower(ArticleModel.title).in_(['no title', 'unknown'])
        )
        article_ids = session.scalars(stmt).all()
        
        if not article_ids:
            print("✅ No untitled articles found.")
            return

        print(f"⚠️  Found {len(article_ids)} untitled articles.")
        
        # Upsert into Queue
        now = datetime.now(timezone.utc)
        count = 0
        
        for aid in article_ids:
            stmt = insert(ArticlesQueueModel).values(
                article_id=aid,
                queue_name=ArticlesQueueName.ENRICH,
                status=JobStatus.PENDING,
                created_at=now,
                updated_at=now,
                attempts=0,
                msg="Manual Requeue: Untitled"
            ).on_conflict_do_update(
                index_elements=['article_id'],
                set_={
                    "queue_name": ArticlesQueueName.ENRICH,
                    "status": JobStatus.PENDING,
                    "updated_at": now,
                    "attempts": 0,
                    "msg": "Manual Requeue: Untitled"
                }
            )
            session.execute(stmt)
            count += 1
            
        session.commit()
        print(f"🚀 Successfully re-queued {count} articles to ENRICH queue.")

if __name__ == "__main__":
    main()