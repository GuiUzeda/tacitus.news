import sys
import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker

# Add service root to path to allow imports (services/editorial)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to add common to path if not present
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
common_path = os.path.join(project_root, "common")
if common_path not in sys.path:
    sys.path.append(common_path)

from config import Settings
from news_events_lib.models import NewsEventModel, JobStatus
from core.models import EventsQueueModel

def main():
    print("🧹 Tacitus: Clean Old Events from Queue")
    
    try:
        settings = Settings()
    except Exception as e:
        print(f"❌ Could not load Settings: {e}")
        return

    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    try:
        days_str = input("Enter max event age in days (default 7): ").strip()
        days = int(days_str) if days_str else 7
    except ValueError:
        print("Invalid number. Using 7 days.")
        days = 7

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    print(f"📅 Cleaning events created before: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    with SessionLocal() as session:
        # Find pending queue items linked to old events
        # We join EventsQueueModel -> NewsEventModel to check the EVENT'S creation date
        stmt = (
            select(EventsQueueModel.id)
            .join(NewsEventModel, EventsQueueModel.event_id == NewsEventModel.id)
            .where(
                EventsQueueModel.status == JobStatus.PENDING,
                NewsEventModel.created_at < cutoff_date
            )
        )
        
        queue_ids = session.scalars(stmt).all()
        
        if not queue_ids:
            print("✅ No old events found in the pending queue.")
            return

        print(f"⚠️  Found {len(queue_ids)} pending jobs for old events.")
        confirm = input("Mark them as COMPLETED (skip processing)? (y/n): ").lower()
        
        if confirm == 'y':
            update_stmt = (
                update(EventsQueueModel)
                .where(EventsQueueModel.id.in_(queue_ids))
                .values(
                    status=JobStatus.COMPLETED,
                    msg=f"Cleanup: Event older than {days} days",
                    updated_at=datetime.now(timezone.utc)
                )
            )
            session.execute(update_stmt)
            session.commit()
            print(f"🚀 Successfully cleaned {len(queue_ids)} jobs.")
        else:
            print("Operation cancelled.")

if __name__ == "__main__":
    main()