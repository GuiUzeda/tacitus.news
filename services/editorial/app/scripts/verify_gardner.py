import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to path to allow imports
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from app.config import Settings  # noqa: E402
from app.core.models import EventsQueueModel  # noqa: E402
from app.workers.cron_gardener import GardenerService  # noqa: E402
from news_events_lib.models import EventStatus, JobStatus, NewsEventModel  # noqa: E402


async def verify_gardener_tasks():
    """
    Verifies that GardenerService correctly handles:
    1. Archiving stale events (Archivist)
    2. Decaying hot scores (Score Decay)
    3. Cleaning old queue items (Janitor)
    """
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    gardener = GardenerService()

    # Generate unique IDs to avoid collisions
    stale_id = uuid.uuid4()
    decay_id = uuid.uuid4()
    queue_id = uuid.uuid4()

    logger.info("🧪 Preparing test data...")

    with SessionLocal() as session:
        # 1. Stale Event: > 7 days old, low score -> Should Archive
        stale_event = NewsEventModel(
            id=stale_id,
            title="[TEST] Stale Event",
            status=EventStatus.PUBLISHED,
            is_active=True,
            hot_score=10.0,
            editorial_score=10.0,
            article_count=5,
            last_updated_at=datetime.now(timezone.utc) - timedelta(days=8),
            created_at=datetime.now(timezone.utc) - timedelta(days=8),
        )

        # 2. Decay Event: Recent, artificially high score -> Should Decay
        decay_event = NewsEventModel(
            id=decay_id,
            title="[TEST] Decay Event",
            status=EventStatus.PUBLISHED,
            is_active=True,
            hot_score=999.0,  # Impossible high score
            editorial_score=50.0,
            article_count=5,
            last_updated_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )

        # 3. Old Queue Item: > 3 days old, Completed -> Should Delete
        old_queue_item = EventsQueueModel(
            id=queue_id,
            status=JobStatus.COMPLETED,
            updated_at=datetime.now(timezone.utc) - timedelta(days=5),
            payload={"test": "data"},
            created_at=datetime.now(timezone.utc) - timedelta(days=5),
        )

        session.add_all([stale_event, decay_event, old_queue_item])
        session.commit()
        logger.info("✅ Test data inserted.")

    try:
        logger.info("🚜 Running Gardener Cycle...")
        # Run the service
        with SessionLocal() as session:
            await gardener.run_cycle(session)

        logger.info("🔍 Verifying results...")
        with SessionLocal() as session:
            # Verify Archivist
            e_stale = session.get(NewsEventModel, stale_id)
            if e_stale.status == EventStatus.ARCHIVED and not e_stale.is_active:
                logger.success("✅ Archivist: Stale event archived successfully.")
            else:
                logger.error(
                    f"❌ Archivist Failed: Status={e_stale.status}, Active={e_stale.is_active}"
                )

            # Verify Score Decay
            e_decay = session.get(NewsEventModel, decay_id)
            if e_decay.hot_score < 999.0:
                logger.success(
                    f"✅ Score Decay: Score reduced from 999.0 to {e_decay.hot_score}"
                )
            else:
                logger.error("❌ Score Decay Failed: Score did not change.")

            # Verify Janitor
            q_item = session.get(EventsQueueModel, queue_id)
            if q_item is None:
                logger.success("✅ Janitor: Old queue item deleted successfully.")
            else:
                logger.error("❌ Janitor Failed: Queue item still exists.")

    finally:
        # Cleanup
        logger.info("🧹 Cleaning up test artifacts...")
        with SessionLocal() as session:
            session.query(NewsEventModel).filter(
                NewsEventModel.id.in_([stale_id, decay_id])
            ).delete(synchronize_session=False)
            session.query(EventsQueueModel).filter(
                EventsQueueModel.id == queue_id
            ).delete(synchronize_session=False)
            session.commit()


if __name__ == "__main__":
    asyncio.run(verify_gardener_tasks())
