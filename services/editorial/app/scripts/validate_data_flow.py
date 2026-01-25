import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from app.config import Settings  # noqa: E402
from app.services.editorial.domain.publishing import NewsPublisherDomain  # noqa: E402
from app.utils.article_manager import EventAggregator  # noqa: E402
from app.workers.cron_gardener import GardenerService  # noqa: E402
from loguru import logger
from news_events_lib.models import NewsEventModel  # noqa: E402
from news_events_lib.models import ArticleModel, EventStatus, NewspaperModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


async def validate_data_flow():
    """
    End-to-End Validation of the Editorial Data Flow:
    1. Aggregation: Articles -> Event Stats
    2. Publishing: Event Stats -> Score & Insights (Blind Spot)
    3. Maintenance: Time Passing -> Score Decay
    """
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    # Initialize Domains
    publisher = NewsPublisherDomain()
    gardener = GardenerService()

    # Test IDs
    event_id = uuid.uuid4()
    paper_id = uuid.uuid4()

    logger.info("🧪 Starting Editorial Data Flow Validation...")

    with SessionLocal() as session:
        try:
            # --- SETUP ---
            # 1. Create a Test Newspaper (Left Bias)
            paper = NewspaperModel(
                id=paper_id,
                name="The Daily Test",
                bias="left",
                description="test",
                ownership_type="independent",
                home_url="dailytest.com",
                icon_url="",
                logo_url="",
            )
            session.merge(paper)
            session.commit()

            # 2. Create a Draft Event
            event = NewsEventModel(
                id=event_id,
                title="[TEST] Data Flow Validation Event",
                status=EventStatus.DRAFT,
                is_active=True,
                created_at=datetime.now(timezone.utc),
                last_updated_at=datetime.now(timezone.utc),
                article_count=0,
                editorial_score=0.0,
            )
            session.add(event)
            session.commit()
            logger.info("✅ Setup: Draft Event & Newspaper created.")

            # --- PHASE 1: AGGREGATION ---
            # Add 3 articles with critical stance (-0.5) from Left source
            # This setup targets the "Blind Spot" logic (Single side, > 2 articles)
            articles = []
            for i in range(3):
                art = ArticleModel(
                    id=uuid.uuid4(),
                    event_id=event_id,
                    newspaper_id=paper_id,
                    title=f"Article {i}",
                    url=f"http://dailytest.com/{i}",
                    published_date=datetime.now(timezone.utc),
                    stance=-0.5,
                    source_rank=1,
                )
                articles.append(art)
                session.add(art)
            session.commit()

            # Simulate Aggregation Logic
            session.refresh(event)
            for art in articles:
                # Manually attach newspaper for aggregation context
                art.newspaper = paper
                EventAggregator.aggregate_basic_stats(event, art)
                EventAggregator.aggregate_stance(event, paper.bias, art.stance)
                EventAggregator.aggregate_bias_counts(event, art)
                EventAggregator.aggregate_metadata(event, art)

            session.commit()
            session.refresh(event)

            # Verify Aggregation Results
            if event.article_count != 3:
                raise AssertionError(
                    f"Aggregation Failed: Expected 3 articles, got {event.article_count}"
                )
            if event.stance != -0.5:
                raise AssertionError(
                    f"Aggregation Failed: Expected stance -0.5, got {event.stance}"
                )
            if event.article_counts_by_bias.get("left") != 3:
                raise AssertionError("Aggregation Failed: Bias counts incorrect")

            logger.info("✅ Phase 1: Aggregation Verified.")

            # --- PHASE 2: PUBLISHING ---
            # Publish the event. Expecting Blind Spot detection (Only Left, >= 3 articles)
            publisher.publish_event_direct(session, event, commit=True)
            session.refresh(event)

            if event.status != EventStatus.PUBLISHED:
                raise AssertionError("Publishing Failed: Status not PUBLISHED")
            if not event.is_blind_spot:
                raise AssertionError("Publishing Failed: Blind Spot not detected")
            if event.blind_spot_side != "left":
                raise AssertionError(
                    f"Publishing Failed: Wrong blind spot side {event.blind_spot_side}"
                )
            if "BLIND_SPOT" not in event.is_blind_spot:
                raise AssertionError("Publishing Failed: Missing BLIND_SPOT tag")

            initial_score = event.hot_score
            logger.info(
                f"✅ Phase 2: Publishing Verified. Score: {initial_score}, BlindSpot: {event.blind_spot_side}"
            )

            # --- PHASE 3: DECAY (GARDENER) ---
            # Simulate 48 hours passing
            event.first_article_date = datetime.now(timezone.utc) - timedelta(hours=48)
            session.commit()

            # Run Gardener Decay
            await gardener._run_score_decay(session)
            session.refresh(event)

            decayed_score = event.hot_score
            if decayed_score >= initial_score:
                raise AssertionError(
                    f"Decay Failed: Score did not decrease ({initial_score} -> {decayed_score})"
                )

            # Verify Blind Spot flags persisted/updated
            if not event.is_blind_spot:
                raise AssertionError("Decay Failed: Blind Spot flag lost during update")

            logger.info(f"✅ Phase 3: Decay Verified. Score dropped to {decayed_score}")

        except Exception as e:
            logger.error(f"❌ Validation Failed: {e}")
            raise
        finally:
            # Cleanup
            logger.info("🧹 Cleaning up test data...")
            session.query(ArticleModel).filter(
                ArticleModel.event_id == event_id
            ).delete()
            session.query(NewsEventModel).filter(NewsEventModel.id == event_id).delete()
            session.query(NewspaperModel).filter(NewspaperModel.id == paper_id).delete()
            session.commit()
            logger.success("✨ Validation Complete.")


if __name__ == "__main__":
    asyncio.run(validate_data_flow())
