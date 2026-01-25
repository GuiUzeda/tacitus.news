import json
import os
import sys

from app.config import Settings  # noqa: E402
from news_events_lib.models import NewspaperModel  # noqa: E402
from news_events_lib.models import FeedModel, RateLimitConfigModel, RateLimitMetric
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


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
                        is_active=True,
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
        print("✅ Feeds seeding complete.")


def seed_rate_limits():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    json_path = os.path.join(os.path.dirname(__file__), "../data/models.json")
    if not os.path.exists(json_path):
        print(f"⚠️ Rate Limit Seed file not found: {json_path}")
        return

    with open(json_path, "r") as f:
        data = json.load(f)

    with SessionLocal() as session:
        print(f"🌱 Seeding Rate Limits for {len(data)} models...")

        for model_data in data:
            model_id = model_data["model_id"]

            for limit in model_data["limits"]:
                metric_str = limit["metric"].lower()
                try:
                    metric = RateLimitMetric(metric_str)
                except ValueError:
                    print(f"   [!] Invalid Metric: {metric_str} for {model_id}")
                    continue

                stmt = select(RateLimitConfigModel).where(
                    RateLimitConfigModel.model_id == model_id,
                    RateLimitConfigModel.metric == metric,
                )
                config = session.execute(stmt).scalar_one_or_none()

                if not config:
                    config = RateLimitConfigModel(
                        model_id=model_id,
                        metric=metric,
                        window_seconds=limit["window_seconds"],
                        limit_value=limit["limit_value"],
                    )
                    session.add(config)
                    print(f"   [+] Created Rule: {model_id} | {metric_str}")
                else:
                    config.window_seconds = limit["window_seconds"]
                    config.limit_value = limit["limit_value"]
                    print(f"   [.] Updated Rule: {model_id} | {metric_str}")

        session.commit()
        print("✅ Rate Limits seeding complete.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "limits":
        seed_rate_limits()
    else:
        seed_feeds()
        seed_rate_limits()
