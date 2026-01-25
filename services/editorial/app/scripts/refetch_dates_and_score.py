import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import aiohttp
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import joinedload, sessionmaker

# Add service root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from app.config import Settings  # noqa: E402
from app.core.browser import BrowserFetcher  # noqa: E402
from app.core.llm_parser import CloudNewsAnalyzer  # noqa: E402
from app.services.editorial.domain.publishing import NewsPublisherDomain  # noqa: E402
from app.workers.enricher.domain import ContentEnricherDomain  # noqa: E402
from news_events_lib.models import NewsEventModel, NewspaperModel  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
MISSES_FILE = os.path.join(DATA_DIR, "regex_misses.json")


async def fetch_html(session, url):
    """Async fetch with Browser Fallback"""
    html = None
    use_browser = False
    try:
        headers = {
            "User-Agent": BrowserFetcher.USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        async with session.get(url, headers=headers, timeout=15) as response:
            if response.status == 200:
                html = await response.text(errors="replace")
                if not html or len(html) < 500:
                    use_browser = True
            elif response.status in [403, 429, 503]:
                use_browser = True
            else:
                logger.warning(f"HTTP {response.status} for {url}")
    except Exception as e:
        logger.warning(f"Fetch failed for {url}: {e}")
        use_browser = True

    if use_browser:
        try:
            html = await BrowserFetcher.fetch(url)
        except Exception as e:
            logger.error(f"Browser fetch failed for {url}: {e}")

    return html


def parse_date(date_val):
    if not date_val or str(date_val).lower() == "null":
        return None
    try:
        clean_val = str(date_val).replace("Z", "+00:00").replace(" ", "T").strip()
        if len(clean_val) == 10:
            clean_val += "T00:00:00"
        elif len(clean_val) == 16:
            clean_val += ":00"

        dt = datetime.fromisoformat(clean_val)

        # Normalize EVERYTHING to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Convert -03:00 to UTC (+00:00) so DB stores consistent values
            dt = dt.astimezone(timezone.utc)

        return dt
    except Exception as e:
        logger.debug(f"Date parse error: {e}")
        return None


async def process_article(sem, http_session, analyzer, article, regex_misses):
    """Fetches HTML, extracts date via LLM, and updates article object"""
    async with sem:
        if not article.original_url:
            return False

        html = await fetch_html(http_session, article.original_url)
        if not html:
            return False

        # Try Regex First (using shared domain logic)
        soup = BeautifulSoup(html, "html.parser")
        new_date = ContentEnricherDomain.extract_published_date(soup)

        # Fallback to LLM
        if not new_date:
            logger.warning(f"⚠️ Regex Miss for: {article.original_url}")
            regex_misses.append(article.original_url)
            date_str = await analyzer.extract_date_from_html(html)
            new_date = parse_date(date_str)

        if not new_date:
            return False

        article.published_date = new_date
        return True


async def main():
    settings = Settings()
    engine = create_engine(str(settings.pg_dsn))
    SessionLocal = sessionmaker(bind=engine)

    analyzer = CloudNewsAnalyzer()
    publisher = NewsPublisherDomain()

    # Concurrency limit for fetching/LLM to avoid rate limits
    sem = asyncio.Semaphore(5)

    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    with SessionLocal() as session:
        # 0. Select Newspaper
        newspapers = session.scalars(
            select(NewspaperModel).order_by(NewspaperModel.name)
        ).all()
        print("\n📰 Available Newspapers:")
        print("0. All")
        for i, np in enumerate(newspapers):
            print(f"{i + 1}. {np.name}")

        try:
            choice = int(input(f"\nSelect Newspaper (0-{len(newspapers)}): "))
        except ValueError:
            choice = 0

        selected_np_id = None
        if choice > 0 and choice <= len(newspapers):
            selected_np_id = newspapers[choice - 1].id
            logger.info(f"🎯 Target: {newspapers[choice - 1].name}")
        else:
            logger.info("🎯 Target: All Newspapers")

        # 1. Get Active Events
        logger.info("Fetching active events...")
        events = (
            session.scalars(
                select(NewsEventModel)
                .where(NewsEventModel.is_active.is_(True))
                .options(joinedload(NewsEventModel.articles))
            )
            .unique()
            .all()
        )

        logger.info(f"Found {len(events)} active events.")

        # 2. Collect Unique Articles
        articles_map = {}
        for e in events:
            for a in e.articles:
                if selected_np_id and a.newspaper_id != selected_np_id:
                    continue
                articles_map[a.id] = a

        articles_to_process = list(articles_map.values())
        logger.info(f"Found {len(articles_to_process)} unique articles to check.")

        # 3. Process Articles
        logger.info("Starting date refetch...")
        regex_misses = []
        async with aiohttp.ClientSession() as http_session:
            tasks = [
                process_article(sem, http_session, analyzer, a, regex_misses)
                for a in articles_to_process
            ]

            # Process in chunks to show progress
            chunk_size = 20
            updated_count = 0
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i : i + chunk_size]
                results = await asyncio.gather(*chunk)
                updated_count += sum(1 for r in results if r)
                logger.info(
                    f"Processed {min(i + chunk_size, len(tasks))}/{len(tasks)} articles... (Updated: {updated_count})"
                )

                # Save misses incrementally
                if regex_misses:
                    with open(MISSES_FILE, "w") as f:
                        json.dump(regex_misses, f, indent=2)
                session.commit()  # Commit intermediate results

        logger.success(f"Finished article updates. Total updated: {updated_count}")

        # 4. Recalculate Events
        logger.info("Recalculating Event Scores & Dates...")
        for event in events:
            # This helper recalculates dates, score, and blind spot logic
            publisher.publish_event_direct(session, event, commit=False)

        session.commit()
        logger.success("✅ All events recalculated.")


if __name__ == "__main__":
    asyncio.run(main())
