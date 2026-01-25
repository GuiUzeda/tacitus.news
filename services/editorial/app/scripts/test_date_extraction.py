import asyncio
import json
import os
import random
import re
import sys

import aiohttp
from bs4 import BeautifulSoup
from dateutil import parser
from htmldate import find_date
from loguru import logger

# Add service root to path to allow imports from app.core
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from app.config import Settings  # noqa: E402
from app.core.browser import BrowserFetcher  # noqa: E402
from app.core.llm_parser import CloudNewsAnalyzer  # noqa: E402
from news_events_lib.models import ArticleModel, NewspaperModel  # noqa: E402
from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


async def fetch_html(url: str):
    """Fetches HTML using the same headers as the main service."""
    headers = {
        "User-Agent": BrowserFetcher.USER_AGENTS[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    # try with playwright
                    try:
                        html = await BrowserFetcher.fetch(url)
                        return html
                    except Exception as browser_e:
                        logger.error(
                            f"Browser fetch fallback failed for {url}: {browser_e}"
                        )
                        return None

        except Exception as e:

            logger.error(f"Fetch failed: {e}")
            return None


def extract_date_regex(html: str):
    """Same logic as in enriching.py for testing"""
    if not html:
        return None


def extract_json_ld(soup):
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            if not script.string:
                continue
            data = json.loads(script.string)

            objects_to_check = []
            if isinstance(data, dict):
                if "@graph" in data:
                    graph_data = data["@graph"]
                    if isinstance(graph_data, list):
                        objects_to_check = graph_data
                    else:
                        objects_to_check = [graph_data]
                else:
                    objects_to_check = [data]
            elif isinstance(data, list):
                objects_to_check = data

            for item in objects_to_check:
                if not isinstance(item, dict):
                    continue
                # Check Type safely (could be a list or string)
                otype = item.get("@type")
                if isinstance(otype, list):
                    if any(
                        t in ["NewsArticle", "Article", "BlogPosting", "Report"]
                        for t in otype
                    ):
                        return item
                elif otype in ["NewsArticle", "Article", "BlogPosting", "Report"]:
                    return item

        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    return None


async def test_date_extraction(html: str, url: str):
    print(f"\n🔍 Testing Date Extraction for: {url}")
    print("=" * 60)

    bs4_html = BeautifulSoup(html, "html.parser")
    date = None

    # 1. Strong Signal Check
    has_strong_signal = [
        bs4_html.find("time"),
        bs4_html.find(
            "meta", attrs={"property": re.compile(r"date|time|published", re.I)}
        ),
        bs4_html.find("meta", attrs={"name": re.compile(r"date|time|published", re.I)}),
    ]
    ld_json = extract_json_ld(bs4_html)
    if ld_json:
        date_published = ld_json.get("datePublished") or ld_json.get("dateCreated")
        date_updated = ld_json.get("dateModified") or ld_json.get("dateUpdated")
        if date_published:
            date_published = parser.parse(date_published)
        if date_updated:
            date_updated = parser.parse(date_updated)

        if date_published and date_updated:
            date = date_published if date_published <= date_updated else date_updated
        elif date_published:
            date = date_published
        elif date_updated:
            date = date_updated

        print(f"✅ Found on ld_json: {date} : {ld_json}")

    if any(has_strong_signal) and not date:
        logger.info(
            f"Found strong date signal. Trusting htmldate...  {has_strong_signal}"
        )
        try:
            date = find_date(
                html,
                original_date=True,
                outputformat="%Y-%m-%dT%H:%M:%S",
                extensive_search=True,
            )

        except Exception as e:
            logger.error(f"htmldate error: {e}")

        if date:
            print(f"✅ Found on htmldate: {date}")
            date = parser.parse(date)
    # 2. Time Augmentation
    if date and date.hour == 0 and date.minute == 0:
        text_content = bs4_html.get_text(separator="\n", strip=True)

        # Dynamic Regex based on the found date parts
        # Matches: "2025.01.17" OR "17.01.2025" followed by "14:30" or "14h30"
        # We use re.escape to be safe with separators
        d_year, d_month, d_day = date.year, date.month, date.day

        # Pattern: (YYYY.MM.DD OR DD.MM.YYYY) + junk + (HH:MM or HHhMM)
        date_part = f"(?:{d_year}.{d_month}.{d_day}|{d_day}.{d_month}.{d_year})"
        time_part = r"(\d{1,2}(:|h)\d{2})"

        # Search pattern
        full_pattern = f"{date_part}.*?{time_part}"

        # Limit search to first 5000 chars to save CPU
        time_match = re.search(
            full_pattern, text_content[:5000], re.DOTALL | re.IGNORECASE
        )

        if time_match:

            raw_time = time_match.group(1).replace("h", ":")
            if len(raw_time) == 4:
                raw_time = "0" + raw_time
            date = date.replace(hour=int(raw_time[:2]), minute=int(raw_time[3:5]))

            print(f"   + Time augmented: {date}")
    # 3. Fallback: Manual Regex
    if not date:
        print("⚠️ htmldate failed/skipped. Attempting BS4 + Regex...")
        r_date_pattern = r"(?:publicado|criado|postado|data).*?(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{4}(?:\s+\d{1,2}[:h]\d{2})?)"
        date_class = re.compile(
            r"(date|time|published|clock|data|meta|author|info)", re.IGNORECASE
        )

        candidates = bs4_html.find_all(
            ["div", "span", "p", "time", "li"], class_=date_class
        )
        for obj in candidates:
            clean_text = " ".join(obj.get_text().split())
            match = re.search(r_date_pattern, clean_text, re.IGNORECASE)
            if match:
                raw_date = match.group(1).replace("h", ":")
                print(f"✅ Found via BS4 Regex: {raw_date}")
                date = raw_date
                date = parser.parse(date)
                break

    # 4. Fallback: LLM
    if not date:
        print("⚠️ All heuristics failed. Asking LLM...")
        try:
            # Send only the head + top body to save tokens
            date = await CloudNewsAnalyzer().extract_date_from_html(html[:15000])
            if date:
                date = parser.parse(date)
                print(f"✅ Found via LLM: {date}")
        except Exception:
            print("❌ LLM failed.")

    return date


async def main():
    url = None
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        print("1. Enter URL manually")
        print("2. Pick random article from Newspaper")
        choice = input("Choice (1/2): ").strip()

        if choice == "2":
            settings = Settings()
            engine = create_engine(str(settings.pg_dsn))
            SessionLocal = sessionmaker(bind=engine)

            with SessionLocal() as session:
                newspapers = session.scalars(
                    select(NewspaperModel).order_by(NewspaperModel.name)
                ).all()
                print("\n📰 Available Newspapers:")
                for i, np in enumerate(newspapers):
                    print(f"{i + 1}. {np.name}")

                try:
                    idx = int(input("\nSelect Newspaper: ")) - 1
                    if 0 <= idx < len(newspapers):
                        np = newspapers[idx]
                        articles = session.scalars(
                            select(ArticleModel)
                            .where(ArticleModel.newspaper_id == np.id)
                            .order_by(ArticleModel.published_date.desc())
                            .limit(50)
                        ).all()
                        if articles:
                            target = random.choice(articles)
                            url = target.original_url
                            print(f"\n🔗 Testing: {target.title}\n🔗 URL: {url}")
                except Exception as e:
                    print(f"Error: {e}")

        if not url:
            url = input("Enter URL to test: ").strip()

    if not url:
        print("❌ URL is required.")
        return

    print("⏳ Fetching HTML...")
    html = await fetch_html(url)

    if html:
        await test_date_extraction(html, url)
    else:
        print("❌ Failed to retrieve content.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
