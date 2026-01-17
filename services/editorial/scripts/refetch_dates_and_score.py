import sys
import os
import asyncio
import aiohttp
import re
import json
from datetime import datetime, timezone
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload
from loguru import logger
from bs4 import BeautifulSoup
from htmldate import find_date
from dateutil import parser

# Add service root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Settings
from news_events_lib.models import NewsEventModel, ArticleModel, NewspaperModel
from core.llm_parser import CloudNewsAnalyzer
from domain.publisher import NewsPublisherDomain
from core.browser import BrowserFetcher

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
    if not date_val or str(date_val).lower() == 'null': return None
    try:
        clean_val = str(date_val).replace("Z", "+00:00").replace(" ", "T").strip()
        if len(clean_val) == 10: clean_val += "T00:00:00"
        elif len(clean_val) == 16: clean_val += ":00"
             
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

def extract_json_ld(soup):
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            if not script.string: continue
            data = json.loads(script.string)
            
            objects_to_check = []
            if isinstance(data, dict):
                if '@graph' in data:
                    graph_data = data['@graph']
                    if isinstance(graph_data, list):
                        objects_to_check = graph_data
                    else:
                        objects_to_check = [graph_data]
                else:
                    objects_to_check = [data]
            elif isinstance(data, list):
                objects_to_check = data
            
            for item in objects_to_check:
                if not isinstance(item, dict): continue
                # Check Type safely (could be a list or string)
                otype = item.get('@type')
                if isinstance(otype, list):
                    if any(t in ['NewsArticle', 'Article', 'BlogPosting', 'Report'] for t in otype):
                        return item
                elif otype in ['NewsArticle', 'Article', 'BlogPosting', 'Report']:
                    return item
                    
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
            
    return None

def extract_date_regex(html: str):
    """
    Robust date extraction strategy:
    1. Strong Signal Check (Meta/JSON-LD) -> htmldate
    2. Time Augmentation (if date found but no time)
    3. Fallback: Manual Regex on specific tags
    """
    if not html: return None
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        date = None
        
        # 0. JSON-LD Check (New Strategy)
        ld_json = extract_json_ld(soup)
        if ld_json:
            try:
                date_published = ld_json.get("datePublished") or ld_json.get("dateCreated")
                date_updated = ld_json.get("dateModified") or ld_json.get("dateUpdated")
                
                dt_pub = None
                dt_mod = None

                if date_published:
                    dt_pub = parser.parse(date_published)
                
                if date_updated:
                    dt_mod = parser.parse(date_updated)
                
                final_dt = None
                if dt_pub and dt_mod:
                    final_dt = dt_pub if dt_pub <= dt_mod else dt_mod
                elif dt_pub:
                    final_dt = dt_pub
                elif dt_mod:
                    final_dt = dt_mod
                
                if final_dt:
                    date = final_dt.isoformat()
            except Exception:
                pass

        # 1. Strong Signal Check (if no JSON-LD date found)
        if not date:
            has_strong_signal = (
                soup.find('time') or 
                soup.find('meta', attrs={'property': re.compile(r'date|time|published', re.I)}) or
                soup.find('meta', attrs={'name': re.compile(r'date|time|published', re.I)})
            )

            if has_strong_signal:
                try:
                    date = find_date(html, original_date=True, outputformat='%Y-%m-%dT%H:%M:%S', extensive_search=False)
                except Exception:
                    pass

        # 2. Time Augmentation
        if date and ("T00:00:00" in date or len(date) == 10):
            if len(date) == 10: date += "T00:00:00"
            if "T00:00:00" in date:
                try:
                    text_content = soup.get_text(separator="\n", strip=True)
                    d_year, d_month, d_day = date[:4], date[5:7], date[8:10]
                    
                    # Allow single digits (e.g. 01 matches 01 or 1)
                    r_day = f"(?:{d_day}|{d_day[1]})" if d_day.startswith("0") else d_day
                    r_month = f"(?:{d_month}|{d_month[1]})" if d_month.startswith("0") else d_month
                    
                    # Pattern: (YYYY.MM.DD OR DD.MM.YYYY) + junk + (HH:MM or HHhMM)
                    date_part = f"(?:{d_year}.{r_month}.{r_day}|{r_day}.{r_month}.{d_year})"
                    time_part = r"(\d{1,2}(:|h)\d{2})"
                    full_pattern = f"{date_part}.*?{time_part}"
                    
                    time_match = re.search(full_pattern, text_content[:5000], re.DOTALL | re.IGNORECASE)
                    if time_match:
                        raw_time = time_match.group(1).replace("h", ":")
                        if len(raw_time) == 4: raw_time = "0" + raw_time
                        date = f"{date[:10]}T{raw_time}:00"
                except Exception:
                    pass

        if date: return date

        # 3. Fallback: Manual Regex
        r_date_pattern = r"(?:publicado|criado|postado|data).*?(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{4}(?:\s+\d{1,2}[:h]\d{2})?)"
        date_class = re.compile(r"(date|time|published|clock|data|meta|author|info)", re.IGNORECASE)
        
        candidates = soup.find_all(['div', 'span', 'p', 'time', 'li'], class_=date_class)
        for obj in candidates:
            clean_text = " ".join(obj.get_text().split())
            match = re.search(r_date_pattern, clean_text, re.IGNORECASE)
            if match:
                raw_date = match.group(1).replace("h", ":")
                d_match = re.search(r"(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})(?:\s+(\d{1,2})[:h](\d{2}))?", raw_date)
                if d_match:
                    day, month, year, hour, minute = d_match.groups()
                    
                    # Explicit check for None is safer
                    if hour is None or minute is None:
                        hour, minute = "00", "00"
                        
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}T{hour.zfill(2)}:{minute.zfill(2)}:00"

    except Exception as e:
        logger.error(f"Robust date extraction error: {e}")
    
    return None

async def process_article(sem, http_session, analyzer, article, regex_misses):
    """Fetches HTML, extracts date via LLM, and updates article object"""
    async with sem:
        if not article.original_url:
            return False
            
        html = await fetch_html(http_session, article.original_url)
        if not html:
            return False
            
        # Try Regex First
        date_str = extract_date_regex(html)
        
        # Fallback to LLM
        if not date_str:
            logger.warning(f"⚠️ Regex Miss for: {article.original_url}")
            regex_misses.append(article.original_url)
            date_str = await analyzer.extract_date_from_html(html)
            
        if not date_str:
            return False
            
        new_date = parse_date(date_str)
        if new_date:
            article.published_date = new_date
            return True
        return False

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
        newspapers = session.scalars(select(NewspaperModel).order_by(NewspaperModel.name)).all()
        print("\n📰 Available Newspapers:")
        print("0. All")
        for i, np in enumerate(newspapers):
            print(f"{i+1}. {np.name}")
        
        try:
            choice = int(input(f"\nSelect Newspaper (0-{len(newspapers)}): "))
        except ValueError:
            choice = 0
            
        selected_np_id = None
        if choice > 0 and choice <= len(newspapers):
            selected_np_id = newspapers[choice-1].id
            logger.info(f"🎯 Target: {newspapers[choice-1].name}")
        else:
            logger.info("🎯 Target: All Newspapers")

        # 1. Get Active Events
        logger.info("Fetching active events...")
        events = session.scalars(
            select(NewsEventModel)
            .where(NewsEventModel.is_active == True)
            .options(joinedload(NewsEventModel.articles))
        ).unique().all()
        
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
            tasks = [process_article(sem, http_session, analyzer, a, regex_misses) for a in articles_to_process]
            
            # Process in chunks to show progress
            chunk_size = 20
            updated_count = 0
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i:i+chunk_size]
                results = await asyncio.gather(*chunk)
                updated_count += sum(1 for r in results if r)
                logger.info(f"Processed {min(i+chunk_size, len(tasks))}/{len(tasks)} articles... (Updated: {updated_count})")
                
                # Save misses incrementally
                if regex_misses:
                    with open(MISSES_FILE, "w") as f:
                        json.dump(regex_misses, f, indent=2)
                session.commit() # Commit intermediate results

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