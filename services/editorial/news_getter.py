import asyncio
import hashlib
import json
import random
import re
from datetime import datetime, timedelta
from functools import partial
from typing import List, Optional, Set, Dict
from uuid import UUID

import aiohttp
import trafilatura
from loguru import logger
from sqlalchemy import select, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

# Local project imports
from config import Settings
from harvesters.factory import HarvesterFactory
from nlp_service import NLPService
from models import ArticlesQueueModel, ArticlesQueueName
from news_events_lib.models import (
    ArticleContentModel,
    ArticleModel,
    FeedModel,
    NewspaperModel,
    JobStatus
)

class NewsGetter:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    ]

    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.harvester_factory = HarvesterFactory()
        self.nlp_service = NLPService()
        
        # Standard Headers
        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }

        
        logger.info("NewsGetter Initialized.")

    @property
    def headers(self):
        h = self.base_headers.copy()
        h["User-Agent"] = random.choice(self.USER_AGENTS)
        return h

    # --- FETCHING PIPELINE ---

    async def fetch_links(self, session: aiohttp.ClientSession, sources: List[dict], newspaper_name: str) -> list[dict]:
        harvester = self.harvester_factory.get_harvester(newspaper_name)
        articles = await harvester.harvest(session, sources)
        logger.info(f"[{newspaper_name}] Found {len(articles)} candidate links.")
        return articles

    async def fetch_article_content(self, http_session: aiohttp.ClientSession, url: str, retries: int = 2):
        for attempt in range(retries):
            try:
                async with http_session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception:
                await asyncio.sleep(1)
        return None

    async def process_newspaper_articles(
        self,
        http_session: aiohttp.ClientSession,
        sources: List[dict],
        newspaper_name: str,
        newspaper_id: UUID,
        ignore_hashes: Optional[set] = None,
        concurrency: int = 5
    ) -> List[dict]:
        
        if ignore_hashes is None: ignore_hashes = set()
        raw_entries = await self.fetch_links(http_session, sources, newspaper_name)
        
        if not raw_entries: return []

        semaphore = asyncio.Semaphore(concurrency)
        loop = asyncio.get_running_loop()
        
        async def _process(entry):
            try:
                clean_link = entry["link"]
                # Basic hash check
                link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()
                if link_hash in ignore_hashes:
                    return None

                async with semaphore:
                    html = await self.fetch_article_content(http_session, clean_link)

                if not html: return None

                # CPU-bound Extraction
                extracted_json = await loop.run_in_executor(
                    None,
                    partial(trafilatura.extract, html, with_metadata=True, output_format="json", include_comments=False)
                )
                
                if not extracted_json: return None
                
                data = json.loads(extracted_json)
                raw_text = data.get("raw_text", "")
                
                if len(raw_text) < 100: return None # Skip empty articles

                # --- CRITICAL: Generate Metadata from CLEAN Text ---
                clean_text = self.nlp_service.clean_text_for_embedding(raw_text)
                
                # 1. Calc Embedding (Uses clean_text internally)
                embedding = await loop.run_in_executor(None, self.nlp_service.calculate_vector, clean_text)
                
                # 2. Extract Entities & Interests
                # Trafilatura tags -> entities (flat)
                tags = [t.strip() for t in data.get("tags", "").split(",") if t.strip()]
                
                # SpaCy -> interests (categorized)
                extraction_text = f"{data.get('title', '')} {data.get('excerpt', '')} {clean_text[:2000]}"
                interests = await loop.run_in_executor(None, self.nlp_service.extract_interests, extraction_text)
                
                # Flatten interests for 'entities' column (legacy/search support)
                flat_interests = [item for sublist in interests.values() for item in sublist]
                all_entities = list(set(tags + flat_interests))[:15]

                return {
                    "title": data.get("title") or entry.get("title"),
                    "link": clean_link,
                    "hash": link_hash,
                    "published": entry.get("published") or data.get("date"),
                    "summary": data.get("excerpt"),
                    "content": raw_text, # Save FULL text
                    "newspaper_id": newspaper_id,
                    "entities": all_entities,
                    "interests": interests,
                    "embedding": embedding
                }
            except Exception as e:
                logger.error(f"Error processing {entry['link']}: {e}")
                return None

        tasks = [_process(e) for e in raw_entries]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    # --- DATABASE SAVING ---

    def _parse_published_date(self, pub_data: Optional[str]) -> datetime:
        if not pub_data: return datetime.now()
        try:
            return datetime.fromisoformat(str(pub_data))
        except:
            return datetime.now()

    def _save_article(self, session, data: dict) -> Optional[ArticleModel]:
        try:
            with session.begin_nested():
                dt = self._parse_published_date(data.get("published"))
                
                article = ArticleModel(
                    title=data["title"],
                    original_url=data["link"],
                    url_hash=data["hash"],
                    summary=data["summary"],
                    summary_date=dt,
                    published_date=dt,
                    newspaper_id=data["newspaper_id"],
                    authors=[],
                    contents=[ArticleContentModel(content=data["content"])],
                    entities=data["entities"],
                    interests=data.get("interests", {}),
                    embedding=data["embedding"],
                    subtitle=None, stance=None, main_topics=None, key_points=None
                )
                session.add(article)
                session.flush()
                return article
        except IntegrityError:
            return None # Duplicate
        except Exception as e:
            logger.error(f"DB Error saving {data['link']}: {e}")
            return None

    def _queue_articles(self, session, articles: List[ArticleModel]):
        if not articles: return
        stmt = insert(ArticlesQueueModel).values([
            {
                "article_id": a.id,
                "estimated_tokens": len(a.contents[0].content)//4,
                "status": JobStatus.PENDING,
                "queue_name": ArticlesQueueName.FILTER
            } for a in articles
        ]).on_conflict_do_nothing()
        session.execute(stmt)

    # --- MAIN EXECUTION ---

    async def get_news(self):
        newspapers = await self._get_newspapers()
        logger.info(f"Processing {len(newspapers)} newspapers.")
        
        async with aiohttp.ClientSession(headers=self.headers) as http_session:
            with self.SessionLocal() as session:
                for np in newspapers:
                    await self._process_single_newspaper(session, http_session, np)

    async def _process_single_newspaper(self, session, http_session, np):
        sources = [{"url": f.url} for f in np["feeds"]]
        articles_data = await self.process_newspaper_articles(
            http_session, sources, np["name"], np["id"], np["article_hashes"]
        )
        
        saved_count = 0
        saved_objs = []
        for d in articles_data:
            obj = self._save_article(session, d)
            if obj: 
                saved_count += 1
                saved_objs.append(obj)
        
        if saved_objs:
            self._queue_articles(session, saved_objs)
        
        session.commit()
        if saved_count > 0:
            logger.success(f"[{np['name']}] Saved {saved_count} new articles.")

    async def _get_newspapers(self) -> List[dict]:
        # Helper to fetch config from DB
        with self.SessionLocal() as session:
            cutoff = datetime.now() - timedelta(days=60)
            
            # 1. Get Newspapers with Active Feeds
            q = select(NewspaperModel).join(FeedModel).where(FeedModel.is_active == True).distinct()
            newspapers = session.execute(q).scalars().all()
            
            if not newspapers: return []
            
            # 2. Pre-fetch Feeds & Hashes (Bulk Optimization)
            ids = [n.id for n in newspapers]
            
            feeds = session.execute(
                select(FeedModel).where(FeedModel.newspaper_id.in_(ids), FeedModel.is_active == True)
            ).scalars().all()
            
            hashes = session.execute(
                select(ArticleModel.newspaper_id, ArticleModel.url_hash)
                .where(ArticleModel.newspaper_id.in_(ids), ArticleModel.published_date >= cutoff)
            ).all()

            # 3. Map Results
            feed_map = {i: [] for i in ids}
            for f in feeds: feed_map[f.newspaper_id].append(f)
            
            hash_map = {i: set() for i in ids}
            for nid, h in hashes: hash_map[nid].add(h)

            return [{
                "id": n.id, 
                "name": n.name, 
                "feeds": feed_map[n.id], 
                "article_hashes": hash_map[n.id]
            } for n in newspapers]

if __name__ == "__main__":
    getter = NewsGetter()
    asyncio.run(getter.get_news())