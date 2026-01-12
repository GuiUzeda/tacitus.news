import asyncio
import time
import hashlib
import json
import random
import os
import re
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import List, Optional, Set, Dict, Any
from uuid import UUID
from concurrent.futures import ProcessPoolExecutor

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

# --- CONFIGURATION ---
# LIMIT THIS to avoid OOM. Each worker takes ~1GB RAM (Spacy + BERT).
# 3 workers = ~3GB RAM usage. Increase only if you have 32GB+ RAM.
MAX_CPU_WORKERS = 3 

# --- WORKER GLOBAL STATE ---
_worker_nlp_service = None

def init_worker():
    """
    Called once when the worker process starts. 
    Loads the AI models into the process's global memory.
    """
    global _worker_nlp_service
    try:
        # Initialize the Singleton (loads Spacy/Transformers)
        _worker_nlp_service = NLPService()
        logger.info(f"Worker process {os.getpid()} initialized with NLP models.")
    except Exception as e:
        logger.error(f"Worker init failed: {e}")

def _process_entry_cpu_task(html: str, entry: dict, newspaper_id: UUID) -> Optional[dict]:
    """
    CPU-intensive task running in a separate process.
    Uses the pre-loaded global _worker_nlp_service.
    """
    global _worker_nlp_service
    
    try:
        # Fallback if init failed or wasn't called (safety check)
        if _worker_nlp_service is None:
            _worker_nlp_service = NLPService()

        # 1. HTML Extraction (CPU Heavy)
        extracted_json = trafilatura.extract(
            html, 
            with_metadata=True, 
            output_format="json", 
            include_comments=False
        )
        
        if not extracted_json: 
            return None
        
        data = json.loads(extracted_json)
        raw_text = data.get("raw_text", "")
        
        if len(raw_text) < 100: 
            return None 

        # 2. NLP Processing (Memory Heavy)
        clean_text = _worker_nlp_service.clean_text_for_embedding(raw_text)
        embedding = _worker_nlp_service.calculate_vector(clean_text)
        
        extraction_text = f"{data.get('title', '')} {data.get('excerpt', '')} {clean_text[:2000]}"
        interests = _worker_nlp_service.extract_interests(extraction_text)
        
        tags = [t.strip() for t in data.get("tags", "").split(",") if t.strip()]
        
        flat_interests = [item for sublist in interests.values() for item in sublist]
        all_entities = list(set(tags + flat_interests))[:15]

        clean_link = entry["link"]
        link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()

        return {
            "title": data.get("title") or entry.get("title"),
            "link": clean_link,
            "hash": link_hash,
            "published": entry.get("published") or data.get("date"),
            "summary": data.get("excerpt"),
            "content": raw_text,
            "newspaper_id": newspaper_id,
            "entities": all_entities,
            "interests": interests,
            "embedding": embedding
        }
    except Exception as e:
        # Print is safer than logger in multiprocess errors if logger isn't configured for it
        print(f"CPU Task Error processing {entry.get('link', 'unknown')}: {e}")
        return None


class NewsGetter:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    ]

    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(
            str(self.settings.pg_dsn),
            pool_size=20,
            max_overflow=10,
            pool_timeout=30
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.harvester_factory = HarvesterFactory()
        
        # Initialize Executor with SAFE worker count and Initializer
        # This prevents OOM by limiting concurrent model instances
        self.cpu_executor = ProcessPoolExecutor(
            max_workers=MAX_CPU_WORKERS, 
            initializer=init_worker
        )

        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }
        
        logger.info(f"NewsGetter Initialized. CPU Workers Limited to: {MAX_CPU_WORKERS}")

    @property
    def headers(self):
        h = self.base_headers.copy()
        h["User-Agent"] = random.choice(self.USER_AGENTS)
        return h

    # --- FETCHING PIPELINE ---

    async def fetch_links(self, session: aiohttp.ClientSession, sources: List[dict], newspaper_name: str) -> list[dict]:
        harvester = self.harvester_factory.get_harvester(newspaper_name)
        try:
            articles = await harvester.harvest(session, sources)
            logger.info(f"[{newspaper_name}] Found {len(articles)} candidate links.")
            return articles
        except Exception as e:
            logger.error(f"[{newspaper_name}] Harvester failed: {e}")
            return []

    async def fetch_article_content(self, http_session: aiohttp.ClientSession, url: str, retries: int = 2):
        for attempt in range(retries):
            try:
                timeout = aiohttp.ClientTimeout(total=45)
                async with http_session.get(url, headers=self.headers, timeout=timeout) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception:
                await asyncio.sleep(0.5 * (attempt + 1))
        return None

    def _sanitize_entry_for_pickle(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensures the entry dict contains only picklable types.
        Fixes the 're.Match' object error.
        """
        clean_entry = entry.copy()
        published_val = clean_entry.get("published")
        
        if published_val and not isinstance(published_val, (str, datetime, float, int)):
            if hasattr(published_val, "group"): # It is a re.Match object
                clean_entry["published"] = published_val.group(0)
            else:
                clean_entry["published"] = str(published_val)
        
        return clean_entry

    async def process_newspaper_articles(
        self,
        http_session: aiohttp.ClientSession,
        sources: List[dict],
        newspaper_name: str,
        newspaper_id: UUID,
        ignore_hashes: Optional[set] = None,
        concurrency: int = 15
    ) -> List[dict]:
        
        if ignore_hashes is None: ignore_hashes = set()
        
        raw_entries = await self.fetch_links(http_session, sources, newspaper_name)
        if not raw_entries: return []

        loop = asyncio.get_running_loop()
        io_semaphore = asyncio.Semaphore(concurrency)
        
        async def _pipeline(entry):
            clean_link = entry["link"]
            
            link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()
            if link_hash in ignore_hashes:
                return None

            html = None
            async with io_semaphore:
                html = await self.fetch_article_content(http_session, clean_link)

            if not html: return None

            # Fix Pickle Error
            safe_entry = self._sanitize_entry_for_pickle(entry)

            # Offload to ProcessPool
            try:
                result = await loop.run_in_executor(
                    self.cpu_executor,
                    partial(_process_entry_cpu_task, html, safe_entry, newspaper_id)
                )
                return result
            except Exception as e:
                logger.error(f"Pipeline error for {clean_link}: {e}")
                return None

        tasks = [_pipeline(e) for e in raw_entries]
        results = await asyncio.gather(*tasks)
        
        valid_results = [r for r in results if r is not None]
        if valid_results:
             logger.info(f"[{newspaper_name}] Successfully processed {len(valid_results)} articles.")
        return valid_results

    # --- DATABASE SAVING ---

    def _parse_published_date(self, pub_data: Optional[str]) -> datetime:
        if not pub_data: return datetime.now(timezone.utc)
        try:
            if isinstance(pub_data, str):
                return datetime.fromisoformat(pub_data.replace('Z', '+00:00'))
            return pub_data
        except:
            return datetime.now(timezone.utc)

    def _bulk_save_articles(self, session, articles_data: List[dict]) -> List[ArticleModel]:
        if not articles_data: return []

        valid_models = []
        for data in articles_data:
            dt = self._parse_published_date(data.get("published"))
            
            article = ArticleModel(
                title=data["title"][:500] if data["title"] else "No Title",
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
                subtitle=None, stance=None, main_topics=None, key_points=None,
                summary_status=JobStatus.PENDING
            )
            valid_models.append(article)

        if not valid_models: return []

        try:
            session.add_all(valid_models)
            session.flush()
            return valid_models
        except IntegrityError:
            session.rollback()
            # Retry individually if bulk fails
            saved = []
            for model in valid_models:
                try:
                    with session.begin_nested():
                        session.add(model)
                        session.flush()
                    saved.append(model)
                except IntegrityError:
                    pass
                except Exception:
                    pass
            return saved
        except Exception as e:
            session.rollback()
            logger.error(f"Critical Bulk Save Error: {e}")
            return []

    def _queue_articles_bulk(self, session, articles: List[ArticleModel]):
        if not articles: return
        
        queue_data = []
        for a in articles:
            if a.id:
                token_est = 0
                if a.contents and len(a.contents) > 0:
                     token_est = len(a.contents[0].content or "") // 4

                queue_data.append({
                    "article_id": a.id,
                    "estimated_tokens": token_est,
                    "status": JobStatus.PENDING,
                    "queue_name": ArticlesQueueName.FILTER
                })
        
        if not queue_data: return

        stmt = insert(ArticlesQueueModel).values(queue_data).on_conflict_do_nothing()
        try:
            session.execute(stmt)
        except Exception as e:
            logger.error(f"Queue insert failed: {e}")

    # --- MAIN EXECUTION ---

    async def get_news(self):
        newspapers = await self._get_newspapers()
        logger.info(f"Starting Extraction for {len(newspapers)} newspapers.")
        
        # Parallel Newspapers (IO Bound)
        newspaper_semaphore = asyncio.Semaphore(8) 

        try:
            async with aiohttp.ClientSession(headers=self.headers) as http_session:
                tasks = []
                for np in newspapers:
                    tasks.append(
                        self._process_single_newspaper_wrapper(
                            newspaper_semaphore, http_session, np
                        )
                    )
                
                await asyncio.gather(*tasks)
            logger.success("NewsGetter Cycle Completed.")
        finally:
            self.cpu_executor.shutdown(wait=True)

    async def _process_single_newspaper_wrapper(self, sem, http_session, np):
        async with sem:
            with self.SessionLocal() as session:
                await self._process_single_newspaper(session, http_session, np)

    async def _process_single_newspaper(self, session, http_session, np):
        try:
            sources = [{"url": f.url} for f in np["feeds"]]
            
            articles_data = await self.process_newspaper_articles(
                http_session, 
                sources, 
                np["name"], 
                np["id"], 
                np["article_hashes"]
            )
            
            if not articles_data:
                return

            saved_objs = self._bulk_save_articles(session, articles_data)
            
            if saved_objs:
                self._queue_articles_bulk(session, saved_objs)
            
            session.commit()
            
            if saved_objs:
                logger.success(f"[{np['name']}] ✅ Saved {len(saved_objs)} new articles.")
                
        except Exception as e:
            logger.error(f"[{np['name']}] ❌ Failed processing: {e}")
            session.rollback()

    async def _get_newspapers(self) -> List[dict]:
        with self.SessionLocal() as session:
            cutoff = datetime.now() - timedelta(days=60)
            
            q = select(NewspaperModel).join(FeedModel).where(FeedModel.is_active == True).distinct()
            newspapers = session.execute(q).scalars().all()
            
            if not newspapers: return []
            
            ids = [n.id for n in newspapers]
            
            feeds = session.execute(
                select(FeedModel).where(FeedModel.newspaper_id.in_(ids), FeedModel.is_active == True)
            ).scalars().all()
            
            hashes = session.execute(
                select(ArticleModel.newspaper_id, ArticleModel.url_hash)
                .where(ArticleModel.newspaper_id.in_(ids), ArticleModel.published_date >= cutoff)
            ).all()

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
    from multiprocessing import freeze_support
    freeze_support()

    while True:
        try:
            getter = NewsGetter()
            asyncio.run(getter.get_news())
        except Exception as e:
            logger.error(f"NewsGetter crashed: {e}")
        
        logger.info("Sleeping for 1 hour...")
        time.sleep(3600)