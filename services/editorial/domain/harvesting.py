import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from functools import partial
from typing import List, Optional, Dict, Any, Set
from uuid import UUID
from concurrent.futures import ProcessPoolExecutor

import aiohttp
import trafilatura
from loguru import logger
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

# Project Imports
from core.nlp_service import NLPService
from harvesters.base import BaseHarvester
from news_events_lib.models import (
    ArticleContentModel,
    ArticleModel,
    JobStatus
)
from core.models import ArticlesQueueModel, ArticlesQueueName

# --- MULTIPROCESSING HELPERS ---

_worker_nlp_service = None

def init_worker():
    """
    Called once when the worker process starts. 
    Loads the AI models into the process's global memory.
    """
    global _worker_nlp_service
    try:
        _worker_nlp_service = NLPService()
    except Exception as e:
        logger.error(f"Worker init failed: {e}")

def _process_entry_cpu_task(html: str, entry: dict, newspaper_id: UUID) -> Optional[dict]:
    """
    CPU-intensive task: Extract Content -> Clean -> Embed -> Extract Interests.
    """
    global _worker_nlp_service
    try:
        if _worker_nlp_service is None:
            _worker_nlp_service = NLPService()

        # 1. Extract Body Text
        extracted_json = trafilatura.extract(
            html, with_metadata=True, output_format="json", include_comments=False
        )
        
        if not extracted_json: return None
        data = json.loads(extracted_json)
        raw_text = data.get("raw_text", "")
        
        # Filter very short content
        if len(raw_text) < 100: return None 

        # 2. NLP Processing
        clean_text = _worker_nlp_service.clean_text_for_embedding(raw_text)
        embedding = _worker_nlp_service.calculate_vector(clean_text)
        
        extraction_text = f"{data.get('title', '')} {data.get('excerpt', '')} {clean_text[:2000]}"
        interests = _worker_nlp_service.extract_interests(extraction_text)
        
        # 3. Entity & Tag extraction
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
            "embedding": embedding,
            # Pass the Rank through
            "rank": entry.get("rank") 
        }
    except Exception as e:
        print(f"CPU Task Error: {e}")
        return None

# --- DOMAIN CLASS ---

class HarvestingDomain:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    def __init__(self, max_cpu_workers=3):
        # UPDATED: Use BaseHarvester directly
        self.harvester = BaseHarvester()
        
        # Initialize Executor for CPU tasks
        self.cpu_executor = ProcessPoolExecutor(
            max_workers=max_cpu_workers, 
            initializer=init_worker
        )
        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def shutdown(self):
        self.cpu_executor.shutdown(wait=True)

    async def process_newspaper(
        self, 
        session: Session, 
        http_session: aiohttp.ClientSession, 
        newspaper_data: dict
    ):
        """
        Orchestrates the pipeline for a SINGLE newspaper.
        """
        try:
            name = newspaper_data["name"]
            # UPDATED: Pass the full feed objects (with patterns/flags)
            feeds = newspaper_data["feeds"]
            
            # 1. Pipeline Execution
            articles_data = await self._run_pipeline(
                http_session, 
                feeds, 
                name, 
                newspaper_data["id"], 
                newspaper_data["article_hashes"]
            )
            
            if not articles_data: return 0

            # 2. Save
            saved_objs = self._bulk_save_articles(session, articles_data)
            
            # 3. Queue
            if saved_objs:
                self._queue_articles_bulk(session, saved_objs)
            
            return len(saved_objs)

        except Exception as e:
            logger.error(f"[{newspaper_data['name']}] Pipeline Failed: {e}")
            raise e

    # --- INTERNAL PIPELINE STEPS ---

    async def _run_pipeline(self, http_session, feeds, name, np_id, ignore_hashes):
        # A. Fetch Links (Uses the new BaseHarvester with Browser/Regex support)
        try:
            raw_entries = await self.harvester.harvest(http_session, feeds)
        except Exception as e:
            logger.error(f"[{name}] Link fetch failed: {e}")
            return []

        if not raw_entries: return []

        # B. Fetch Content & Process (Concurrency Limit for IO)
        loop = asyncio.get_running_loop()
        io_semaphore = asyncio.Semaphore(15)
        
        async def _process_entry(entry):
            clean_link = entry["link"]
            link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()
            
            if link_hash in ignore_hashes: return None

            html = None
            async with io_semaphore:
                headers = self.base_headers.copy()
                headers["User-Agent"] = self.USER_AGENTS[0] 
                try:
                    timeout = aiohttp.ClientTimeout(total=45)
                    async with http_session.get(clean_link, headers=headers, timeout=timeout) as resp:
                        if resp.status == 200: html = await resp.text()
                except: pass

            if not html: return None

            # Sanitize for pickle
            safe_entry = self._sanitize_entry(entry)

            # Offload to CPU
            return await loop.run_in_executor(
                self.cpu_executor,
                partial(_process_entry_cpu_task, html, safe_entry, np_id)
            )

        tasks = [_process_entry(e) for e in raw_entries]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    def _sanitize_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Ensures entry data is pickle-safe for multiprocessing"""
        clean = entry.copy()
        pub = clean.get("published")
        if pub and hasattr(pub, "group"): # Fix re.Match objects from Regex
            clean["published"] = pub.group(0)
        return clean

    def _bulk_save_articles(self, session, articles_data: List[dict]) -> List[ArticleModel]:
        valid_models = []
        for data in articles_data:
            dt = self._parse_date(data.get("published"))
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
                summary_status=JobStatus.PENDING,
                
                # UPDATED: Save the Editorial Rank
                source_rank=data.get("rank") 
            )
            valid_models.append(article)

        if not valid_models: return []

        try:
            session.add_all(valid_models)
            session.flush()
            return valid_models
        except IntegrityError:
            session.rollback()
            return [] 
        except Exception as e:
            logger.error(f"Save Error: {e}")
            session.rollback()
            return []

    def _queue_articles_bulk(self, session, articles: List[ArticleModel]):
        if not articles: return
        queue_data = [{
            "article_id": a.id,
            "status": JobStatus.PENDING,
            "queue_name": ArticlesQueueName.FILTER
        } for a in articles if a.id]
        
        if queue_data:
            stmt = insert(ArticlesQueueModel).values(queue_data).on_conflict_do_nothing()
            session.execute(stmt)

    def _parse_date(self, pub_data):
        if not pub_data: return datetime.now(timezone.utc)
        try:
            if isinstance(pub_data, str):
                return datetime.fromisoformat(pub_data.replace('Z', '+00:00'))
            return pub_data
        except: return datetime.now(timezone.utc)