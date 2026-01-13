import asyncio
import json
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Optional
from datetime import datetime, timezone, timedelta

import aiohttp
import trafilatura
from loguru import logger
from sqlalchemy.orm import Session

# Project Imports
from core.nlp_service import NLPService
from news_events_lib.models import ArticleModel, ArticleContentModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings

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

def _process_content_cpu_task(html: str, title: str, existing_summary: str) -> Optional[dict]:
    """
    CPU-intensive task: Extract Content -> Clean -> Embed -> Extract Interests.
    """
    global _worker_nlp_service
    try:
        if _worker_nlp_service is None:
            _worker_nlp_service = NLPService()

        # 1. Extract Body Text (if HTML provided)
        raw_text = ""
        extracted_title = None
        extracted_description = None
        published_date= None

        if html:
            extracted_json = trafilatura.extract(
                html, with_metadata=True, output_format="json", include_comments=False
            )
            if extracted_json:
                data = json.loads(extracted_json)
                raw_text = data.get("raw_text", "")
                extracted_title = data.get("title")
                extracted_description = data.get("excerpt")
                published_date = data.get("date", data.get('filedate'))
                
        
        # If no HTML was provided, we assume we are processing existing content? 
        # For now, this function assumes it receives HTML or needs to fail.
        if not raw_text and html:
             return None

        # Filter very short content
        if len(raw_text) < 100: return None 

        # 2. NLP Processing
        clean_text = _worker_nlp_service.clean_text_for_embedding(raw_text)
        embedding = _worker_nlp_service.calculate_vector(clean_text)
        
        # Use extracted metadata if original is missing/placeholder
        effective_title = title
        if (not title or title in ["No Title", "Unknown"]) and extracted_title:
            effective_title = extracted_title
        
        effective_summary = existing_summary or extracted_description or ""
        extraction_text = f"{effective_title} {effective_summary} {clean_text[:2000]}"
        interests = _worker_nlp_service.extract_interests(extraction_text)
        
        # 3. Entity & Tag extraction
        flat_interests = [item for sublist in interests.values() for item in sublist]
        all_entities = list(set(flat_interests))[:15]

        return {
            "content": raw_text,
            "entities": all_entities,
            "interests": interests,
            "embedding": embedding,
            "extracted_title": extracted_title,
            "extracted_description": extracted_description,
            "published_date": published_date,
       
        }
    except Exception as e:
        print(f"CPU Task Error: {e}")
        return None

class EnrichingDomain:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    def __init__(self, max_cpu_workers=2, http_concurrency=5):
        self.settings = Settings()
        self.max_cpu_workers = max_cpu_workers
        self.http_concurrency = http_concurrency
        self.semaphore = None
        self.cpu_executor = ProcessPoolExecutor(
            max_workers=max_cpu_workers, 
            initializer=init_worker
        )
        self.headers = {
            "User-Agent": self.USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    def _parse_date(self, date_val: str | datetime) -> Optional[datetime]:
        if not date_val:
            return None
        if isinstance(date_val, datetime):
            return date_val
        try:
            # Handle standard ISO format
            return datetime.fromisoformat(str(date_val).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            # If format is unknown, we return None to avoid crashing
            # The DB will keep the original date or None
            return None

    def shutdown(self):
        self.cpu_executor.shutdown(wait=True)

    def warmup(self):
        logger.info(f"🔥 Warming up {self.max_cpu_workers} CPU workers (loading AI models)...")
        futures = [self.cpu_executor.submit(pow, 1, 1) for _ in range(self.max_cpu_workers)]
        for f in futures:
            f.result()
        logger.success("✅ CPU workers ready.")

    async def process_batch(self, session: Session, jobs: List[ArticlesQueueModel]):
        if not jobs: return

        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(self.http_concurrency)

        loop = asyncio.get_running_loop()
        
        async with aiohttp.ClientSession(headers=self.headers) as http_session:
            tasks = []
            for job in jobs:
                tasks.append(self._process_single_job(loop, http_session, job))
            
            await asyncio.gather(*tasks)

    async def _process_single_job(self, loop, http_session, job):
        async with self.semaphore:
            article = job.article
            
            # 0. Age Check: Ignore articles older than 7 days
            if article.published_date:
                pdate = article.published_date
                if pdate.tzinfo is None:
                    pdate = pdate.replace(tzinfo=timezone.utc)
                
                if pdate < datetime.now(timezone.utc) - timedelta(days=7):
                    job.status = JobStatus.COMPLETED
                    job.msg = "Skipped: Too old (> 7 days)"
                    return

            try:
                html = None
                # Check if we need to fetch content
                has_content = article.contents and len(article.contents) > 0 and len(article.contents[0].content) > 100
                
                if not has_content:
                    try:
                        timeout = aiohttp.ClientTimeout(total=30)
                        async with http_session.get(article.original_url, timeout=timeout) as resp:
                            if resp.status == 200:
                                # Check Content-Type to avoid binary files (images, PDFs)
                                ctype = resp.headers.get("Content-Type", "").lower()
                                if "text" not in ctype and "html" not in ctype and "json" not in ctype:
                                    logger.warning(f"Skipping non-text content {article.id}: {ctype}")
                                    job.status = JobStatus.FAILED
                                    job.msg = f"Skipped Content-Type: {ctype}"
                                    return

                                html = await resp.text(errors="replace")
                    except Exception as e:
                        logger.warning(f"Fetch failed for {article.id}: {e}")
                        job.status = JobStatus.FAILED
                        job.msg = f"Fetch Error: {str(e)[:50]}"
                        return

                    if not html:
                        job.status = JobStatus.FAILED
                        job.msg = "Empty HTML"
                        return
                else:
                    # If we already have content (e.g. from RSS), we treat it as HTML/Text for the processor
                    html = f"<html><body><p>{article.contents[0].content}</p></body></html>"

                # Offload CPU task
                result = await loop.run_in_executor(
                    self.cpu_executor,
                    partial(_process_content_cpu_task, html, article.title, article.summary)
                )

                if result:
                    if not has_content:
                        article.contents = [ArticleContentModel(content=result["content"])]
                    
                    # Update Metadata if missing
                    if (not article.title or article.title.lower() in ["no title", "unknown"]) and result.get("extracted_title"):
                        article.title = result["extracted_title"]

                    if not article.summary and result.get("extracted_description"):
                        article.summary = result["extracted_description"]
                    if not article.subtitle  and result.get("extracted_description"):
                        article.subtitle = result["extracted_description"]
                    if not article.published_date and result.get("published_date"):
                        article.published_date = self._parse_date(result["published_date"])
                    
                    article.embedding = result["embedding"]
                    article.interests = result["interests"]
                    article.entities = result["entities"]
                    
                    if not article.title or article.title.lower() in ["no title", "unknown"]:
                        logger.error(f"❌ Article {article.id} remains untitled after enrichment. URL: {article.original_url}")
                        job.status = JobStatus.FAILED
                        job.msg = "Extraction"
                        return
                    
                    job.status = JobStatus.PENDING
                    job.queue_name = ArticlesQueueName.CLUSTER
                    job.updated_at = datetime.now(timezone.utc)
                    logger.success(f"✅ Enriched: {article.title[:30]}")
                else:
                    job.status = JobStatus.FAILED
                    job.msg = "Extraction/NLP Failed"

            except Exception as e:
                logger.error(f"Enrichment error {article.id}: {e}")
                job.status = JobStatus.FAILED
                job.msg = f"Critical: {str(e)[:50]}"