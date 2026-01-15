import asyncio
import json
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import aiohttp
from sqlalchemy import select
import trafilatura
from loguru import logger
from sqlalchemy.orm import Session

# Project Imports
from core.nlp_service import NLPService
from core.llm_parser import CloudNewsAnalyzer, LLMNewsOutputSchema, LLMSummaryResponse  # 👈 Added LLM
from news_events_lib.models import ArticleModel, ArticleContentModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.browser import BrowserFetcher

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

def _process_content_cpu_task(html: str, title: str, existing_summary: str, force_vectorize: bool = False) -> Optional[dict]:
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

        # 1. Extraction (Trafilatura)
        if html:
            # Check if it's raw HTML or pre-extracted text
            if html.strip().startswith("<") or "http" in html:
                extracted_json = trafilatura.extract(
                    html, with_metadata=True, output_format="json", include_comments=False
                )
                if extracted_json:
                    data = json.loads(extracted_json)
                    raw_text = data.get("raw_text", "")
                    extracted_title = data.get("title")
                    extracted_description = data.get("excerpt")
                    published_date = data.get("date", data.get('filedate'))
            else:
                # Assume it's already plain text (Re-processing existing content)
                raw_text = html
        
        # 2. Validation
        if not raw_text or len(raw_text) < 100: 
            return None 

        # 3. NLP & Vectorization
        # We clean and vectorize if this is a new scrape OR if we are forcing a re-vectorization
        clean_text = _worker_nlp_service.clean_text_for_embedding(raw_text)
        embedding = _worker_nlp_service.calculate_vector(clean_text)
        
        # Use extracted metadata if original is missing/placeholder
        effective_title = extracted_title if extracted_title else title
        
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
        logger.error(f"CPU Task Error: {e}")
        return None

class EnrichingDomain:
    USER_AGENTS = BrowserFetcher.USER_AGENTS

    def __init__(self, max_cpu_workers=2, http_concurrency=5):
        self.settings = Settings()
        self.max_cpu_workers = max_cpu_workers
        self.http_concurrency = http_concurrency
        self.semaphore = asyncio.Semaphore(self.http_concurrency)
        self.cpu_executor = ProcessPoolExecutor(
            max_workers=max_cpu_workers, 
            initializer=init_worker
        )
        self.headers = {
            "User-Agent": self.USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        
        # LLM for Phase 2
        self.llm = CloudNewsAnalyzer()
        
        # 🚦 NEW: Control concurrency for the LLM 
        # 12B is heavier than Flash, so we limit to 5 parallel requests to avoid Rate Limits (429)
        self.llm_semaphore = asyncio.Semaphore(5) 

    def _check_for_blocking(self, html: str) -> Optional[str]:
        """
        Checks for common WAF/Blocking signatures in the HTML.
        Returns the name of the blocker if found, else None.
        """
        if not html: return None
        
        # Check first 3KB for efficiency (WAF pages are usually small)
        head = html[:3000].lower()
        
        signatures = {
            "Cloudflare": ["attention required! | cloudflare", "just a moment...", "security by cloudflare", "ray id:", "cloudflare-nginx"],
            "Incapsula": ["incapsula", "_incapsula_resource"],
            "Akamai": ["reference #18.", "reference #", "akamai"],
            "Generic Block": ["please turn javascript on", "checking your browser", "error 1020", "error 1015", "<title>403 forbidden</title>", "<title>access denied</title>", "enable cookies"]
        }
        
        for blocker, sigs in signatures.items():
            for sig in sigs:
                if sig in head:
                    return blocker
        return None

    def shutdown(self):
        self.cpu_executor.shutdown(wait=True)

    def warmup(self):
        logger.info(f"🔥 Warming up {self.max_cpu_workers} CPU workers...")
        futures = [self.cpu_executor.submit(pow, 1, 1) for _ in range(self.max_cpu_workers)]
        for f in futures: f.result()
        logger.success("✅ CPU workers ready.")

    async def process_batch(self, session: Session, jobs: List[ArticlesQueueModel]):
        if not jobs: return

        loop = asyncio.get_running_loop()
        
        # 1. PHASE 1: Fetch & Vectorize (Parallel)
        async with aiohttp.ClientSession(headers=self.headers) as http_session:
            tasks = [self._process_single_job_fetch_and_cpu(loop, http_session, job) for job in jobs]
            results = await asyncio.gather(*tasks)

        # 2. PHASE 2: LLM Analysis (Concurrent Chunking)
        
        llm_candidates = []
        llm_indices = []

        # Filter valid items
        for i, (job, res, needs_refilter) in enumerate(results):
            if res and not needs_refilter:
                art = job.article
                # Apply temporary extracted data for LLM context
                art.title = res.get("extracted_title") or art.title
                if not art.contents:
                     art.contents = [ArticleContentModel(content=res["content"])]
                else:
                     art.contents[0].content = res["content"]
                
                llm_candidates.append(art)
                llm_indices.append(i)

        if llm_candidates:
            logger.info(f"🧠 Processing {len(llm_candidates)} articles with Gemma-12B (Parallel)...")
            
            # --- CHUNKING LOGIC ---
            BATCH_SIZE = 5  # Small chunks prevent context overflow and timeouts
            chunks = [llm_candidates[i:i + BATCH_SIZE] for i in range(0, len(llm_candidates), BATCH_SIZE)]
            
            async def process_chunk(chunk_articles):
                async with self.llm_semaphore: # Guard against 429 errors
                    texts = [f"{a.title}\n\n{a.contents[0].content}" for a in chunk_articles]
                    return await self.llm.analyze_articles_batch(texts)

            # Fire all chunks at once!
            chunk_tasks = [process_chunk(chunk) for chunk in chunks]
            
            try:
                # Wait for all chunks
                chunk_results = await asyncio.gather(*chunk_tasks)
                
                # Flatten the list of lists: [[A,B], [C,D]] -> [A,B,C,D]
                flat_outputs = [item for sublist in chunk_results for item in sublist]

                # Map results back to the original jobs
                for k, output in enumerate(flat_outputs):
                    if k < len(llm_indices):
                        original_idx = llm_indices[k]
                        # Inject result into the Phase 3 pipeline
                        results[original_idx][1]["llm_output"] = output
                        
            except Exception as e:
                logger.error(f"⚠️ Concurrent LLM Batch Failed: {e}")

        # 3. PHASE 3: Save & Route
        for job, res, needs_refilter in results:
            if res:
                self._apply_enrichment_result(session, job, res, needs_refilter)
            else:
                if job.status == JobStatus.PROCESSING:
                     job.status = JobStatus.FAILED
                     job.msg = "Enrichment Failed"

    async def _process_single_job_fetch_and_cpu(self, loop, http_session, job) -> Tuple[ArticlesQueueModel, dict|None, bool]:
        """
        Returns: (job, result_dict, needs_refilter_bool)
        """
        async with self.semaphore:
            article = job.article
            needs_refilter = False

            # Flag: Did this article start with a bad title?
            bad_title_start = not article.title or article.title.lower() in ["no title", "unknown", ""]
            
            # 0. Age Check: Ignore articles older than 7 days
            if article.published_date:
                pdate = article.published_date
                if pdate.tzinfo is None:
                    pdate = pdate.replace(tzinfo=timezone.utc)
                
                if pdate < datetime.now(timezone.utc) - timedelta(days=2):
                    job.status = JobStatus.COMPLETED
                    job.msg = "Skipped: Too old (> 2 days)"
                    return job, None, False

            try:
                html = None
                # 1. OPTIMIZATION: Check for existing content (Pass 2 Logic)
                has_content = article.contents and len(article.contents) > 0 and len(article.contents[0].content) > 100
                
                if has_content:
                    # ✅ Skip Scrape
                    html = article.contents[0].content
                    # If we have content, we assume it's raw text
                else:
                    # 🌐 Scrape (Pass 1)
                    use_browser = False
                    try:
                        timeout = aiohttp.ClientTimeout(total=30)
                        async with http_session.get(article.original_url, timeout=timeout) as resp:
                            if resp.status in [403, 429, 503]:
                                logger.warning(f"⚠️ HTTP {resp.status} for {article.id}. Triggering Browser Fallback...")
                                use_browser = True
                            
                            elif resp.status == 200:
                                # Check Content-Type to avoid binary files (images, PDFs)
                                ctype = resp.headers.get("Content-Type", "").lower()
                                if "text" not in ctype and "html" not in ctype and "json" not in ctype:
                                    logger.warning(f"Skipping non-text content {article.id}: {ctype}")
                                    job.status = JobStatus.COMPLETED
                                    job.msg = f"Skipped Content-Type: {ctype}"
                                    return job, None, False

                                html = await resp.text(errors="replace")
                                
                                # SPA / Empty Fallback
                                if not html or len(html) < 500 or "<div id=\"root\"></div>" in html:
                                    logger.warning(f"⚠️ Empty/SPA HTML for {article.id}. Triggering Browser Fallback...")
                                    use_browser = True

                            else:
                                job.status = JobStatus.FAILED
                                job.msg = f"HTTP {resp.status}"
                                return job, None, False
                    except Exception as e:
                        logger.warning(f"Standard Fetch failed for {article.id}: {e}. Triggering Browser Fallback...")
                        use_browser = True

                    if use_browser:
                        html = await BrowserFetcher.fetch(article.original_url)

                if not html:
                    job.status = JobStatus.FAILED
                    job.msg = "Empty HTML"
                    return job, None, False

                # 🛡️ BLOCKING DETECTION
                blocker = self._check_for_blocking(html)
                if blocker:
                    job.status = JobStatus.FAILED
                    job.msg = f"Blocked: {blocker}"
                    return job, None, False

                # 2. CPU Task (Embeddings)
                result = await loop.run_in_executor(
                    self.cpu_executor,
                    partial(_process_content_cpu_task, str(html), article.title, article.summary)
                )

                if result and bad_title_start:
                    new_title = result.get("extracted_title")
                    if new_title and new_title.lower() not in ["no title", "unknown", ""]:
                        # 🚨 BOOMERANG TRIGGER
                        # We found a title for a previously untitled article.
                        # Send back to FILTER to ensure it's not spam.
                        needs_refilter = True
                
                return job, result, needs_refilter

            except Exception as e:
                logger.error(f"Enrichment error {article.id}: {e}")
                job.status = JobStatus.FAILED
                job.msg = f"Critical: {str(e)[:50]}"
                return job, None, False

    def _apply_enrichment_result(self, session,  job, result, needs_refilter):
        article: ArticleModel = job.article
        
        # 1. Content & Metadata
        if not article.contents or not article.contents[0].content:
             article.contents = [ArticleContentModel(content=result["content"])]
        
        if (not article.title or article.title.lower() in ["no title", "unknown"]) and result.get("extracted_title"):
            article.title = result["extracted_title"]
        
        if not article.published_date and result.get("published_date"):
            article.published_date = self._parse_date(result["published_date"]) or datetime.now(timezone.utc)
        
        article.embedding = result["embedding"]
        article.interests = result["interests"]
        article.entities = result["entities"]
        
        # 2. LLM Data (If available)
        llm_out:LLMNewsOutputSchema | None= result.get("llm_output")
        if llm_out:
            if llm_out.status == "error":
                job.status = JobStatus.FAILED
                job.msg = f"LLM Blocked: {llm_out.error_message}"
                return
            # Handle Content Irrelevance (The new logic)
            if llm_out.status == "irrelevant":
                job.status = JobStatus.COMPLETED # Or REJECTED
                job.msg = f"Content Irrelevant: {llm_out.error_message}"
                # Do NOT queue for clustering
                return
            article.summary = llm_out.summary
            article.stance = llm_out.stance
            article.stance_reasoning = llm_out.stance_reasoning
            article.clickbait_score = llm_out.clickbait_score
            article.clickbait_reasoning = llm_out.clickbait_reasoning
            article.key_points = llm_out.key_points
            article.interests = llm_out.entities
            article.main_topics = llm_out.main_topics
            article.title = llm_out.title
            article.subtitle = llm_out.subtitle
            for entities in llm_out.entities.values():
                article.entities.extend(entities)
            article.summary_date = datetime.now(timezone.utc)
            article.summary_status = JobStatus.COMPLETED
            

        # 3. ROUTING LOGIC
        
        if needs_refilter:
            logger.info(f"↩️ Boomerang: Sending '{article.title[:20]}' back to FILTER.")
            job.status = JobStatus.PENDING
            job.queue_name = ArticlesQueueName.FILTER
            job.updated_at = datetime.now(timezone.utc)
            return

        # 2. Untitled Check
        if not article.title or article.title.lower() in ["no title", "unknown", ""]:
            job.status = JobStatus.REJECTED
            job.msg = "Untitled (Auto-Reject)"
            return

        # 3. SHORT CIRCUIT: Existing Event (The Optimization)
        # If this article is already part of an event (e.g., it was just updated/corrected),
        # we skip the Cluster worker to avoid double-counting and go straight to Enhancer.
        if article.event_id:
            logger.info(f"⚡ Article {article.id} already in Event {article.event_id}. Waking Enhancer.")
            
            # A. Wake up the Event
            # We use a raw SQL upsert or a helper to push the Event to the Enhancer Queue
            from core.models import EventsQueueModel, EventsQueueName # Ensure imports
            
            # Check if event job exists
            existing_event_job = session.scalar(
                select(EventsQueueModel).where(EventsQueueModel.event_id == article.event_id)
            )
            
            if existing_event_job:
                existing_event_job.status = JobStatus.PENDING
                existing_event_job.queue_name = EventsQueueName.ENHANCER
                existing_event_job.updated_at = datetime.now(timezone.utc)
            else:
                new_event_job = EventsQueueModel(
                    event_id=article.event_id,
                    queue_name=EventsQueueName.ENHANCER,
                    status=JobStatus.PENDING,
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc)
                )
                session.add(new_event_job)

            # B. Mark Article as Done (Skip Cluster)
            job.status = JobStatus.COMPLETED
            job.queue_name = ArticlesQueueName.CLUSTER # Technically done, but keeping name for history is fine
            job.updated_at = datetime.now(timezone.utc)
            return

        # 4. Standard Flow: Go to Cluster
        job.status = JobStatus.PENDING
        job.queue_name = ArticlesQueueName.CLUSTER
        job.updated_at = datetime.now(timezone.utc)
        logger.success(f"✅ Enriched: {article.title[:30]} -> Cluster")
    
    def _parse_date(self, date_val: str | datetime) -> Optional[datetime]:
        if not date_val: return None
        
        dt = None
        if isinstance(date_val, datetime): 
            dt = date_val
        else:
            try:
                dt = datetime.fromisoformat(str(date_val).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None
        
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            # Future Guard: If date is > 1 hour in future, clamp to now
            now = datetime.now(timezone.utc)
            if dt > (now + timedelta(hours=1)):
                return now
            return dt
        return None