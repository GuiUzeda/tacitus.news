import asyncio
import json
import re
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Optional, Tuple, Dict
from datetime import datetime, timezone, timedelta

import aiohttp
import trafilatura
from loguru import logger
from htmldate import find_date
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup
from dateutil import parser

# Project Imports
from core.nlp_service import NLPService
from core.llm_parser import CloudNewsAnalyzer, LLMNewsOutputSchema
from news_events_lib.models import ArticleModel, ArticleContentModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName
from config import Settings
from core.browser import BrowserFetcher

# --- WORKER GLOBAL STATE ---
_worker_nlp_service = None


def init_worker():
    """Initializes NLP models in the separate process."""
    global _worker_nlp_service
    try:
        _worker_nlp_service = NLPService()
    except Exception as e:
        logger.error(f"Worker init failed: {e}")


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
                    # @graph is usually a list, but handle single object edge case
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


def _extract_time(bs4_html: BeautifulSoup, date: datetime):
    # Safety: Only run if date is valid
    if not date:
        return None

    if date.hour == 0 and date.minute == 0:
        text_content = bs4_html.get_text(separator="\n", strip=True)

        d_year, d_month, d_day = date.year, date.month, date.day

        # Regex to find date + optional junk + time
        date_part = f"(?:{d_year}.{d_month}.{d_day}|{d_day}.{d_month}.{d_year})"
        time_part = r"(\d{1,2}(:|h)\d{2})"
        full_pattern = f"{date_part}.*?{time_part}"

        time_match = re.search(
            full_pattern, text_content[:5000], re.DOTALL | re.IGNORECASE
        )

        if time_match:
            # Group 1 is the full time string (e.g. "14h30"), Group 2 is separator
            # Wait! In "(\d{1,2}(:|h)\d{2})", group 1 is the full match, group 2 is ":" or "h"
            # Your previous code used group(1), which is correct.
            raw_time = time_match.group(1).replace("h", ":")

            # Handle "9:30" -> "09:30"
            if len(raw_time) == 4:
                raw_time = "0" + raw_time

            try:
                hour = int(raw_time[:2])
                minute = int(raw_time[3:5])
                date = date.replace(hour=hour, minute=minute)
            except ValueError:
                pass  # regex matched something weird, ignore

    return date


async def _get_html(http_session, url: str) -> str:
    html = ""
    try:
        async with http_session.get(url, timeout=30) as resp:
            if resp.status == 200:
                html = await resp.text()
            elif resp.status in [403, 429]:
                html = await BrowserFetcher.fetch(url)

    except Exception:
        html = await BrowserFetcher.fetch(url)
    finally:
        return str(html)


def _extract_published_date(soup: BeautifulSoup):
    published_date = None
    ld_json = extract_json_ld(soup)
    if ld_json:
        try:
            date_published = ld_json.get("datePublished") or ld_json.get("dateCreated")
            date_updated = ld_json.get("dateModified") or ld_json.get("dateUpdated")

            dt_pub = None
            dt_mod = None

            if date_published:
                dt_pub = parser.parse(date_published)
                if dt_pub.tzinfo is None:
                    dt_pub = dt_pub.replace(tzinfo=timezone.utc)

            if date_updated:
                dt_mod = parser.parse(date_updated)
                if dt_mod.tzinfo is None:
                    dt_mod = dt_mod.replace(tzinfo=timezone.utc)

            if dt_pub and dt_mod:
                published_date = dt_pub if dt_pub <= dt_mod else dt_mod
            elif dt_pub:
                published_date = dt_pub
            elif dt_mod:
                published_date = dt_mod
        except Exception:
            pass

    if not published_date:
        # A. Strong Signal Check
        has_strong_signal = (
            soup.find("time")
            or soup.find(
                "meta", attrs={"property": re.compile(r"date|time|published", re.I)}
            )
            or soup.find(
                "meta", attrs={"name": re.compile(r"date|time|published", re.I)}
            )
        )

        if has_strong_signal:
            try:
                # Use htmldate
                date_str = find_date(
                    str(soup),
                    original_date=True,
                    outputformat="%Y-%m-%dT%H:%M:%S",
                    extensive_search=False,
                )
                if date_str:
                    date = parser.parse(date_str)
                    date = _extract_time(soup, date)
                    published_date = date
            except Exception:
                pass

        # B. Fallback Regex
    if not published_date:
        date = None
        r_date_pattern = r"(?:publicado|atualizado|data).*?(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{4}(?:\s+\d{1,2}[:h]\d{2})?)"
        candidates = soup.find_all(
            class_=re.compile(r"(date|time|published|meta)", re.I)
        )
        for obj in candidates:
            match = re.search(r_date_pattern, obj.get_text(), re.I)
            if match:
                date_str = match.group(1).replace("h", ":")
                date = parser.parse(date_str)
                break

        # Parse String to Datetime
        if date:
            try:

                if date.tzinfo is None:
                    date= date.replace(tzinfo=timezone.utc)
                published_date = date
            except:
                pass
    return published_date


def _cpu_extract_date_and_content(html: str, result) -> Dict:
    """
    Combined CPU Task: Date Extraction -> Age Check -> Content -> Vectorization.
    """
    global _worker_nlp_service
    if _worker_nlp_service is None:
        _worker_nlp_service = NLPService()

    soup = BeautifulSoup(html, "html.parser")

    # --- STEP 3: TRY TO GET DATE (If missing) ---
    if not result["published_date"]:
        published_date = _extract_published_date(soup)
        if not published_date:
            result["status"] = "failed"
            result["stop_reason"] = "No Date Found"
            return result
        result["published_date"] = published_date

    # --- STEP 4: AGE CHECK (Fail Fast) ---
    if result["published_date"]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        if result["published_date"] < cutoff:
            # STOP HERE: The article is too old.
            result["status"] = "archived"
            result["stop_reason"] = f"Old Article ({result['published_date'].date()})"
            return result
    if not result["content"]:
        # --- STEP 2 (Continued): CONTENT EXTRACTION ---
        extracted = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            output_format="json",
            with_metadata=True,
        )

        raw_text = ""
        if extracted:
            data = json.loads(extracted)
            raw_text = data.get("raw_text", "")
            result["title"] = data.get("title")
            result["subtitle"] = data.get("excerpt")
            result["content"] = raw_text

    if not result["content"] or len(result["content"]) < 100:
        result["status"] = "failed"
        result["stop_reason"] = "Empty Content"
        return result

    if not result["embedding"]:
        clean_text = _worker_nlp_service.clean_text_for_embedding(result["content"])
        result["embedding"] = _worker_nlp_service.calculate_vector(clean_text)

    return result


# --- MAIN SERVICE ---


class EnrichingDomain:
    def __init__(self, max_cpu_workers=2, http_concurrency=10):
        self.settings = Settings()
        self.semaphore = asyncio.Semaphore(http_concurrency)
        self.cpu_executor = ProcessPoolExecutor(
            max_workers=max_cpu_workers, initializer=init_worker
        )
        self.llm = CloudNewsAnalyzer()
        self.llm_semaphore = asyncio.Semaphore(5)
        self.max_cpu_workers = max_cpu_workers

    async def shutdown(self):
        self.cpu_executor.shutdown(wait=True)

    def warmup(self):
        logger.info(f"🔥 Warming up {self.max_cpu_workers} CPU workers...")
        futures = [
            self.cpu_executor.submit(pow, 1, 1) for _ in range(self.max_cpu_workers)
        ]
        for f in futures:
            f.result()
        logger.success("✅ CPU workers ready.")

    # --- PHASE 1: CPU (Fetch, Filter, Vectorize) ---
    async def run_cpu_enrichment(
        self, jobs: List[ArticlesQueueModel]
    ) -> List[Tuple[ArticlesQueueModel, Dict]]:
        """
        Processes a raw batch of jobs. Returns a list of (Job, ResultDict).
        Does NOT write to DB.
        """
        loop = asyncio.get_running_loop()
        async with aiohttp.ClientSession() as http_session:
            tasks = [
                self._process_single_job_fetch_and_cpu(loop, http_session, job)
                for job in jobs
            ]
            return await asyncio.gather(*tasks)

    # --- PHASE 2: LLM (Summarize Batch) ---
    async def run_llm_enrichment(
        self, valid_items: List[Tuple[ArticlesQueueModel, Dict]]
    ):
        """
        Takes a list of pre-validated (Job, Result) tuples.
        Runs the LLM on them in one massive efficient batch.
        Updates the 'ResultDict' in-place with LLM data.
        """
        if not valid_items:
            return

        # 1. Prepare Inputs
        llm_inputs = []
        for job, res in valid_items:
            # We use the extracted title if available, else DB title
            title = res.get("title") or job.article.title
            date = res.get("published_date")
            content = res.get("content", "")[:15000]  # Cap context
            llm_inputs.append(f"Title: {title}\nDate: {date}\nContent: {content}")

        # 2. Call LLM
        logger.info(
            f"🧠 Optimized Batch: Running LLM on {len(llm_inputs)} valid articles..."
        )
        try:
            outputs = await self.llm.analyze_articles_batch(llm_inputs)

            # 3. Merge Results
            for (job, res), output in zip(valid_items, outputs):
                res["llm_output"] = output

        except Exception as e:
            logger.error(f"LLM Batch Failed: {e}")
            # Mark all as failed so we don't lose them
            for job, res in valid_items:
                res["status"] = "failed"
                res["stop_reason"] = f"LLM Batch Error: {str(e)[:50]}"

    def _check_for_blocking(self, html: str) -> Optional[str]:
        if not html:
            return None
        head = html[:2000].lower()
        if "cloudflare" in head or "access denied" in head or "403 forbidden" in head:
            return "WAF Block"
        return None

    async def _process_single_job_fetch_and_cpu(
        self,
        loop: asyncio.AbstractEventLoop,
        http_session: aiohttp.ClientSession,
        job: ArticlesQueueModel,
    ) -> Tuple[ArticlesQueueModel, Dict]:
        async with self.semaphore:
            article: ArticleModel = job.article

            result = {
                "status": "success",
                "published_date": article.published_date,
                "content": article.contents[0] if article.contents else None,
                "subtitle": article.subtitle,
                "title": article.title,
                "embedding": article.embedding,
                "stop_reason": "Original",
            }
            if (
                (not result["content"] or len(result["content"].content) < 100)
                or not result["published_date"]
                or not result["title"]
                or result["title"].lower() in ["unknwon", "no title"]
                or not result["embedding"]
                or sum(result["embedding"]) < 0
            ):
                html = await _get_html(http_session, article.original_url)
                if not html or self._check_for_blocking(html):
                    return job, {
                        "status": "failed",
                        "stop_reason": "Empty/Blocked",
                    }
                result = await loop.run_in_executor(
                    self.cpu_executor,
                    partial(_cpu_extract_date_and_content, html, result),
                )
                if result["status"] == "success" and not result["published_date"]:
                    try:
                        async with self.llm_semaphore:
                            llm_date_str = await self.llm.extract_date_from_html(
                                html[:5000]
                            )
                            if llm_date_str:
                                dt = parser.parse(llm_date_str)
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                result["published_date"] = dt

                                # Final Age Check after LLM finding
                                if dt < (
                                    datetime.now(timezone.utc) - timedelta(days=5)
                                ):
                                    result["status"] = "archived"
                                    result["stop_reason"] = "Old Article (LLM)"
                    except Exception as e:
                        return job, {
                            "status": "failed",
                            "stop_reason": "Data extraction Failed",
                        }
            if len(result["title"]) < 10 or len(result["content"]) < 100:
                result["status"] = "failed"
                result["stop_reason"] = "Empty Content"
                return job, result

            if (
                article.title.lower() in ["unknwon", "no title"]
                and result["status"] == "success"

            ):
                result["status"] = "boomerang"
                result["stop_reason"] = "No Title"

            return job, result
