import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import aiohttp
import trafilatura
from app.config import Settings
from app.core.browser import BrowserFetcher
from app.core.llm_parser import CloudNewsAnalyzer
from app.core.nlp_service import NLPService
from bs4 import BeautifulSoup
from dateutil import parser
from htmldate import find_date
from loguru import logger

# Project Imports
from news_events_lib.models import ArticleModel, JobStatus


class EnrichmentStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ARCHIVED = "ARCHIVED"


@dataclass
class EnrichmentResult:
    status: EnrichmentStatus
    reason: str


class ContentEnricherDomain:
    def __init__(self):
        self.settings = Settings()
        self.nlp_service = NLPService()
        self.llm = CloudNewsAnalyzer()

        self.user_agents = BrowserFetcher.USER_AGENTS

    async def enrich_article(self, session, article: ArticleModel) -> EnrichmentResult:
        if self._is_complete(article):
            return EnrichmentResult(EnrichmentStatus.SUCCESS, "Already Enriched")

        url = article.original_url
        if self._is_binary_resource(url):
            return EnrichmentResult(EnrichmentStatus.ARCHIVED, "Binary Extension")

        # 1. Fetch Content
        html = await self._fetch_html(url)
        if html == "binary":
            return EnrichmentResult(
                EnrichmentStatus.ARCHIVED, "Binary Content Detected"
            )
        if not html:
            return EnrichmentResult(EnrichmentStatus.FAILED, "Could not fetch content")

        if self._check_for_blocking(html):
            return EnrichmentResult(EnrichmentStatus.FAILED, "WAF/Blocking Detected")

        # 2. Parse Content
        try:
            extracted = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=False,
                output_format="json",
                with_metadata=True,
            )

            content_text = ""
            trafilatura_date = None

            if extracted:
                data = json.loads(extracted)
                content_text = data.get("raw_text", "")

                # Capture Trafilatura's guess (Priority 6)
                if data.get("date"):
                    try:
                        trafilatura_date = parser.parse(data["date"])
                    except Exception:
                        pass

                if (
                    not article.title
                    or len(article.title) < 5
                    or article.title == "Unknown"
                ):
                    article.title = data.get("title") or "Unknown"
                if not article.subtitle:
                    article.subtitle = data.get("excerpt")

            if not content_text:
                soup = BeautifulSoup(html, "html.parser")
                for script in soup(
                    ["script", "style", "nav", "footer", "header", "iframe", "svg"]
                ):
                    script.extract()
                content_text = soup.get_text(" ", strip=True)

            if len(content_text) < 100:
                return EnrichmentResult(
                    EnrichmentStatus.FAILED, f"Content too short/empty - {content_text}"
                )

            # 3. DATE EXTRACTION (Your 6-Step Strategy)
            if not article.published_date:
                soup = BeautifulSoup(html, "html.parser")

                found_date = self.extract_published_date(
                    soup, url, html, trafilatura_date
                )

                if found_date:
                    article.published_date = found_date
                else:
                    # Last Resort: LLM
                    try:
                        llm_date_str = await self.llm.extract_date_from_html(
                            html[:4000]
                        )
                        if llm_date_str:
                            dt = parser.parse(llm_date_str)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            article.published_date = dt
                    except Exception:
                        pass

            # 4. Age Check
            if article.published_date:
                if article.published_date.tzinfo is None:
                    article.published_date = article.published_date.replace(
                        tzinfo=timezone.utc
                    )

                cutoff = datetime.now(timezone.utc) - self.settings.cutoff_period
                if article.published_date < cutoff:
                    return EnrichmentResult(
                        EnrichmentStatus.ARCHIVED,
                        f"Too Old ({article.published_date.date()})",
                    )

            # 5. Embedding
            clean_text = self.nlp_service.clean_text_for_embedding(content_text)
            article.embedding = self.nlp_service.calculate_vector(clean_text)

            # Pass content to Worker
            article.content = content_text
            article.summary_status = JobStatus.WAITING
            return EnrichmentResult(EnrichmentStatus.SUCCESS, "Enriched successfully")

        except Exception as e:
            logger.opt(exception=True).error(f"Enrichment Logic Error: {e}")
            return EnrichmentResult(
                EnrichmentStatus.FAILED, f"Logic Error: {str(e)[:100]}"
            )

    # --- ADVANCED EXTRACTION HELPERS ---

    @staticmethod
    def extract_published_date(
        soup: BeautifulSoup, url: str, html: str, traf_date: Optional[datetime]
    ) -> Optional[datetime]:
        """
        Your Requested Strategy:
        1. JSON-LD (Reliable)
        2. URL Date + Body Regex (Composite)
        3. htmldate (Strong Signals Only)
        4. Head Metadata (No Body/Sidebar)
        5. Regex "Publicado em"
        6. Trafilatura Fallback
        """

        # 1. JSON-LD
        # Very reliable, handles @graph for WordPress
        ld_date = ContentEnricherDomain.extract_json_ld(soup)
        if ld_date:
            return ld_date

        # 2. URL + Body Regex
        # If URL has YYYY/MM/DD, scan body for that date + time
        url_date = ContentEnricherDomain.extract_date_from_url(url)
        if url_date:
            # Try to refine it with time from text
            refined_date = ContentEnricherDomain.extract_time(soup, url_date)
            if refined_date:
                return refined_date
            # If no time found in text, return the date from URL (still very good)
            return url_date

        # 3. HTMLDate (Strong Signal)
        # We limit extensive_search=False to avoid random footer dates
        try:
            # Check if there is a <time> tag or specific class before running
            has_strong_signal = soup.find("time") or soup.find(
                class_=re.compile(r"date|time|published", re.I)
            )

            if has_strong_signal:
                found = find_date(
                    html,
                    original_date=True,
                    extensive_search=False,
                    outputformat="%Y-%m-%d %H:%M:%S%z",
                )
                if found:
                    return parser.parse(found)
        except Exception:
            pass

        # 4. Metadata (HEAD ONLY)
        # Avoids "Related Articles" in body polluting the result
        if soup.head:
            meta_targets = [
                {"property": "article:published_time"},
                {"property": "og:published_time"},
                {"name": "pubdate"},
                {"name": "sailthru.date"},
            ]
            for attrs in meta_targets:
                tag = soup.head.find("meta", attrs)  # type: ignore
                if tag and tag.get("content"):
                    try:
                        return parser.parse(tag["content"])
                    except Exception:
                        continue

        # 5. Regex (Portuguese)
        # Scans visible text for "Publicado em..."
        date = None
        r_date_pattern = r"(?:publicado|atualizado|data).*?(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{4}(?:\s+\d{1,2}[:h]\d{2})?)"
        # Limit search to likely containers to avoid footer noise
        candidates = soup.find_all(
            class_=re.compile(r"(date|time|published|meta|header|info)", re.I)
        )

        for obj in candidates:
            match = re.search(r_date_pattern, obj.get_text(), re.I)
            if match:
                try:
                    date_str = match.group(1).replace("h", ":")
                    date = parser.parse(date_str, dayfirst=True)
                    break
                except Exception:
                    continue

        if date:
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return date

        # 6. Trafilatura Fallback
        if traf_date:
            if traf_date.tzinfo is None:
                traf_date = traf_date.replace(tzinfo=timezone.utc)
            return traf_date

        return None

    @staticmethod
    def extract_json_ld(soup):
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                if not script.string:
                    continue
                data = json.loads(script.string)

                # Handle @graph (WordPress/Yoast)
                objects = []
                if isinstance(data, dict):
                    if "@graph" in data and isinstance(data["@graph"], list):
                        objects = data["@graph"]
                    else:
                        objects = [data]
                elif isinstance(data, list):
                    objects = data

                for item in objects:
                    if not isinstance(item, dict):
                        continue
                    otype = item.get("@type")

                    is_news = False
                    if isinstance(otype, list):
                        if any(
                            t in ["NewsArticle", "Article", "BlogPosting", "Report"]
                            for t in otype
                        ):
                            is_news = True
                    elif otype in ["NewsArticle", "Article", "BlogPosting", "Report"]:
                        is_news = True

                    if is_news:
                        # Extract date
                        d_str = item.get("datePublished") or item.get("dateCreated")
                        if d_str:
                            dt = parser.parse(d_str)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            return dt

            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return None

    @staticmethod
    def extract_date_from_url(url: str) -> Optional[datetime]:
        """Finds YYYY/MM/DD in URL."""
        if not url:
            return None
        match = re.search(r"/(\d{4})[/-](\d{1,2})[/-](\d{1,2})", url)
        if match:
            try:
                y, m, d = map(int, match.groups())
                return datetime(y, m, d, tzinfo=timezone.utc)
            except ValueError:
                pass
        return None

    @staticmethod
    def extract_time(soup: BeautifulSoup, date: datetime) -> datetime:
        """
        Scans the text for the given date (formatted various ways)
        followed by a time pattern (HH:MM).
        """
        if not date or (date.hour != 0 or date.minute != 0):
            return date

        text = soup.get_text(" ", strip=True)

        # Formats to look for: 20/01/2024, 2024-01-20, 20.01.24
        d, m, y = date.day, date.month, date.year

        # We construct a loose regex for the date part
        # Matches: 20/01, 20.01, 2024-01-20
        date_patterns = [
            f"{d:02d}[/.]{m:02d}",  # 20/01
            f"{y}-{m:02d}-{d:02d}",  # 2024-01-20
        ]

        for d_pat in date_patterns:
            # Look for Date... (within 50 chars) ... Time
            # Time: 14:30 or 14h30
            pattern = f"{d_pat}" + r".{0,50}?(\d{1,2}[:h]\d{2})"

            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw_time = match.group(1).replace("h", ":")
                if len(raw_time) == 4:
                    raw_time = "0" + raw_time
                try:
                    h = int(raw_time[:2])
                    mn = int(raw_time[3:])
                    return date.replace(hour=h, minute=mn)
                except Exception:
                    pass

        return date

    # ... (Headers and Fetch helpers remain identical) ...
    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

    async def _fetch_html(self, url: str) -> str:
        try:
            timeout = aiohttp.ClientTimeout(total=25, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    url, headers=self._get_headers(), allow_redirects=True
                ) as resp:
                    if resp.status == 200:
                        ctype = resp.headers.get("Content-Type", "").lower()
                        if "application/pdf" in ctype or "image/" in ctype:
                            return "binary"
                        return await resp.text()
        except Exception:
            pass
        try:
            return await BrowserFetcher.fetch(url)
        except Exception:
            return ""

    def _is_complete(self, article: ArticleModel) -> bool:
        has_embedding = article.embedding is not None and len(article.embedding) > 0
        has_content = (
            hasattr(article, "content")
            and article.content is not None
            and len(article.content) > 0
        )
        return has_embedding and has_content

    def _is_binary_resource(self, url: str) -> bool:
        clean = url.split("?")[0].lower()
        return clean.endswith((".pdf", ".jpg", ".png", ".gif", ".mp4", ".zip", ".exe"))

    def _check_for_blocking(self, html: str) -> bool:
        if not html:
            return False
        head = html[:1000].lower()
        return any(
            x in head
            for x in ["cloudflare", "access denied", "403 forbidden", "captcha"]
        )
