import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from app.core.browser import BrowserFetcher
from bs4 import BeautifulSoup
from loguru import logger


@dataclass
class HarvestResult:
    articles: List[Dict] = field(default_factory=list)
    total_fetched: int = 0
    filtered_date: int = 0
    filtered_hash: int = 0
    filtered_block: int = 0
    errors: List[str] = field(default_factory=list)

    def merge(self, other: "HarvestResult"):
        self.articles.extend(other.articles)
        self.total_fetched += other.total_fetched
        self.filtered_date += other.filtered_date
        self.filtered_hash += other.filtered_hash
        self.filtered_block += other.filtered_block
        self.errors.extend(other.errors)


class BaseHarvester:
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=100),
    ):
        # Universal Garbage Filter (Applied to all links by default)
        self.blocklist: str = (
            r"(login|search|tag|gallery|/esporte/|futebol|bbb|horoscopo|gastronomia|"
            r"patrocinado|publicidade|quiz|ilustrada|podcast|web-stories|/live/|"
            r"\.(jpg|jpeg|png|gif|bmp|webp|svg|mp4|avi|mov|wmv|flv|mp3|wav|pdf|doc|docx|xls|xlsx|zip|rar|7z|exe|apk)(\?.*)?$)"
        )
        # Only accept news from the last 24h (Crucial for Indexes)
        self.cutoff_date = datetime.now(timezone.utc) - cutoff

        # Expanded User Agent List for Rotation
        self.user_agents = BrowserFetcher.USER_AGENTS

        # Standard Headers to mimic a real browser
        self.headers = {
            "User-Agent": self.user_agents[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }

    async def harvest(
        self, session, sources: List[dict], ignore_hashes: set = set()
    ) -> HarvestResult:
        """
        Orchestrator: Iterates sources, fetches data, and handles deduplication/merging.
        Returns detailed HarvestResult with metrics.
        """
        final_result = HarvestResult()
        unique_articles = {}

        for source in sources:
            try:
                # Delegate fetching
                feed_result = await self.fetch_feed_articles(
                    session, source, ignore_hashes
                )

                # Aggregate metrics
                final_result.total_fetched += feed_result.total_fetched
                final_result.filtered_date += feed_result.filtered_date
                final_result.filtered_hash += feed_result.filtered_hash
                final_result.filtered_block += feed_result.filtered_block
                final_result.errors.extend(feed_result.errors)

                # Merge Articles (Deduplication Logic)
                for art in feed_result.articles:
                    url = art["link"]
                    if url in unique_articles:
                        self._merge_article_data(unique_articles[url], art)
                    else:
                        unique_articles[url] = art
            except Exception as e:
                err_msg = f"Feed process failed {source.get('url')}: {e}"
                logger.error(err_msg)
                final_result.errors.append(err_msg)
                continue

        final_result.articles = list(unique_articles.values())
        return final_result

    async def fetch_feed_articles(
        self, session, source: dict, ignore_hashes: set = set()
    ) -> HarvestResult:
        """
        Strategy Implementation: Fetches articles based on feed type.
        Returns HarvestResult.
        """
        feed_type = source.get("feed_type", "sitemap")
        use_browser = source.get("use_browser_render", False)
        is_ranked = source.get("is_ranked", False)
        url = source["url"]

        # 1. HTML Scraping
        if feed_type == "html":
            if use_browser:
                return await self._fetch_from_html_browser(
                    url,
                    scroll_depth=source.get("scroll_depth", 0),
                    blocklist=source.get("blocklist"),
                    allowed_sections=source.get("allowed_sections"),
                    url_pattern=source.get("url_pattern"),
                    is_ranked=is_ranked,
                    ignore_hashes=ignore_hashes,
                )
            else:
                return await self._fetch_from_html(
                    session,
                    url,
                    blocklist=source.get("blocklist"),
                    allowed_sections=source.get("allowed_sections"),
                    url_pattern=source.get("url_pattern"),
                    is_ranked=is_ranked,
                    ignore_hashes=ignore_hashes,
                )

        # 2. RSS
        elif feed_type == "rss":
            return await self.harvest_rss(
                session,
                url,
                blocklist=source.get("blocklist"),
                allowed_sections=source.get("allowed_sections"),
                ignore_hashes=ignore_hashes,
            )

        # 3. Standard Sitemap
        elif feed_type == "sitemap":
            return await self._fetch(
                session,
                url,
                blocklist=source.get("blocklist"),
                allowed_sections=source.get("allowed_sections"),
                ignore_hashes=ignore_hashes,
                use_browser=use_browser,
            )

        # 4. Built-in Dynamic Sitemaps
        elif feed_type == "sitemap_index_id":
            return await self.harvest_latest_id(
                session,
                url,
                id_pattern=source.get("url_pattern"),
                ignore_hashes=ignore_hashes,
            )

        elif feed_type == "sitemap_index_date":
            return await self.harvest_latest_date(
                session,
                url,
                date_pattern=source.get("url_pattern"),
                ignore_hashes=ignore_hashes,
            )

        elif feed_type == "sitemap_index_date_id":
            return await self.harvest_latest_date_id(
                session,
                url,
                pattern=source.get("url_pattern"),
                ignore_hashes=ignore_hashes,
            )

        return HarvestResult()

    def _merge_article_data(self, existing: dict, new: dict):
        """Merges metadata, prioritizing Rank and Date."""
        old_rank = existing.get("rank")
        new_rank = new.get("rank")
        if new_rank is not None:
            if old_rank is None or new_rank < old_rank:
                existing["rank"] = new_rank

        if not existing.get("published") and new.get("published"):
            existing["published"] = new["published"]

        if (not existing.get("content") or existing["content"] == "Unknown") and (
            new.get("content") and new["content"] != "Unknown"
        ):
            existing["content"] = new["content"]

    async def _fetch_from_html(
        self,
        session,
        url: str,
        blocklist=None,
        allowed_sections=None,
        url_pattern=None,
        is_ranked=False,
        ignore_hashes=None,
    ) -> HarvestResult:
        """Fast Static HTML Fetch"""
        result = HarvestResult()
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30), headers=self.headers
            ) as response:
                if response.status != 200:
                    msg = f"Response Error {url}: {response.status}"
                    logger.error(msg)
                    result.errors.append(msg)
                    return result

                ctype = response.headers.get("Content-Type", "").lower()
                if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
                    msg = f"Skipping non-HTML seed {url}: {ctype}"
                    logger.warning(msg)
                    result.errors.append(msg)
                    return result

                html_content = await response.read()
        except Exception as e:
            msg = f"HTML fetch fail {url}: {e}"
            logger.warning(msg)
            result.errors.append(msg)
            return result

        return self._parse_html_links(
            html_content,
            url,
            blocklist,
            allowed_sections,
            url_pattern,
            is_ranked,
            ignore_hashes,
        )

    async def _fetch_from_html_browser(
        self,
        url: str,
        scroll_depth: int = 0,
        blocklist=None,
        allowed_sections=None,
        url_pattern=None,
        is_ranked=False,
        ignore_hashes=None,
    ) -> HarvestResult:
        """Heavy Fetch using Playwright."""
        result = HarvestResult()
        html_content = await BrowserFetcher.fetch(url, scroll_depth=scroll_depth)
        if not html_content:
            result.errors.append(f"Browser fetch returned empty for {url}")
            return result

        return self._parse_html_links(
            html_content,
            url,
            blocklist,
            allowed_sections,
            url_pattern,
            is_ranked,
            ignore_hashes,
        )

    def _parse_html_links(
        self,
        html_content,
        base_url,
        blocklist,
        allowed_sections,
        url_pattern,
        is_ranked,
        ignore_hashes=None,
    ) -> HarvestResult:
        """Shared logic to extract links from HTML."""
        result = HarvestResult()
        soup = BeautifulSoup(html_content, "html.parser")

        seen_urls = set()
        if not blocklist:
            blocklist = self.blocklist

        to_block = re.compile(blocklist, re.IGNORECASE)
        to_allow = re.compile(allowed_sections or ".*", re.IGNORECASE)

        rank_counter = 0
        links = soup.find_all("a", href=True)
        result.total_fetched = len(links)

        for tag in links:
            link_text = tag.get_text(strip=True)
            if not link_text or len(link_text) < 5:
                result.filtered_block += 1  # Treating noise as "block" for simplicity
                continue

            raw_url = str(tag["href"]).strip()
            full_url = urljoin(base_url, raw_url)

            if full_url in seen_urls:
                continue  # Internal de-dupe

            # 1. Blocklist Check
            if to_block.search(full_url) or not to_allow.search(full_url):
                result.filtered_block += 1
                continue

            # 2. Allowlist Check
            if url_pattern:
                if not re.search(url_pattern, full_url):
                    result.filtered_block += 1
                    continue

            # 3. Date Check
            url_date = None
            date_match = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2})", full_url)
            if date_match:
                date_str = date_match.group(1).replace("/", "-")
                if not self._is_date_recent(date_str):
                    result.filtered_date += 1
                    continue
                url_date = date_str

            # 4. Rank Assignment
            rank = None
            if is_ranked:
                rank_counter += 1
                rank = rank_counter

            # 5. Hash Check
            url_hash = self._compute_hash(full_url)
            if ignore_hashes and url_hash in ignore_hashes:
                result.filtered_hash += 1
                continue

            result.articles.append(
                {
                    "title": link_text,
                    "link": full_url,
                    "source": urlparse(full_url).netloc,
                    "published": url_date,
                    "content": "Unknown",
                    "rank": rank,
                    "hash": url_hash,
                }
            )
            seen_urls.add(full_url)

        return result

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        url: str,
        blocklist=None,
        allowed_sections=None,
        ignore_hashes=None,
        use_browser=False,
    ) -> HarvestResult:
        """Standard Sitemap Fetcher (XML)"""
        result = HarvestResult()
        xml_content = None

        if not use_browser:
            for agent in self.user_agents:
                try:
                    headers = self.headers.copy()
                    headers["User-Agent"] = agent

                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=120), headers=headers
                    ) as response:
                        if response.status == 200:
                            xml_content = await response.read()
                            break
                        elif response.status == 403:
                            logger.warning(
                                f"403 Forbidden on {url} with UA {agent[:20]}... Rotating."
                            )
                            continue
                        else:
                            msg = f"Response Error {url}: {response.status}"
                            logger.error(msg)
                            result.errors.append(msg)
                            return result
                except Exception as e:
                    logger.warning(f"Sitemap fail {url} with UA {agent[:20]}: {e}")
                    continue

        if not xml_content:
            logger.warning(" Attempting Browser...")
            xml_content = await self._fetch_text_browser(url)
            if not xml_content:
                msg = f"❌ All User-Agents AND Browser Fallback failed for {url}"
                logger.error(msg)
                result.errors.append(msg)
                return result

        soup = BeautifulSoup(xml_content, "xml")
        urls = soup.find_all("url")
        result.total_fetched = len(urls)

        base_domain = urlparse(url).netloc
        if not blocklist:
            blocklist = self.blocklist
        to_block = re.compile(blocklist, re.IGNORECASE)
        to_allow = re.compile(allowed_sections or ".*", re.IGNORECASE)

        for u in urls:
            loc = u.find("loc")
            if not loc:
                continue
            link = loc.text.strip()

            # Hash Check
            url_hash = self._compute_hash(link)
            if ignore_hashes and url_hash in ignore_hashes:
                result.filtered_hash += 1
                continue

            # Blocklist / Allowlist
            if to_block.search(link):
                result.filtered_block += 1
                continue

            if allowed_sections and not to_allow.search(link):
                result.filtered_block += 1
                continue

            result.articles.append(
                {
                    "title": "Unknown",
                    "link": link,
                    "source": base_domain,
                    "published": None,
                    "content": "Unknown",
                    "rank": None,
                    "hash": url_hash,
                }
            )

        return result

    async def harvest_rss(
        self,
        session: aiohttp.ClientSession,
        url,
        blocklist=None,
        allowed_sections=None,
        ignore_hashes=None,
    ) -> HarvestResult:
        """RSS Feed Fetcher (XML)"""
        result = HarvestResult()
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=60), headers=self.headers
            ) as response:
                if response.status != 200:
                    msg = f"RSS Error {url}: {response.status}"
                    logger.error(msg)
                    result.errors.append(msg)
                    return result
                xml_content = await response.read()
        except Exception as e:
            msg = f"RSS fail {url}: {e}"
            logger.warning(msg)
            result.errors.append(msg)
            return result

        soup = BeautifulSoup(xml_content, "xml")
        items = soup.find_all(["item", "entry"])
        result.total_fetched = len(items)

        base_domain = urlparse(url).netloc
        if not blocklist:
            blocklist = self.blocklist
        to_block = re.compile(blocklist, re.IGNORECASE)
        to_allow = re.compile(allowed_sections or ".*", re.IGNORECASE)

        for item in items:
            link_tag = item.find("link")
            if not link_tag:
                continue

            if link_tag.get("href"):
                link = str(link_tag["href"]).strip()
            else:
                link = link_tag.text.strip()

            # Hash Check
            url_hash = self._compute_hash(link)
            if ignore_hashes and url_hash in ignore_hashes:
                result.filtered_hash += 1
                continue

            # Blocklist / Allowlist
            if to_block.search(link):
                result.filtered_block += 1
                continue

            if allowed_sections and not to_allow.search(link):
                result.filtered_block += 1
                continue

            title_obj = item.find("title")
            title = title_obj.text.strip() if title_obj else "Unknown"

            date_tag = item.find(["pubDate", "published", "dc:date"])
            pub_date_str = date_tag.text.strip() if date_tag else None

            if pub_date_str and not self._is_date_recent(pub_date_str):
                result.filtered_date += 1
                continue

            content_encoded = item.find("content:encoded")
            description = item.find("description")
            content = (
                content_encoded.text.strip()
                if content_encoded
                else (description.text.strip() if description else "Unknown")
            )

            result.articles.append(
                {
                    "title": title,
                    "link": link,
                    "source": base_domain,
                    "published": pub_date_str,
                    "content": content,
                    "rank": None,
                    "hash": url_hash,
                }
            )

        return result

    async def harvest_latest_id(
        self,
        session,
        url,
        id_pattern,
        blocklist=None,
        allowed_sections=None,
        ignore_hashes=None,
    ) -> HarvestResult:
        result = HarvestResult()
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    result.errors.append(f"Index fetch error {url}: {response.status}")
                    return result
                xml_content = await response.read()
        except Exception as e:
            msg = f"Sitemap Index fail {url}: {e}"
            logger.warning(msg)
            result.errors.append(msg)
            return result

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        id_re = re.compile(id_pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag:
                continue
            loc_url = loc_tag.text.strip()
            match = id_re.search(loc_url)
            if match:
                try:
                    sitemap_id = int(match.group(1) or 0)
                    valid_locs.append((sitemap_id, loc_url))
                except ValueError:
                    continue

        if not valid_locs:
            result.errors.append(f"No matching sitemaps found for pattern {id_pattern}")
            return result

        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, target_url = valid_locs[0]

        return await self._fetch(
            session, target_url, blocklist, allowed_sections, ignore_hashes
        )

    async def harvest_latest_date(
        self,
        session,
        url,
        date_pattern,
        blocklist=None,
        allowed_sections=None,
        ignore_hashes=None,
    ) -> HarvestResult:
        result = HarvestResult()
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    result.errors.append(f"Index fetch error {url}: {response.status}")
                    return result
                xml_content = await response.read()
        except Exception as e:
            msg = f"Sitemap Index fail {url}: {e}"
            logger.warning(msg)
            result.errors.append(msg)
            return result

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        date_pattern_re = re.compile(date_pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag:
                continue
            loc_url = loc_tag.text.strip()
            match = date_pattern_re.search(loc_url)
            if match:
                date_str = match.group(1)
                valid_locs.append((date_str, loc_url))

        if not valid_locs:
            result.errors.append(
                f"No matching sitemaps found for pattern {date_pattern}"
            )
            return result

        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, best_url = valid_locs[0]

        return await self._fetch(
            session, best_url, blocklist, allowed_sections, ignore_hashes
        )

    async def _fetch_text_browser(self, url: str) -> Optional[bytes | str]:
        """Fallback: Fetch raw content using Playwright."""
        return await BrowserFetcher.fetch(url, return_bytes=True)

    async def harvest_latest_date_id(
        self,
        session,
        url,
        pattern,
        blocklist=None,
        allowed_sections=None,
        ignore_hashes=None,
    ) -> HarvestResult:
        result = HarvestResult()
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    result.errors.append(f"Index fetch error {url}: {response.status}")
                    return result
                xml_content = await response.read()
        except Exception as e:
            msg = f"Sitemap Index fail {url}: {e}"
            logger.warning(msg)
            result.errors.append(msg)
            return result

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        pattern_re = re.compile(pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag:
                continue
            loc_url = loc_tag.text.strip()
            match = pattern_re.search(loc_url)
            if match:
                val = match.group(1)
                try:
                    parts = val.split("-")
                    if len(parts) == 2:
                        sort_key = (int(parts[1]), int(parts[0]))
                        valid_locs.append((sort_key, loc_url))
                except Exception:
                    continue

        if not valid_locs:
            result.errors.append(f"No matching sitemaps found for pattern {pattern}")
            return result

        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, best_url = valid_locs[0]

        return await self._fetch(
            session, best_url, blocklist, allowed_sections, ignore_hashes
        )

    def _compute_hash(self, url: str) -> str:
        return hashlib.md5(url.split("?")[0].encode()).hexdigest()

    def _is_date_recent(self, date_str: str) -> bool:
        if not date_str:
            return True
        try:
            dt = None
            try:
                dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
            except ValueError:
                try:
                    dt = parsedate_to_datetime(date_str)
                except Exception:
                    pass

            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < self.cutoff_date:
                    return False
        except Exception:
            return True
        return True
