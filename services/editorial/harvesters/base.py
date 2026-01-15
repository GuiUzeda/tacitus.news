import asyncio
import hashlib
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from urllib.parse import urlparse, urljoin
from email.utils import parsedate_to_datetime

import aiohttp
from bs4 import BeautifulSoup
from loguru import logger

from core.browser import BrowserFetcher


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

    async def harvest(self, session, sources: List[dict], ignore_hashes: set = set()) -> list[dict]:
        """
        Orchestrator: Iterates sources, fetches data, and handles deduplication/merging.
        DO NOT OVERRIDE THIS in subclasses unless you want to change deduplication logic.
        """
        unique_articles = {}

        for source in sources:
            # Delegate fetching to a method that subclasses can override
            try:
                new_articles = await self.fetch_feed_articles(session, source, ignore_hashes)
            except Exception as e:
                logger.error(f"Feed process failed {source.get('url')}: {e}")
                continue
            
            # Merge Logic (Rank vs Date vs Content)
            for art in new_articles:
                url = art["link"]
                if url in unique_articles:
                    self._merge_article_data(unique_articles[url], art)
                else:
                    unique_articles[url] = art

        return list(unique_articles.values())

    async def fetch_feed_articles(self, session, source: dict, ignore_hashes: set = set()) -> List[dict]:
        """
        Strategy Implementation: Fetches articles based on feed type.
        Subclasses should override THIS to add custom logic (e.g. Sitemap Index by Date).
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
                use_browser=use_browser
            )
        
        # 4. Built-in Dynamic Sitemaps (New!)
        elif feed_type == "sitemap_index_id":
             # Assumes url_pattern contains the ID regex
             return await self.harvest_latest_id(
                 session, url, id_pattern=source.get("url_pattern"), ignore_hashes=ignore_hashes
             )
             
        elif feed_type == "sitemap_index_date":
             # Assumes url_pattern contains the Date regex
             return await self.harvest_latest_date(
                 session, url, date_pattern=source.get("url_pattern"), ignore_hashes=ignore_hashes
             )
        
        elif feed_type == "sitemap_index_date_id":
             # Assumes url_pattern contains a composite regex like (\d+-\d{4})
             return await self.harvest_latest_date_id(
                 session, url, pattern=source.get("url_pattern"), ignore_hashes=ignore_hashes
             )

        return []

    def _merge_article_data(self, existing: dict, new: dict):
        """Merges metadata, prioritizing Rank and Date."""
        # Rank: Lower is better. None is worst.
        old_rank = existing.get("rank")
        new_rank = new.get("rank")

        if new_rank is not None:
            if old_rank is None or new_rank < old_rank:
                existing["rank"] = new_rank

        # Date: If missing, take new.
        if not existing.get("published") and new.get("published"):
            existing["published"] = new["published"]

        # Content Snippet
        if (not existing.get("content") or existing["content"] == "Unknown") and \
           (new.get("content") and new["content"] != "Unknown"):
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
    ) -> List[dict]:
        """
        Fast Static HTML Fetch using aiohttp.
        Suitable for sites like G1, CNN, Estadao.
        """
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30), headers=self.headers
            ) as response:
                if response.status != 200:
                    logger.error(f"Response Error {url}: {response.status}")
                    return []
                
                # Check Content-Type to avoid binary files
                ctype = response.headers.get("Content-Type", "").lower()
                if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
                     logger.warning(f"Skipping non-HTML seed {url}: {ctype}")
                     return []

                html_content = await response.read()
        except Exception as e:
            logger.warning(f"HTML fetch fail {url}: {e}")
            return []

        return self._parse_html_links(
            html_content, url, blocklist, allowed_sections, url_pattern, is_ranked, ignore_hashes
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
    ) -> List[dict]:
        """
        Heavy Fetch using Playwright.
        Supports JavaScript rendering and Infinite Scroll.
        Suitable for SPAs like Band.com.br.
        """
        html_content = await BrowserFetcher.fetch(url, scroll_depth=scroll_depth)
        if not html_content:
            return []
            
        return self._parse_html_links(
            html_content, url, blocklist, allowed_sections, url_pattern, is_ranked, ignore_hashes
        )

    def _parse_html_links(
        self, 
        html_content, 
        base_url, 
        blocklist, 
        allowed_sections, 
        url_pattern, 
        is_ranked,
        ignore_hashes=None
    ):
        """
        Shared logic to extract links from HTML content (Static or Browser).
        Applies Regex patterns, blocklists, and assigns Rank.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        
        seen_urls = set()
        if not blocklist: 
            blocklist = self.blocklist

        to_block = re.compile(blocklist, re.IGNORECASE)
        to_allow = re.compile(allowed_sections or ".*", re.IGNORECASE)
        
        articles = []
        
        rank_counter = 0
        for tag in soup.find_all("a", href=True):
            link_text = tag.get_text(strip=True)
            if not link_text or len(link_text) < 5: 
                # Skip icons, empty links, or very short text
                continue
            
            raw_url = str(tag["href"]).strip()
            
            # Normalize URL (Handle relative paths like /noticia/...)
            full_url = urljoin(base_url, raw_url)

            if full_url in seen_urls: 
                continue

            # 1. Blocklist Check
            if to_block.search(full_url) or not to_allow.search(full_url):
                continue

            # 2. Allowlist Check (The Newspaper-Specific Regex)
            if url_pattern:
                if not re.search(url_pattern, full_url):
                    continue

            # 3. Date Check (URL Heuristic)
            # Extract YYYY-MM-DD or YYYY/MM/DD
            url_date = None
            date_match = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2})", full_url)
            if date_match:
                date_str = date_match.group(1).replace("/", "-")
                if not self._is_date_recent(date_str):
                    continue
                url_date = date_str

            # 4. Rank Assignment
            rank = None
            if is_ranked:
                rank_counter += 1
                rank = rank_counter
            
            # 5. Hash Check (Optimization)
            url_hash = self._compute_hash(full_url)
            if ignore_hashes and url_hash in ignore_hashes:
                continue

            articles.append({
                "title": link_text,
                "link": full_url,
                "source": urlparse(full_url).netloc,
                "published": url_date, # Often None here; Trafilatura handles extraction later
                "content": "Unknown",
                "rank": rank, # CRITICAL: Used by Publisher for "Hot Score"
                "hash": url_hash
            })
            seen_urls.add(full_url)

        return articles

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        url: str,
        blocklist=None,
        allowed_sections=None,
        ignore_hashes=None,
        use_browser=False
    ) -> list[dict]:
        """ Standard Sitemap Fetcher (XML) """
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
                            logger.warning(f"403 Forbidden on {url} with UA {agent[:20]}... Rotating.")
                            continue
                        else:
                            logger.error(f"Response Error {url}: {response.status}")
                            return []
                except Exception as e:
                    logger.warning(f"Sitemap fail {url} with UA {agent[:20]}: {e}")
                    continue

        if not xml_content:
            logger.warning(f" Attempting Browser...")
            xml_content = await self._fetch_text_browser(url)
            if not xml_content:
                logger.error(f"❌ All User-Agents AND Browser Fallback failed for {url}")
                return []
            
        soup = BeautifulSoup(xml_content, "xml")
        articles = []

        urls = soup.find_all("url")
        base_domain = urlparse(url).netloc
        if not blocklist:
            blocklist = self.blocklist

        to_block = re.compile(blocklist, re.IGNORECASE)
        
        for u in urls:
            loc = u.find("loc")
            if not loc: continue
            link = loc.text.strip()

            # 0. Hash Check
            url_hash = self._compute_hash(link)
            if ignore_hashes and url_hash in ignore_hashes:
                continue

            # 1. Blocklist Check
            if to_block.search(link): continue

            # 2. Date Check (Google News Sitemap)
            news = u.find("news:news")
            lastmod = u.find("lastmod")
            pub_date = None
            if news:
                pdate = news.find("news:publication_date")
                if pdate: pub_date = pdate.text

            if pub_date and not self._is_date_recent(pub_date):
                continue
            
            
            articles.append({
                "title": "Unknown", # Sitemaps rarely have titles
                "link": link,
                "source": base_domain,
                "published": pub_date or (lastmod.text if lastmod else None),
                "content": "Unknown",
                "rank": None, # Sitemaps have no editorial rank
                "hash": url_hash
            })
            
        if not allowed_sections: return articles
        
        filtered = []
        to_allow = re.compile(allowed_sections, re.IGNORECASE)
        for art in articles:
            if to_allow.search(art["link"]):
                filtered.append(art)
        return filtered

    async def harvest_rss(
        self, session: aiohttp.ClientSession, url, blocklist=None, allowed_sections=None, ignore_hashes=None
    ) -> list[dict]:
        """ RSS Feed Fetcher (XML) """
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=60), headers=self.headers
            ) as response:
                if response.status != 200:
                    logger.error(f"RSS Error {url}: {response.status}")
                    return []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"RSS fail {url}: {e}")
            return []

        soup = BeautifulSoup(xml_content, "xml")
        articles = []
        items = soup.find_all(["item", "entry"])
        base_domain = urlparse(url).netloc
        
        if not blocklist: blocklist = self.blocklist
        to_block = re.compile(blocklist, re.IGNORECASE)

        for item in items:
            link_tag = item.find("link")
            if not link_tag: continue

            if link_tag.get("href"):
                link = str(link_tag["href"]).strip()
            else:
                link = link_tag.text.strip()

            # Hash Check
            url_hash = self._compute_hash(link)
            if ignore_hashes and url_hash in ignore_hashes:
                continue

            if to_block.search(link): continue

            title_obj = item.find("title")
            title = title_obj.text.strip() if title_obj else "Unknown"

            date_tag = item.find(["pubDate", "published", "dc:date"])
            pub_date_str = date_tag.text.strip() if date_tag else None

            if pub_date_str and not self._is_date_recent(pub_date_str):
                continue

            content_encoded = item.find("content:encoded")
            description = item.find("description")
            content = content_encoded.text.strip() if content_encoded else (description.text.strip() if description else "Unknown")

            articles.append({
                "title": title,
                "link": link,
                "source": base_domain,
                "published": pub_date_str,
                "content": content,
                "rank": None, # RSS is usually reverse chronological, not editorial rank
                "hash": url_hash
            })

        if not allowed_sections: return articles

        filtered = []
        to_allow = re.compile(allowed_sections, re.IGNORECASE)
        for art in articles:
            if to_allow.search(art["link"]):
                filtered.append(art)
        return filtered

    async def harvest_latest_id(self, session, url, id_pattern, blocklist=None, allowed_sections=None, ignore_hashes=None):
        """ 
        Fetch sitemap index by numeric ID.
        Used for sites that rotate sitemaps by ID (e.g. sitemap-1234.xml).
        """
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200: return []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"Sitemap Index fail {url}: {e}")
            return []

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        id_re = re.compile(id_pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag: continue
            loc_url = loc_tag.text.strip()
            match = id_re.search(loc_url)
            if match:
                try:
                    sitemap_id = int(match.group(1) or 0)
                    valid_locs.append((sitemap_id, loc_url))
                except ValueError: continue

        if not valid_locs: return []
        
        # Sort by ID descending (Highest number = Latest)
        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, target_url = valid_locs[0]
        
        return await self._fetch(session, target_url, blocklist, allowed_sections, ignore_hashes)

    async def harvest_latest_date(self, session, url, date_pattern, blocklist=None, allowed_sections=None, ignore_hashes=None):
        """ 
        Fetch sitemap index by Date string.
        Used for sites that rotate sitemaps by Date (e.g. sitemap-2026-01-12.xml).
        """
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200: return []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"Sitemap Index fail {url}: {e}")
            return []

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        date_pattern_re = re.compile(date_pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag: continue
            loc_url = loc_tag.text.strip()
            match = date_pattern_re.search(loc_url)
            if match:
                date_str = match.group(1)
                valid_locs.append((date_str, loc_url))

        if not valid_locs: return []
        
        # Sort Descending (2026-01-06 > 2026-01-05)
        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, best_url = valid_locs[0]

        return await self._fetch(session, best_url, blocklist, allowed_sections, ignore_hashes)

    async def _fetch_text_browser(self, url: str) -> Optional[bytes|str]:
        """Fallback: Fetch raw content using Playwright."""
        return await BrowserFetcher.fetch(url, return_bytes=True)

    async def harvest_latest_date_id(self, session, url, pattern, blocklist=None, allowed_sections=None, ignore_hashes=None):
        """ 
        Fetch sitemap index with composite ID (e.g. 10-2024).
        Splits by '-' and sorts by (Year, ID) descending.
        """
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200: return []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"Sitemap Index fail {url}: {e}")
            return []

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        pattern_re = re.compile(pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag: continue
            loc_url = loc_tag.text.strip()
            match = pattern_re.search(loc_url)
            if match:
                val = match.group(1) # e.g. "10-2024"
                try:
                    # Handle "10-2024" -> sort by (2024, 10)
                    parts = val.split('-')
                    if len(parts) == 2:
                        sort_key = (int(parts[1]), int(parts[0]))
                        valid_locs.append((sort_key, loc_url))
                except: continue

        if not valid_locs: return []
        
        # Sort Descending by Year then ID
        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, best_url = valid_locs[0]

        return await self._fetch(session, best_url, blocklist, allowed_sections, ignore_hashes)

    def _compute_hash(self, url: str) -> str:
        return hashlib.md5(url.split("?")[0].encode()).hexdigest()

    def _is_date_recent(self, date_str: str) -> bool:
        """Parses date and checks if it is within the cutoff window."""
        if not date_str: return True
        try:
            dt = None
            # 1. Try ISO 8601 (Sitemaps, Atom)
            try:
                dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
            except ValueError:
                # 2. Try RFC 822 (RSS)
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
            # If we can't parse, we assume it's recent/valid to be safe
            return True
        return True