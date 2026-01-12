import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from urllib.parse import urlparse, urljoin

import aiohttp
from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import async_playwright


class BaseHarvester:
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=24),
    ):
        # Universal Garbage Filter (Applied to all links by default)
        self.blocklist: str = (
            r"(login|search|tag|gallery|/esporte/|futebol|bbb|horoscopo|gastronomia|"
            r"patrocinado|publicidade|quiz|ilustrada|podcast|web-stories|/live/)"
        )
        # Only accept news from the last 24h (Crucial for Indexes)
        self.cutoff_date = datetime.now(timezone.utc) - cutoff
        
        # Standard Headers to mimic a real browser
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }

    async def harvest(self, session, sources: List[dict]) -> list[dict]:
        """
        Main entry point. Dispatches to specific fetchers based on feed_type.
        """
        articles = []
        for source in sources:
            new_articles = []
            feed_type = source.get("feed_type", "sitemap")
            use_browser = source.get("use_browser_render", False)
            is_ranked = source.get("is_ranked", False)
            
            # 1. HTML Scraping (The new Rank-aware logic)
            if feed_type == "html":
                if use_browser:
                    new_articles = await self._fetch_from_html_browser(
                        source["url"],
                        scroll_depth=source.get("scroll_depth", 0),
                        blocklist=source.get("blocklist"),
                        allowed_sections=source.get("allowed_sections"),
                        url_pattern=source.get("url_pattern"),
                        is_ranked=is_ranked,
                    )
                else:
                    new_articles = await self._fetch_from_html(
                        session,
                        source["url"],
                        blocklist=source.get("blocklist"),
                        allowed_sections=source.get("allowed_sections"),
                        url_pattern=source.get("url_pattern"),
                        is_ranked=is_ranked,
                    )
            
            # 2. RSS (Legacy)
            elif feed_type == "rss":
                new_articles = await self.harvest_rss(
                    session,
                    source["url"],
                    blocklist=source.get("blocklist"),
                    allowed_sections=source.get("allowed_sections"),
                )

            # 3. Sitemap (Legacy)
            elif feed_type == "sitemap":
                new_articles = await self._fetch(
                    session,
                    source["url"],
                    blocklist=source.get("blocklist"),
                    allowed_sections=source.get("allowed_sections"),
                )
                
            articles.extend(new_articles)
        return articles

    async def _fetch_from_html(
        self,
        session,
        url: str,
        blocklist=None,
        allowed_sections=None,
        url_pattern=None,
        is_ranked=False,
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
                html_content = await response.read()
        except Exception as e:
            logger.warning(f"HTML fetch fail {url}: {e}")
            return []

        return self._parse_html_links(
            html_content, url, blocklist, allowed_sections, url_pattern, is_ranked
        )

    async def _fetch_from_html_browser(
        self,
        url: str,
        scroll_depth: int = 0,
        blocklist=None,
        allowed_sections=None,
        url_pattern=None,
        is_ranked=False,
    ) -> List[dict]:
        """
        Heavy Fetch using Playwright.
        Supports JavaScript rendering and Infinite Scroll.
        Suitable for SPAs like Band.com.br.
        """
        logger.info(f"🎭 Browser Fetching: {url} (Scrolls: {scroll_depth})")
        
        async with async_playwright() as p:
            # Launch without sandbox for Docker compatibility
            # Ensure 'playwright install chromium' has been run in your Dockerfile
            browser = await p.chromium.launch(
                headless=True, 
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            
            context = await browser.new_context(
                user_agent=self.headers["User-Agent"],
                viewport={"width": 1280, "height": 800}
            )
            page = await context.new_page()

            try:
                # 1. Load Page
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                await page.wait_for_timeout(2000) # Settle time

                # 2. Close potential popups/overlays
                try: 
                    await page.keyboard.press("Escape")
                except: 
                    pass

                # 3. Robust Interaction Loop (Scroll / Click Load More)
                for i in range(scroll_depth):
                    # Strategy A: Look for "Load More" buttons
                    try:
                        load_btn = page.get_by_role(
                            "button", 
                            name=re.compile(r"carregar mais|ver mais|leia mais|veja mais", re.IGNORECASE)
                        )
                        if await load_btn.count() > 0 and await load_btn.first.is_visible():
                            await load_btn.first.click(force=True)
                            await page.wait_for_timeout(3000) # Give it time to load content
                            continue 
                    except: 
                        pass
                    
                    # Strategy B: Keyboard Navigation (Works on overflow divs)
                    await page.keyboard.press("End")
                    await page.wait_for_timeout(500)
                    
                    # Strategy C: Mouse Wheel (Simulates human scroll)
                    await page.mouse.wheel(0, 15000)
                    await page.wait_for_timeout(2000)

                html_content = await page.content()

            except Exception as e:
                logger.error(f"Browser Crash {url}: {e}")
                await browser.close()
                return []

            await browser.close()
            
            return self._parse_html_links(
                html_content, url, blocklist, allowed_sections, url_pattern, is_ranked
            )

    def _parse_html_links(
        self, 
        html_content, 
        base_url, 
        blocklist, 
        allowed_sections, 
        url_pattern, 
        is_ranked
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

            # 3. Rank Assignment
            # If is_ranked=True, we trust the order of appearance on the homepage.
            rank = None
            if is_ranked:
                rank = len(articles) + 1
            
            # Attempt to extract date from URL (heuristic)
            url_date = re.match(r"\d{4}-\d{2}-\d{2}|\d{4}/\d{2}/\d{2}", full_url)

            articles.append({
                "title": link_text,
                "link": full_url,
                "source": urlparse(full_url).netloc,
                "published": url_date, # Often None here; Trafilatura handles extraction later
                "content": "Unknown",
                "rank": rank, # CRITICAL: Used by Publisher for "Hot Score"
            })
            seen_urls.add(full_url)

        return articles

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        url: str,
        blocklist=None,
        allowed_sections=None,
    ) -> list[dict]:
        """ Standard Sitemap Fetcher (XML) """
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=120), headers=self.headers
            ) as response:
                if response.status != 200:
                    logger.error(f"Response Error {url}: {response.status}")
                    return []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"Sitemap fail {url}: {e}")
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

            # 1. Blocklist Check
            if to_block.search(link): continue

            # 2. Date Check (Google News Sitemap)
            news = u.find("news:news")
            lastmod = u.find("lastmod")
            pub_date = None
            if news:
                pdate = news.find("news:publication_date")
                if pdate: pub_date = pdate.text

            if pub_date:
                # Basic check: is it from the current year?
                if str(datetime.now().year) not in pub_date:
                    continue

            articles.append({
                "title": "Unknown", # Sitemaps rarely have titles
                "link": link,
                "source": base_domain,
                "published": pub_date or (lastmod.text if lastmod else None),
                "content": "Unknown",
                "rank": None # Sitemaps have no editorial rank
            })
            
        if not allowed_sections: return articles
        
        filtered = []
        to_allow = re.compile(allowed_sections, re.IGNORECASE)
        for art in articles:
            if to_allow.search(art["link"]):
                filtered.append(art)
        return filtered

    async def harvest_rss(
        self, session: aiohttp.ClientSession, url, blocklist=None, allowed_sections=None
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

            if to_block.search(link): continue

            title_obj = item.find("title")
            title = title_obj.text.strip() if title_obj else "Unknown"

            date_tag = item.find(["pubDate", "published", "dc:date"])
            pub_date_str = date_tag.text.strip() if date_tag else None

            if pub_date_str and str(datetime.now().year) not in pub_date_str:
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
                "rank": None # RSS is usually reverse chronological, not editorial rank
            })

        if not allowed_sections: return articles

        filtered = []
        to_allow = re.compile(allowed_sections, re.IGNORECASE)
        for art in articles:
            if to_allow.search(art["link"]):
                filtered.append(art)
        return filtered

    async def harvest_latest_id(self, session, url, id_pattern, blocklist=None, allowed_sections=None):
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
                    sitemap_id = int(match.group(1))
                    valid_locs.append((sitemap_id, loc_url))
                except ValueError: continue

        if not valid_locs: return []
        
        # Sort by ID descending (Highest number = Latest)
        valid_locs.sort(key=lambda x: x[0], reverse=True)
        _, target_url = valid_locs[0]
        
        return await self._fetch(session, target_url, blocklist, allowed_sections)

    async def harvest_latest_date(self, session, url, date_pattern, blocklist=None, allowed_sections=None):
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

        return await self._fetch(session, best_url, blocklist, allowed_sections)