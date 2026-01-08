import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from urllib.parse import urlparse
import aiohttp
from bs4 import BeautifulSoup
from loguru import logger


class BaseHarvester:
    def __init__(
        self,
        cutoff: timedelta = timedelta(hours=24),
    ):
        # Universal Garbage Filter
        # self.blocklist = re.compile(
        #     blocklist,
        #     re.IGNORECASE,
        # )

        self.blocklist: str = (
            r"(login|search|tag|gallery|/esporte/|futebol|bbb|horoscopo|gastronomia|patrocinado|publicidade|quiz|ilustrada|podcast|web-stories|/live/)"
        )
        # Only accept news from the last 24h (Crucial for Indexes)
        self.cutoff_date = datetime.now(timezone.utc) - cutoff
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
        }

    async def harvest(
        self, session, sources:List[dict]
    ) -> list[dict]:
        """
        Main entry point. Can be overridden by subclasses.
        """
        articles =[]
        for source in sources:
            articles.extend(await self._fetch(session, source['url'], source['blocklist'], source['allowed_sections']))
        return articles

    async def harvest_latest_id(
        self, session, url, id_pattern, blocklist=None, allowed_sections=None
    ):
        # 1. Fetch the Index
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    return []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"Sitemap Index fail {url}: {e}")
            return []

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        id_re = re.compile(id_pattern)

        # 2. Extract and Sort
        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag:
                continue

            loc_url = loc_tag.text.strip()
            match = id_re.search(loc_url)

            if match:
                # We store a tuple: (ID_NUMBER, FULL_URL)
                # We use match.group(1) to get the digits inside (\d*)
                try:
                    sitemap_id = int(match.group(1))
                    valid_locs.append((sitemap_id, loc_url))
                except ValueError:
                    continue

        if not valid_locs:
            logger.warning(f"No matching sitemaps found for pattern {id_pattern}")
            return []

        # Sort by ID descending (Highest number = Latest)
        valid_locs.sort(key=lambda x: x[0], reverse=True)

        best_id, target_url = valid_locs[0]

        logger.info(f"📍 Found latest sitemap: {target_url} (ID: {best_id})")

        # 3. FIX: Fetch the TARGET URL, not the original Index URL
        return await self._fetch(session, target_url, blocklist, allowed_sections)

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        url: str,
        blocklist=None,
        allowed_sections=None,
    ) -> list[dict]:
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

        to_block = re.compile(
            blocklist,
            re.IGNORECASE,
        )
        for u in urls:
            loc = u.find("loc")
            if not loc:
                continue
            link = loc.text.strip()

            # 1. Blocklist Check
            if to_block.search(link):
                continue

            # 2. Date Check (If available)
            # Google News Sitemaps use <news:publication_date>
            news = u.find("news:news")
            lastmod = u.find("lastmod")
            pub_date = None
            if news:
                pdate = news.find("news:publication_date")
                if pdate:
                    pub_date = pdate.text

            if pub_date:
                # Basic check: is it today? (You can add stricter parsing if needed)
                if str(datetime.now().year) not in pub_date:
                    continue

            articles.append(
                {
                    "title": "Unknown",  # Trafilatura will fix this later
                    "link": link,
                    "source": base_domain,
                    "published": pub_date or lastmod.text if lastmod else None,
                    "content": "Unknown",  # Trafilatura will fix this later
                }
            )
        if not allowed_sections:
            return articles
        filtered = []
        to_allow = re.compile(
            allowed_sections,
            re.IGNORECASE,
        )
        for art in articles:
            # Check if URL contains one of our allowed sections
            if to_allow.search(art["link"]):
                filtered.append(art)
        return filtered

    async def harvest_rss(
        self, session: aiohttp.ClientSession, url, blocklist=None, allowed_sections=None
    ) -> list[dict]:
        """
        specialized fetcher for RSS feeds (xml)
        """
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=120), headers=self.headers
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

        # RSS 2.0 uses <item>, Atom uses <entry> (covering both bases)
        items = soup.find_all(["item", "entry"])

        base_domain = urlparse(url).netloc
        if not blocklist:
            blocklist = self.blocklist

        to_block = re.compile(blocklist, re.IGNORECASE)

        for item in items:
            # 1. Extract Link
            # RSS: <link>http...</link>
            # Atom: <link href="http..." />
            link_tag = item.find("link")
            if not link_tag:
                continue

            # Handle Atom style <link href=... /> vs RSS style <link>text</link>
            if link_tag.get("href"):
                link = str(link_tag["href"]).strip()
            else:
                link = link_tag.text.strip()

            # 2. Blocklist Check
            if to_block.search(link):
                continue

            # 3. Extract Meta (Title, Date, Desc)
            title_obj = item.find("title")
            title = title_obj.text.strip() if title_obj else "Unknown"

            # Dates in RSS are messy (RFC-822 vs ISO-8601).
            # We try standard tags: pubDate (RSS) or updated/published (Atom)
            date_tag = item.find(["pubDate", "published", "dc:date"])
            pub_date_str = date_tag.text.strip() if date_tag else None

            # 4. Naive Date Filter (matches your sitemap logic)
            # If you want stricter parsing, we can use email.utils.parsedate_to_datetime
            if pub_date_str:
                if str(datetime.now().year) not in pub_date_str:
                    continue

            # 5. Extract Content/Description (RSS usually has this, unlike Sitemaps)
            # We look for 'content:encoded' (often used for full text) or 'description'
            content_encoded = item.find("content:encoded")
            description = item.find("description")

            if content_encoded:
                content = content_encoded.text.strip()
            elif description:
                content = description.text.strip()
            else:
                content = "Unknown"

            articles.append(
                {
                    "title": title,  # RSS actually gives us the title!
                    "link": link,
                    "source": base_domain,
                    "published": pub_date_str,
                    "content": content,  # RSS often gives a summary or full text
                }
            )

        # 6. Filter by Allowed Sections (if provided)
        if not allowed_sections:
            return articles

        filtered = []
        to_allow = re.compile(allowed_sections, re.IGNORECASE)

        for art in articles:
            if to_allow.search(art["link"]):
                filtered.append(art)

        return filtered

    async def harvest_latest_date(self, session, url , date_pattern,blocklist=None, allowed_sections=None):
        """
        Fetches the Metropoles sitemap index and finds the URL for the latest date.
        Pattern: .../noticias-YYYY-MM-DD.xml
        """
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    logger.warning(f"Metropoles Index fail {url}: {response.status}")
                    return  []
                xml_content = await response.read()
        except Exception as e:
            logger.warning(f"Metropoles Connection fail {url}: {e}")
            return []

        soup = BeautifulSoup(xml_content, "xml")
        maps = soup.find_all("sitemap")
        

        date_pattern = re.compile(date_pattern)

        valid_locs = []
        for m in maps:
            loc_tag = m.find("loc")
            if not loc_tag:
                continue

            loc_url = loc_tag.text.strip()
            match = date_pattern.search(loc_url)

            if match:
                date_str = match.group(1)
                # We store (DATE_STRING, FULL_URL)
                # ISO Format (YYYY-MM-DD) sorts correctly as a string, no need to parse to datetime object
                valid_locs.append((date_str, loc_url))

        if not valid_locs:
            logger.warning(f"No daily sitemaps found in {url}")
            return []

        # Sort Descending (2026-01-06 > 2026-01-05)
        valid_locs.sort(key=lambda x: x[0], reverse=True)
        
        latest_date, best_url = valid_locs[0]
        logger.info(f"📍 Latest sitemap: {latest_date} -> {best_url}")
        
        return await self._fetch(session, best_url, blocklist, allowed_sections)