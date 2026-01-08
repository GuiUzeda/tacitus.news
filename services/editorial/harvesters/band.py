import re
import aiohttp
from datetime import datetime
from loguru import logger
from .base import BaseHarvester

class BandHarvester(BaseHarvester):
    def __init__(self):
        super().__init__()

    async def harvest(self, session: aiohttp.ClientSession, sources: list[dict]) -> list[dict]:
        articles = []
        for source in sources:
            url = source["url"]
            
            # Prepare Blocklist Regex
            # Use source-specific blocklist or fallback to default
            blocklist_pattern = source.get("blocklist")
            if not blocklist_pattern:
                blocklist_pattern = self.blocklist
            
            try:
                block_re = re.compile(blocklist_pattern, re.IGNORECASE)
            except re.error:
                logger.warning(f"Invalid regex for Band: {blocklist_pattern}")
                block_re = re.compile(self.blocklist, re.IGNORECASE)

            try:
                # Band API returns JSON
                async with session.get(url, timeout=10, headers=self.headers) as response:
                    if response.status != 200:
                        logger.error(f"Band API Error {response.status}: {url}")
                        continue
                    data = await response.json()
            except Exception as e:
                logger.error(f"Band harvest failed {url}: {e}")
                continue

            items = data.get("items", [])
            if not items:
                logger.warning(f"No items found in Band API response: {url}")
                continue

            for item in items:
                try:
                    # 1. Extract Title
                    # Structure: item -> config -> order -> data -> title
                    # Some items might have it in 'data' directly or different structure, 
                    # but based on sample, it's inside config.order.data
                    config = item.get("config", {})
                    order = config.get("order", {})
                    data_obj = order.get("data", {})
                    title = data_obj.get("title")
                    
                    if not title:
                        continue

                    # 2. Extract Link
                    # Prefer 'linkExterno' (e.g. agromais domains), fallback to constructed 'url'
                    link = item.get("linkExterno")
                    if not link:
                        relative_url = item.get("url")
                        if relative_url:
                            # Normalize slash
                            if relative_url.startswith("/"):
                                link = f"https://www.band.com.br{relative_url}"
                            else:
                                link = f"https://www.band.com.br/{relative_url}"
                    
                    if not link:
                        continue

                    # 3. Blocklist Check
                    if block_re.search(link):
                        continue

                    # 4. Date Check
                    pub_date_str = item.get("createdAt")
                    if pub_date_str:
                        # Parse ISO format: 2026-01-06T21:40:03.000Z
                        try:
                            # Python 3.11+ handles Z, older needs replacement
                            dt = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                            if dt < self.cutoff_date:
                                continue
                        except ValueError:
                            pass # Ignore date parsing errors, keep article

                    articles.append({
                        "title": title,
                        "link": link,
                        "source": "band.com.br",
                        "published": pub_date_str,
                        "content": "" # Handled by NewsGetter/Trafilatura later
                    })

                except Exception as e:
                    # Don't crash the loop for one bad item
                    continue
        
        return articles