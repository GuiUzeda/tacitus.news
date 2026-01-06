# import re
# from datetime import datetime
# from .base import BaseHarvester
# from loguru import logger

# class G1Harvester(BaseHarvester):
#     def __init__(self):
#         super().__init__()
#         # Custom "Allowlist" to filter out local noise
#         self.allowed_sections = re.compile(r'(/politica/|/economia/|/mundo/)')

#     async def harvest(self, session, urls=[]):
#         """
#         We IGNORE the url passed in the DB and construct the dynamic one.
#         """
#         today = datetime.now()
#         # Construct: https://g1.globo.com/sitemap/g1/YYYY/MM/DD_1.xml
#         target_url = f"https://g1.globo.com/sitemap/g1/news.xml"
        
#         logger.info(f"Using Custom G1 Logic -> {target_url}")
        
#         # Reuse the base logic to parse the XML, but we wrap it to apply our filters
#         raw_articles = await self._fetch(session, target_url)
        
#         # Apply G1-Specific Filter (The "Bouncer")
#         filtered = []
#         for art in raw_articles:
#             # Check if URL contains one of our allowed sections
#             if self.allowed_sections.search(art['link']):
#                 filtered.append(art)
                
#         return filtered