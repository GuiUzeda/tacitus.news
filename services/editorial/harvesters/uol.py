# import re
# from datetime import datetime
# from .base import BaseHarvester
# from loguru import logger

# class UOLHarvester(BaseHarvester):
#     def __init__(self):
#         super().__init__()
#         # Custom "Allowlist" to filter out local noise
#         self.allowed_sections = re.compile(r'(/newsletters/|/economia/|/colunas/|/internacional/|/cotidiano/)')

#     async def harvest(self, session, urls=[]):
#         """
#         We IGNORE the url passed in the DB and construct the dynamic one.
#         """
        
#         target_url = f"https://noticias.uol.com.br/sitemap/v2/news-01.xml"
        
#         logger.info(f"Using Custom UOL Logic -> {target_url}")
        
#         # Reuse the base logic to parse the XML, but we wrap it to apply our filters
#         raw_articles = await self._fetch(session, target_url)
        
#         # Apply G1-Specific Filter (The "Bouncer")
#         filtered = []
#         for art in raw_articles:
#             # Check if URL contains one of our allowed sections
#             if self.allowed_sections.search(art['link']):
#                 filtered.append(art)
                
#         return filtered