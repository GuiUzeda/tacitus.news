from typing import List

from app.config import Settings
from app.core.llm_parser import CloudNewsFilter
from loguru import logger

# Models


class NewsFilterDomain:
    def __init__(self):
        self.settings = Settings()
        # Initialize the LLM Service (from app.core)
        self.llm_filter = CloudNewsFilter()

    async def filter_batch(self, titles: List[str]) -> List[bool]:
        """
        Pure Logic: Takes a list of titles and determines which are relevant.
        Returns a list of booleans matching the input order.
        True = Keep (Relevant)
        False = Discard (Irrelevant/Spam)
        """
        if not titles:
            return []

        logger.info(f"🧠 Filtering batch of {len(titles)} titles...")

        try:
            # The LLM returns the INDICES of the approved articles
            approved_indices = await self.llm_filter.filter_batch(titles)

            # Map indices back to a boolean mask [True, False, True...]
            results = []
            for i in range(len(titles)):
                results.append(i in approved_indices)

            return results

        except Exception as e:

            logger.opt(exception=True).error(f"LLM Filter Error: {e}")
            # Fail-safe: If LLM crashes, we shouldn't drop everything silently.
            # Reraise so the worker can retry the batch later.
            raise e
