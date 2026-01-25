import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from app.core.browser import BrowserFetcher
from app.core.llm_parser import CloudNewsAnalyzer
from app.services.base_service import BaseService
from app.services.queue_service import QueueService
from app.workers.enricher.domain import ContentEnricherDomain
from bs4 import BeautifulSoup
from news_events_lib.models import (
    ArticleModel,
    ArticlesQueueModel,
    ArticlesQueueName,
    JobStatus,
)

logger = logging.getLogger(__name__)


class ArticleService(BaseService, QueueService):
    """
    Service for the Article domain.
    Domain Model: ArticleModel
    Queue Model: ArticlesQueueModel
    """

    def search(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        return self._search(ArticleModel, filters, limit, offset)

    def list_queue(
        self, filters: Dict[str, Any], limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        # Handle queue_name mapping if present in filters
        if "queue_name" in filters and isinstance(filters["queue_name"], str):
            for q in ArticlesQueueName:
                if q.value.lower() == filters["queue_name"].lower():
                    filters["queue_name"] = q
                    break
        return self._list_queue(ArticlesQueueModel, filters, limit, offset)

    def retry_queue(self, queue_name: Optional[str] = None) -> int:
        q_enum = None
        if queue_name:
            for q in ArticlesQueueName:
                if q.value.lower() == queue_name.lower():
                    q_enum = q
                    break
        return self._retry_failed(ArticlesQueueModel, q_enum)

    def requeue_article_for_analysis(self, article_id: str) -> str:
        """
        Re-queues a single article for Enrichment/Analysis.
        """
        import uuid

        from sqlalchemy import select

        try:
            uid = uuid.UUID(article_id)
        except ValueError:
            return "❌ Invalid UUID"

        article = self.session.get(ArticleModel, uid)
        if not article:
            return "❌ Article not found."

        now = datetime.now(timezone.utc)
        article.summary_status = JobStatus.PENDING

        # Check for existing pending/processing job
        stmt = select(ArticlesQueueModel).where(
            ArticlesQueueModel.article_id == article.id,
            ArticlesQueueModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
        )
        existing_job = self.session.scalar(stmt)

        if existing_job:
            existing_job.status = JobStatus.PENDING
            existing_job.attempts = 0
            existing_job.updated_at = now
            existing_job.msg = "Manual Re-Analyze"
            existing_job.queue_name = ArticlesQueueName.ENRICHER
        else:
            new_job = ArticlesQueueModel(
                article_id=article.id,
                status=JobStatus.PENDING,
                queue_name=ArticlesQueueName.ENRICHER,
                created_at=now,
                updated_at=now,
                attempts=0,
                msg="Manual Re-Analyze",
            )
            self.session.add(new_job)

        self.session.commit()
        return f"✅ Article '{article.title[:20]}...' queued for Analysis."

    def get_article_details(self, article_id: str) -> Optional[Dict[str, Any]]:
        """Returns full details of an article including content."""
        import uuid

        try:
            uid = uuid.UUID(article_id)
        except ValueError:
            return None

        article = self.session.get(ArticleModel, uid)
        if not article:
            return None

        # Determine content source (ArticleContent table or legacy)
        content_text = ""
        # Assuming ArticleModel has a relationship 'contents' or field 'content'
        # Turn 1 context suggests 'contents' relationship to ArticleContentModel or just 'content' field in some places?
        # Let's check ArticleModel definition? No, I don't have it open.
        # But 'test_enrichment.py' used: article.contents[0].content
        # 'read_article_content' in 'cli_common.py' used: fresh_article.content
        # Let's try direct access and fallback.

        if hasattr(article, "content") and article.content:
            content_text = article.content
        elif hasattr(article, "contents") and article.contents:
            # Join content parts
            content_text = "\n\n".join(
                [c.content for c in article.contents if c.content]
            )

        return {
            "id": str(article.id),
            "title": article.title,
            "source": article.newspaper.name if article.newspaper else "Unknown",
            "url": article.original_url,
            "published_date": (
                article.published_date.isoformat() if article.published_date else None
            ),
            "summary": str(article.summary) if article.summary else None,
            "content": content_text,
            "entities": article.entities,
            "stance": article.stance,
            "stance_reasoning": article.stance_reasoning,
            "clickbait_score": article.clickbait_score,
            "clickbait_reasoning": article.clickbait_reasoning,
            "analysis": article.key_points,
        }

    async def refetch_article(self, article_id: str) -> str:
        """
        Refetches HTML for a specific article and updates its published date using Regex/LLM.
        """
        import uuid

        try:
            uid = uuid.UUID(article_id)
        except ValueError:
            return "❌ Invalid UUID"

        article = self.session.get(ArticleModel, uid)
        if not article:
            return "❌ Article not found."

        if not article.original_url:
            return "❌ No URL to fetch."

        # Fetch Logic
        html = None
        use_browser = False
        try:
            headers = {
                "User-Agent": BrowserFetcher.USER_AGENTS[0],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    article.original_url, headers=headers, timeout=15
                ) as response:
                    if response.status == 200:
                        html = await response.text(errors="replace")
                        if not html or len(html) < 500:
                            use_browser = True
                    else:
                        use_browser = True
        except Exception as e:
            logger.warning(f"Fetch failed: {e}")
            use_browser = True

        if use_browser:
            try:
                html = await BrowserFetcher.fetch(article.original_url)
            except Exception as e:
                return f"❌ Browser Fetch Failed: {e}"

        if not html:
            return "❌ Failed to retrieve HTML."

        # Extract Date
        soup = BeautifulSoup(html, "html.parser")
        new_date = ContentEnricherDomain.extract_published_date(soup)

        source = "Regex"
        if not new_date:
            analyzer = CloudNewsAnalyzer()
            date_str = await analyzer.extract_date_from_html(html)
            # Simple parse helper
            from dateutil import parser

            try:
                if date_str:
                    new_date = parser.parse(date_str)
                    source = "LLM"
            except Exception:
                pass

        if new_date:
            # Ensure UTC
            from datetime import timezone

            if new_date.tzinfo is None:
                new_date = new_date.replace(tzinfo=timezone.utc)
            else:
                new_date = new_date.astimezone(timezone.utc)

            article.published_date = new_date
            self.session.commit()
            return f"✅ Updated Date ({source}): {new_date}"

        return "⚠️ Could not extract new date."
