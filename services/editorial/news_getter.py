import asyncio
import hashlib
import json
import random
import uuid
from datetime import datetime, timedelta
from functools import partial
from time import mktime
from typing import Any, Dict, List, Optional, Sequence, Set
from uuid import UUID

import aiohttp
import feedparser
import spacy
import trafilatura
from config import Settings
from harvesters.factory import HarvesterFactory
from llm_parser import CloudNewsFilter
from loguru import logger
from models import  ArticlesQueueModel, ArticlesQueueName
from news_events_lib.models import (
    ArticleContentModel,
    ArticleModel,
    AuthorModel,
    FeedModel,
    NewspaperModel,
    JobStatus
)
from sentence_transformers import SentenceTransformer
from sqlalchemy import Row, create_engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


class NewsGetter:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
    ]

    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.harvester_factory = HarvesterFactory()
        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, brotli",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
        }
        logger.info("Loading AI Models...")
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True, device="cpu"
        )
        # 2. NER Model (SpaCy) - Fast Entity Extraction
        # Ensure you ran: python -m spacy download pt_core_news_lg
        try:
            self.nlp = spacy.load("pt_core_news_lg")
            # Disable components we don't need for speed (parser, lemmatizer)
            self.nlp.disable_pipes(["parser", "lemmatizer"])
        except OSError:
            logger.error(
                "SpaCy model 'pt_core_news_lg' not found. Run: python -m spacy download pt_core_news_lg"
            )
            self.nlp = None
        logger.info("Models Loaded.")

    @property
    def headers(self):
        headers = self.base_headers.copy()
        headers["User-Agent"] = random.choice(self.USER_AGENTS)
        return headers

    async def fetch_links(
        self, session: aiohttp.ClientSession, sources: List[dict], newspaper_name: str
    ) -> list[dict]:

        harvester = self.harvester_factory.get_harvester(newspaper_name)

        articles = await harvester.harvest(session, sources)

        logger.info(
            f"Harvested {len(articles)} High-Priority articles from {newspaper_name}"
        )
        return articles
    def extract_fast_entities(self, text: str) -> List[str]:
        """
        Extracts PERSON, ORG, and LOC from text using SpaCy.
        Returns a list of clean strings.
        """
        if not self.nlp or not text: 
            return []
            
        # Truncate to avoid memory spikes with massive articles
        doc = self.nlp(text[:50000]) 
        
        entities = []
        interests = {}
        seen = set()
        
        # Prioritize these labels
        target_labels = {"PER", "ORG", "LOC", "MISC"}
        
        for ent in doc.ents:
            clean_text = ent.text.strip()
            if len(clean_text) < 2 or clean_text.lower()  in seen:
                continue
            # Basic filter: remove short noise and duplicates
            if ent.label_ in target_labels:
                entities.append(clean_text)
                if ent.label == "PER":
                    interests['person'] = interests.get("person",[]) + [clean_text]
                elif ent.label == "LOC":
                    interests['place'] = interests.get("place",[]) + [clean_text]
                elif ent.label == "ORG":
                    interests['organization'] = interests.get("organization",[]) + [clean_text]
                else:
                    interests['topic'] = interests.get("topic",[]) + [clean_text]

                seen.add(clean_text.lower())
                
        # Return top 10 most relevant-looking entities
        return entities[:10]
    async def process_newspaper_articles(
        self,
        http_session: aiohttp.ClientSession,
        sources: List[dict],
        newspaper_name: str,
        newspaper_id: UUID,
        ignore_hashes: Optional[set] = None,
        concurrency: int = 10,
        max_failures: int = 5,
    ) -> List[dict]:
        """
        Uses the Harvester to get links from the homepage,
        then passes them to the standard parser.
        """
        if ignore_hashes is None:
            ignore_hashes = set()

        raw_entries = await self.fetch_links(http_session, sources, newspaper_name)

        if not raw_entries:
            return []

        semaphore = asyncio.Semaphore(concurrency)
        failure_count = 0
        circuit_broken = False
        loop = asyncio.get_running_loop()

        async def _parse_entry(entry):
            nonlocal failure_count, circuit_broken
            try:
                if circuit_broken:
                    return None

                clean_link = "https://" + entry["link"].split(r"https://")[-1]
                link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()
                if link_hash in ignore_hashes:
                    return None

                async with semaphore:
                    if circuit_broken:
                        return None
                    url_content = await self.fetch_article_content(
                        http_session, clean_link
                    )

                if not url_content:
                    failure_count += 1
                    if failure_count >= max_failures:
                        circuit_broken = True
                        logger.warning(
                            f"Circuit breaker tripped for {newspaper_name} after {failure_count} consecutive failures."
                        )
                    return None

                # Reset failure count on success
                failure_count = 0

                extracted_json = {}
                try:
                    # Run CPU-bound extraction in a separate thread to avoid blocking the event loop
                    extracted = await loop.run_in_executor(
                        None,
                        partial(
                            trafilatura.extract,
                            url_content,
                            with_metadata=True,
                            output_format="json",
                            include_comments=False,
                        ),
                    )
                    if extracted:
                        extracted_json = json.loads(extracted)
                except Exception as e:
                    logger.error(f"Error parsing {clean_link}: {e}")
                    return None

                if extracted_json:
                    entities = [
                            tag.strip()
                            for tag in extracted_json.get("tags", "")
                            .replace(";", ", ")
                            .split(", ")
                        ]
                    content = extracted_json.get("raw_text", "")
                    if not entities:
                        entities = await asyncio.to_thread(self.extract_fast_entities, content) 
                    embedding = await asyncio.to_thread(self.calculate_vector, content)
                        

                    return {
                        "title": extracted_json.get("title"),
                        "link": clean_link,
                        "hash": link_hash,
                        "published": entry.get(
                            "published",
                            extracted_json.get("date", extracted_json.get("filedate")),
                        ),
                        "summary": extracted_json.get("excerpt"),
                        "content": content,
                        "newspaper_id": newspaper_id,
                        "entities": entities,
                        "embedding": embedding
                    }
                return []
            except Exception as e:
                logger.error(f"Unexpected error processing entry {entry}: {e}")
                return []

        tasks = [_parse_entry(entry) for entry in raw_entries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        final_entries = [r for r in results if r is not None and isinstance(r, dict)]
        return final_entries

    async def fetch_article_content(
        self, http_session: aiohttp.ClientSession, url: str, retries: int = 3
    ):
        for attempt in range(retries):
            try:
                async with http_session.get(
                    url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=40)
                ) as response:
                    response.raise_for_status()
                    return await response.text()
            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error(f"Failed to fetch article content from {url}: {e}")
                return None

    def _parse_published_date(self, pub_data: Optional[str]) -> datetime:
        dt_object = datetime.now()
        if pub_data:
            try:
                dt_object = datetime.fromisoformat(pub_data)
            except (ValueError, TypeError, OverflowError):
                pass
        return dt_object

    def _save_article(self, session, article_data: dict) -> Optional[ArticleModel]:
        try:
            # Use Nested Transaction (Savepoint) for each article
            # If this article fails, it rolls back ONLY this article
            with session.begin_nested():
                dt_object = self._parse_published_date(article_data.get("published"))

                content_model = ArticleContentModel(content=article_data["content"])

                article = ArticleModel(
                    title=article_data["title"],
                    original_url=article_data["link"],
                    url_hash=article_data["hash"],
                    summary=article_data["summary"],
                    summary_date=dt_object,
                    published_date=dt_object,
                    newspaper_id=article_data["newspaper_id"],
                    authors=[],
                    contents=[content_model],
                    # Handle Nullables explicitly to prevent accidental errors
                    subtitle=None,
                    stance_label=None,
                    main_topics=None,
                    entities=article_data["entities"],
                    key_points=None,
                    embedding=article_data["embedding"],
                )

                session.add(article)
                session.flush()  # Force SQL generation to catch constraints
                return article

        except IntegrityError as e:
            # Log unique violations as warnings, ignore them
            logger.warning(f"Skipping duplicate: {article_data['title'][:30]}... {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to save article {article_data['title'][:30]}: {e}")
            return None

    def _queue_articles(self, session, articles: List[ArticleModel]):
        queue_count = 0
        for article in articles:
            if not article.contents:
                continue

            txt = article.contents[0].content
            tokens = len(txt) // 4

            stmt = (
                insert(ArticlesQueueModel)
                .values(
                    article_id=article.id,
                    estimated_tokens=tokens,
                    status=JobStatus.PENDING,
                    queue_name=ArticlesQueueName.FILTER,
                )
                .on_conflict_do_nothing(index_elements=["article_id"])
            )

            session.execute(stmt)
            queue_count += 1

        session.commit()
        logger.success(f"Analysis Queue populated with {queue_count} jobs.")

    async def get_news(
        self,
    ):

        newspapers = await self._get_newspapers()
        logger.info(f"Found {len(newspapers)} newspapers.")
        with self.SessionLocal() as session:
            async with aiohttp.ClientSession(headers=self.headers) as http_session:

                for newspaper in newspapers:
                    await self._process_single_newspaper(
                        session, http_session, newspaper
                    )

    async def _process_single_newspaper(self, session, http_session, newspaper):
        sources = [
            {
                "url": feed.url,
                "blocklist": feed.blocklist,
                "allowed_sections": feed.allowed_sections,
            }
            for feed in newspaper["feeds"]
        ]

        articles_data = await self.process_newspaper_articles(
            http_session,
            sources,
            newspaper["name"],
            newspaper["id"],
            newspaper["article_hashes"],
        )

        successful_articles = []
        for article_data in articles_data:
            article = self._save_article(session, article_data)
            if article:
                successful_articles.append(article)

        session.commit()
        logger.success(
            f"Saved {len(successful_articles)} articles for {newspaper['name']}."
        )

        if successful_articles:
            self._queue_articles(session, successful_articles)

    def calculate_vector(self, text: str) -> list[float]:
        """Generates the vector using your chosen model (Nomic/BGE)."""
        if not text or len(text) < 5:
            logger.warning("Text too short for vectorization. Returning zero vector.")
            return [0.0] * 768

        # Nomic specific prefix
        prefix = "search_document: "
        try:
            return self.embedder.encode(prefix + text).tolist()
        except Exception as e:
            logger.error(f"Vectorization failed: {e}")
            return [0.0] * 768

    async def _get_newspapers(self) -> List[dict]:
        three_months = datetime.now() - timedelta(days=90)
        with self.SessionLocal() as session:
            stmt = (
                select(NewspaperModel)
                .join(FeedModel)
                .where(FeedModel.is_active == True)
                .distinct()
            )
            newspapers = session.execute(stmt).scalars().all()
            if not newspapers:
                return []

            newspaper_ids = [n.id for n in newspapers]

            # Bulk fetch feeds
            feeds_stmt = select(FeedModel).where(
                FeedModel.newspaper_id.in_(newspaper_ids), FeedModel.is_active == True
            )
            all_feeds = session.execute(feeds_stmt).scalars().all()
            feeds_map = {nid: [] for nid in newspaper_ids}
            for feed in all_feeds:
                feeds_map[feed.newspaper_id].append(feed)

            # Bulk fetch hashes
            hashes_stmt = select(
                ArticleModel.newspaper_id, ArticleModel.url_hash
            ).where(
                ArticleModel.newspaper_id.in_(newspaper_ids),
                ArticleModel.published_date >= three_months,
            )
            all_hashes = session.execute(hashes_stmt).all()
            hashes_map = {nid: set() for nid in newspaper_ids}
            for nid, url_hash in all_hashes:
                hashes_map[nid].add(url_hash)

            results = []
            for newspaper in newspapers:
                results.append(
                    {
                        "name": newspaper.name,
                        "id": newspaper.id,
                        "feeds": feeds_map.get(newspaper.id, []),
                        "article_hashes": hashes_map.get(newspaper.id, set()),
                    }
                )
        return results

    async def get_hashes_ignore(self, newspaper_id) -> Set[str]:
        with self.SessionLocal() as session:
            stmt = select(ArticleModel.url_hash).where(
                ArticleModel.newspaper_id == newspaper_id
            )
            hashes = session.execute(stmt).scalars().all()
        return set(hashes)

    async def get_news_from_feed(
        self,
        http_session,
        sources: List[dict],
        newspapaer_name,
        newspaper_id,
        ignore_hashes=set(),
    ):
        return await self.process_newspaper_articles(
            http_session, sources, newspapaer_name, newspaper_id, ignore_hashes
        )


if __name__ == "__main__":
    link_harvester = NewsGetter()

    async def main():
        news = await link_harvester.get_news()
        print(news)

    asyncio.run(main())
