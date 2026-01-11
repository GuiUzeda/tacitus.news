import asyncio
import hashlib
import json
import random
import re
from datetime import datetime, timedelta
from functools import partial
from typing import List, Optional, Set, Dict
from uuid import UUID

import aiohttp
import spacy
import trafilatura
from loguru import logger
from sentence_transformers import SentenceTransformer
from sqlalchemy import select, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

# Local project imports
from config import Settings
from harvesters.factory import HarvesterFactory
from models import ArticlesQueueModel, ArticlesQueueName
from news_events_lib.models import (
    ArticleContentModel,
    ArticleModel,
    FeedModel,
    NewspaperModel,
    JobStatus
)

class NewsGetter:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    ]

    # Phrases that usually signal the start of the footer/marketing junk
    STOP_PHRASES = [
        "apoie o jornalismo",
        "assine a edição",
        "assine agora",
        "faça parte da nossa comunidade",
        "receba as notícias",
        "receba as principais notícias",
        "siga-nos no",
        "siga a gente",
        "clique aqui para",
        "leia mais em:",
        "leia também:",
        "copyright ©",
        "todos os direitos reservados",
        "entre no canal do whatsapp",
        "participe do grupo",
        "conteúdo exclusivo para assinantes",
        "fale com o colunista",
        "newsletter",
        "inscreva-se",
        "baixe o app",
        "google news",
        "redação:",
        "colaboração para o",
        "veja também",
        "o post apareceu primeiro em"
    ]

    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.harvester_factory = HarvesterFactory()
        
        # Standard Headers
        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }

        logger.info("Loading AI Models...")
        # 1. Embedding Model
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True, device="cpu"
        )
        # Warmup to initialize lazy buffers (Rotary Embeddings) before multi-threaded use
        # This prevents race conditions where _sin_cached is None during concurrent forward passes
        # We use a text longer than our truncation limit (3000 chars) to ensure buffers are fully allocated.
        self.embedder.encode("warmup " * 1000)
        
        # 2. NER Model (SpaCy)
        try:
            self.nlp = spacy.load("pt_core_news_lg")
            self.nlp.disable_pipes(["parser", "lemmatizer"])
        except OSError:
            logger.error("SpaCy model 'pt_core_news_lg' not found.")
            self.nlp = None
        
        logger.info("NewsGetter Initialized.")

    @property
    def headers(self):
        h = self.base_headers.copy()
        h["User-Agent"] = random.choice(self.USER_AGENTS)
        return h

    # --- TEXT CLEANING & VECTORIZATION (The Fix) ---

    def clean_text_for_embedding(self, text: str) -> str:
        """
        Aggressively strips footer boilerplate to prevent vector contamination.
        """
        if not text:
            return ""

        lines = text.split('\n')
        clean_lines = []
        
        # Heuristic: If we hit a stop phrase, we assume everything after is junk.
        for line in lines:
            lower_line = line.lower().strip()
            
            # Check for exact stop phrase matches
            if any(phrase in lower_line for phrase in self.STOP_PHRASES):
                # Extra check: Don't stop if the line is very long (it might be a narrative sentence containing a common word)
                if len(line) < 150: 
                    break 
            
            clean_lines.append(line)
            
        cleaned_text = "\n".join(clean_lines).strip()

        # Safety Fallback: If we stripped too much (e.g., empty string), use the original first 1000 chars
        if len(cleaned_text) < 50 and len(text) > 200:
            logger.warning("Cleaning removed almost all text. Reverting to head of original.")
            return text[:2000]

        return cleaned_text

    def calculate_vector(self, text: str) -> list[float]:
        """
        Generates vector from CLEANED and TRUNCATED text.
        """
        if not text or len(text) < 10:
            return [0.0] * 768

        # 1. Clean
        clean_txt = self.clean_text_for_embedding(text)
        
        # 2. Truncate (Nomic allows 8192, but for clustering, the lead is key)
        # We take the first ~3000 chars (approx 500-700 tokens) to ensure 
        # the footer never influences the centroid.
        truncated_txt = clean_txt[:3000]

        prefix = "search_document: "
        try:
            return self.embedder.encode(prefix + truncated_txt).tolist()
        except Exception as e:
            logger.error(f"Vectorization failed: {e}")
            return [0.0] * 768

    def extract_interests(self, text: str) -> Dict[str, List[str]]:
        """Extracts entities categorized for interests."""
        if not self.nlp or not text: 
            return {}
            
        # We only need the start of the article for main entities
        doc = self.nlp(text[:5000]) 
        
        interests = {
            "person": [],
            "organization": [],
            "place": [],
            "topic": []
        }
        seen = set()
        
        # Map SpaCy labels to our schema
        label_map = {
            "PER": "person",
            "ORG": "organization",
            "LOC": "place",
            "MISC": "topic"
        }
        
        for ent in doc.ents:
            clean = ent.text.strip()
            if len(clean) < 2 or clean.lower() in seen:
                continue
            
            category = label_map.get(ent.label_)
            if category:
                interests[category].append(clean)
                seen.add(clean.lower())
                
        # Clean up empty keys and limit
        return {k: v[:5] for k, v in interests.items() if v}

    # --- FETCHING PIPELINE ---

    async def fetch_links(self, session: aiohttp.ClientSession, sources: List[dict], newspaper_name: str) -> list[dict]:
        harvester = self.harvester_factory.get_harvester(newspaper_name)
        articles = await harvester.harvest(session, sources)
        logger.info(f"[{newspaper_name}] Found {len(articles)} candidate links.")
        return articles

    async def fetch_article_content(self, http_session: aiohttp.ClientSession, url: str, retries: int = 2):
        for attempt in range(retries):
            try:
                async with http_session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception:
                await asyncio.sleep(1)
        return None

    async def process_newspaper_articles(
        self,
        http_session: aiohttp.ClientSession,
        sources: List[dict],
        newspaper_name: str,
        newspaper_id: UUID,
        ignore_hashes: Optional[set] = None,
        concurrency: int = 5
    ) -> List[dict]:
        
        if ignore_hashes is None: ignore_hashes = set()
        raw_entries = await self.fetch_links(http_session, sources, newspaper_name)
        
        if not raw_entries: return []

        semaphore = asyncio.Semaphore(concurrency)
        loop = asyncio.get_running_loop()
        
        async def _process(entry):
            try:
                clean_link = entry["link"]
                # Basic hash check
                link_hash = hashlib.md5(clean_link.split("?")[0].encode()).hexdigest()
                if link_hash in ignore_hashes:
                    return None

                async with semaphore:
                    html = await self.fetch_article_content(http_session, clean_link)

                if not html: return None

                # CPU-bound Extraction
                extracted_json = await loop.run_in_executor(
                    None,
                    partial(trafilatura.extract, html, with_metadata=True, output_format="json", include_comments=False)
                )
                
                if not extracted_json: return None
                
                data = json.loads(extracted_json)
                raw_text = data.get("raw_text", "")
                
                if len(raw_text) < 100: return None # Skip empty articles

                # --- CRITICAL: Generate Metadata from CLEAN Text ---
                clean_text = self.clean_text_for_embedding(raw_text)
                
                # 1. Calc Embedding (Uses clean_text internally)
                embedding = await loop.run_in_executor(None, self.calculate_vector, clean_text)
                
                # 2. Extract Entities & Interests
                # Trafilatura tags -> entities (flat)
                tags = [t.strip() for t in data.get("tags", "").split(",") if t.strip()]
                
                # SpaCy -> interests (categorized)
                extraction_text = f"{data.get('title', '')} {data.get('excerpt', '')} {clean_text[:2000]}"
                interests = await loop.run_in_executor(None, self.extract_interests, extraction_text)
                
                # Flatten interests for 'entities' column (legacy/search support)
                flat_interests = [item for sublist in interests.values() for item in sublist]
                all_entities = list(set(tags + flat_interests))[:15]

                return {
                    "title": data.get("title") or entry.get("title"),
                    "link": clean_link,
                    "hash": link_hash,
                    "published": entry.get("published") or data.get("date"),
                    "summary": data.get("excerpt"),
                    "content": raw_text, # Save FULL text
                    "newspaper_id": newspaper_id,
                    "entities": all_entities,
                    "interests": interests,
                    "embedding": embedding
                }
            except Exception as e:
                logger.error(f"Error processing {entry['link']}: {e}")
                return None

        tasks = [_process(e) for e in raw_entries]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    # --- DATABASE SAVING ---

    def _parse_published_date(self, pub_data: Optional[str]) -> datetime:
        if not pub_data: return datetime.now()
        try:
            return datetime.fromisoformat(str(pub_data))
        except:
            return datetime.now()

    def _save_article(self, session, data: dict) -> Optional[ArticleModel]:
        try:
            with session.begin_nested():
                dt = self._parse_published_date(data.get("published"))
                
                article = ArticleModel(
                    title=data["title"],
                    original_url=data["link"],
                    url_hash=data["hash"],
                    summary=data["summary"],
                    summary_date=dt,
                    published_date=dt,
                    newspaper_id=data["newspaper_id"],
                    authors=[],
                    contents=[ArticleContentModel(content=data["content"])],
                    entities=data["entities"],
                    interests=data.get("interests", {}),
                    embedding=data["embedding"],
                    subtitle=None, stance=None, main_topics=None, key_points=None
                )
                session.add(article)
                session.flush()
                return article
        except IntegrityError:
            return None # Duplicate
        except Exception as e:
            logger.error(f"DB Error saving {data['link']}: {e}")
            return None

    def _queue_articles(self, session, articles: List[ArticleModel]):
        if not articles: return
        stmt = insert(ArticlesQueueModel).values([
            {
                "article_id": a.id,
                "estimated_tokens": len(a.contents[0].content)//4,
                "status": JobStatus.PENDING,
                "queue_name": ArticlesQueueName.FILTER
            } for a in articles
        ]).on_conflict_do_nothing()
        session.execute(stmt)

    # --- MAIN EXECUTION ---

    async def get_news(self):
        newspapers = await self._get_newspapers()
        logger.info(f"Processing {len(newspapers)} newspapers.")
        
        async with aiohttp.ClientSession(headers=self.headers) as http_session:
            with self.SessionLocal() as session:
                for np in newspapers:
                    await self._process_single_newspaper(session, http_session, np)

    async def _process_single_newspaper(self, session, http_session, np):
        sources = [{"url": f.url} for f in np["feeds"]]
        articles_data = await self.process_newspaper_articles(
            http_session, sources, np["name"], np["id"], np["article_hashes"]
        )
        
        saved_count = 0
        saved_objs = []
        for d in articles_data:
            obj = self._save_article(session, d)
            if obj: 
                saved_count += 1
                saved_objs.append(obj)
        
        if saved_objs:
            self._queue_articles(session, saved_objs)
        
        session.commit()
        if saved_count > 0:
            logger.success(f"[{np['name']}] Saved {saved_count} new articles.")

    async def _get_newspapers(self) -> List[dict]:
        # Helper to fetch config from DB
        with self.SessionLocal() as session:
            cutoff = datetime.now() - timedelta(days=60)
            
            # 1. Get Newspapers with Active Feeds
            q = select(NewspaperModel).join(FeedModel).where(FeedModel.is_active == True).distinct()
            newspapers = session.execute(q).scalars().all()
            
            if not newspapers: return []
            
            # 2. Pre-fetch Feeds & Hashes (Bulk Optimization)
            ids = [n.id for n in newspapers]
            
            feeds = session.execute(
                select(FeedModel).where(FeedModel.newspaper_id.in_(ids), FeedModel.is_active == True)
            ).scalars().all()
            
            hashes = session.execute(
                select(ArticleModel.newspaper_id, ArticleModel.url_hash)
                .where(ArticleModel.newspaper_id.in_(ids), ArticleModel.published_date >= cutoff)
            ).all()

            # 3. Map Results
            feed_map = {i: [] for i in ids}
            for f in feeds: feed_map[f.newspaper_id].append(f)
            
            hash_map = {i: set() for i in ids}
            for nid, h in hashes: hash_map[nid].add(h)

            return [{
                "id": n.id, 
                "name": n.name, 
                "feeds": feed_map[n.id], 
                "article_hashes": hash_map[n.id]
            } for n in newspapers]

if __name__ == "__main__":
    getter = NewsGetter()
    asyncio.run(getter.get_news())