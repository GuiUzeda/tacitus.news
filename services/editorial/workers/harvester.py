import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import time
import uuid
from datetime import datetime, timedelta
from typing import List
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
import aiohttp

# Imports
from config import Settings
from domain.harvesting import HarvestingDomain
from news_events_lib.models import NewspaperModel, FeedModel, ArticleModel

class HarvesterWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(
            str(self.settings.pg_dsn),
            pool_size=10,
            pool_timeout=30
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        
        # Instantiate Domain (manages ProcessPool)
        self.domain = HarvestingDomain()
        self.worker_id = str(uuid.uuid4())[:8]
        
    async def run(self):
        logger.info(f"🌾 Harvester Worker Started. ID: {self.worker_id}")
        
        while True:
            cycle_start_time = datetime.now()
            try:
                # 1. Fetch Configuration (IO Bound)
                newspapers = self._get_active_newspapers()
                if not newspapers:
                    logger.warning(f"[{self.worker_id}] No active newspapers found. Sleeping...")
                    await asyncio.sleep(300)
                    continue

                logger.info(f"[{self.worker_id}] Starting Cycle for {len(newspapers)} newspapers...")

                # 2. Shared HTTP Session for the cycle
                async with aiohttp.ClientSession() as http_session:
                    
                    # 3. Process Parallelly (IO Bound)
                    # We limit concurrent newspapers to avoid overloading DB connections or network
                    sem = asyncio.Semaphore(5)
                    
                    tasks = [
                        self._process_wrapper(sem, http_session, np) 
                        for np in newspapers
                    ]
                    await asyncio.gather(*tasks)

                logger.success(f"[{self.worker_id}] Cycle Finished.")
                
                # Sleep to align with hourly schedule (1 hour after START)
                elapsed = (datetime.now() - cycle_start_time).total_seconds()
                sleep_duration = max(0, 3600 - elapsed)
                logger.info(f"[{self.worker_id}] Sleeping {sleep_duration:.0f}s to align with hourly schedule...")
                await asyncio.sleep(sleep_duration)

            except Exception as e:
                logger.critical(f"[{self.worker_id}] Harvester Cycle Crashed: {e}")
                await asyncio.sleep(60)

    async def _process_wrapper(self, sem, http_session, np_data):
        async with sem:
            logger.info(f"[{self.worker_id}] [{np_data['name']}] 🗞️  Processing Newspaper...")

            # We open a NEW DB session per newspaper to keep transactions short
            # and avoid "idle in transaction" issues during long http requests
            with self.SessionLocal() as session:
                try:
                    count = await self.domain.process_newspaper(
                        session, http_session, np_data
                    )
                    if count > 0:
                        logger.success(f"[{self.worker_id}] [{np_data['name']}] Cycle finished. Total {count} articles saved.")
                    else:
                        session.rollback()
                except Exception:
                    session.rollback()
    def _get_active_newspapers(self) -> List[dict]:
        with self.SessionLocal() as session:
            cutoff = datetime.now() - timedelta(days=60)
            
            # 1. Get Newspapers
            q = select(NewspaperModel).join(FeedModel).where(FeedModel.is_active == True).distinct()
            newspapers = session.execute(q).scalars().all()
            
            if not newspapers: return []
            ids = [n.id for n in newspapers]
            
            # 2. Get Feeds
            feeds = session.execute(
                select(FeedModel).where(FeedModel.newspaper_id.in_(ids), FeedModel.is_active == True)
            ).scalars().all()
            
            # 3. Get Hashes
            hashes = session.execute(
                select(ArticleModel.newspaper_id, ArticleModel.url_hash)
                .where(ArticleModel.newspaper_id.in_(ids), ArticleModel.published_date >= cutoff)
            ).all()

            feed_map = {i: [] for i in ids}
            for f in feeds:
                feed_map[f.newspaper_id].append({
                    "url": f.url,
                    "feed_type": f.feed_type,
                    "url_pattern": f.url_pattern,
                    "blocklist": f.blocklist,
                    "allowed_sections": f.allowed_sections,
                    # Pass the flags down
                    "is_ranked": f.is_ranked,
                    "use_browser_render": f.use_browser_render,
                    "scroll_depth": f.scroll_depth
                })
            
            hash_map = {i: set() for i in ids}
            for nid, h in hashes: hash_map[nid].add(h)

            return [{
                "id": n.id, 
                "name": n.name, 
                "feeds": feed_map[n.id], 
                "article_hashes": hash_map[n.id]
            } for n in newspapers]

if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()
    
    worker = HarvesterWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Stopping...")