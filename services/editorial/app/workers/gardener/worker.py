import asyncio
import signal
from datetime import datetime, timezone

from app.config import Settings
from app.workers.gardener.domain import NewsGardenerDomain
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class NewsGardenerWorker:
    def __init__(self):
        self.settings = Settings()
        self.engine = create_engine(str(self.settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)

        self.domain = NewsGardenerDomain()
        self.running = True
        self.INTERVAL_SECONDS = 3600  # 1 Hour
        self.HOT_SCORE_INTERVAL = 15 * 60  # 15 min
        self.last_run_score_decay = None

    async def run(self):
        logger.info("🌿 Gardener Maintenance Worker started")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

        while self.running:
            try:
                # 1. Sync Maintenance (Janitor/Archivist)
                await asyncio.to_thread(self._run_sync_maintenance)

                # 2. Async Maintenance (Splitter/Score Decay)
                await self._run_async_maintenance()

                # Sleep
                for _ in range(self.INTERVAL_SECONDS):
                    if (
                        not self.last_run_score_decay
                        or (
                            datetime.now(timezone.utc) - self.last_run_score_decay
                        ).total_seconds()
                        > self.HOT_SCORE_INTERVAL
                    ):
                        self.last_run_score_decay = datetime.now(timezone.utc)
                        await self.domain.run_score_decay(self.SessionLocal())
                    if not self.running:
                        break
                    await asyncio.sleep(1)

            except Exception as e:
                logger.critical(f"🔥 Gardener Crash: {e}")
                await asyncio.sleep(60)

    def stop(self):
        self.running = False
        logger.warning("🛑 Stopping Gardener...")

    def _run_sync_maintenance(self):
        with self.SessionLocal() as session:
            stats = self.domain.run_maintenance_cycle(session)
            session.commit()
            if any(stats.values()):
                logger.success(f"🧹 Janitor: {stats}")

    async def _run_async_maintenance(self):
        # We need a new session for the async part
        with self.SessionLocal() as session:
            await self.domain.run_splitter_cycle(session)
            # Commit happens inside domain methods if needed


if __name__ == "__main__":
    worker = NewsGardenerWorker()
    asyncio.run(worker.run())
