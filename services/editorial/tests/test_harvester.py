import pytest
import asyncio
import sys
import os
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone

# Ensure the service root is in path so we can import workers/domain
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.harvester import HarvesterWorker
from news_events_lib.models import ArticleModel

@pytest.fixture
def mock_worker():
    """
    Creates a HarvesterWorker instance with mocked dependencies
    to avoid connecting to real DB or loading real settings.
    """
    with patch("workers.harvester.create_engine"), \
         patch("workers.harvester.sessionmaker"), \
         patch("workers.harvester.Settings"), \
         patch("workers.harvester.HarvestingDomain"), \
         patch("workers.harvester.GardnerService"):
        
        worker = HarvesterWorker()
        # Mock the SessionLocal to return a MagicMock when called
        worker.SessionLocal = MagicMock()
        return worker

@pytest.mark.asyncio
async def test_process_wrapper_success(mock_worker):
    """
    Verifies that when articles are successfully fetched:
    1. They are saved to the DB.
    2. They are queued for the next stage (FILTER/ENRICH).
    """
    # --- ARRANGE ---
    mock_session = MagicMock()
    # Context manager mock for 'with self.SessionLocal() as session:'
    mock_worker.SessionLocal.return_value.__enter__.return_value = mock_session
    
    # Mock Domain returning 2 raw articles
    raw_articles = [
        {"title": "Article 1", "hash": "hash1"}, 
        {"title": "Article 2", "hash": "hash2"}
    ]
    mock_worker.domain.process_newspaper = AsyncMock(return_value=raw_articles)
    
    # Mock _bulk_save_articles to return 2 saved models
    saved_models = [
        ArticleModel(id=1, title="Article 1"), 
        ArticleModel(id=2, title="Article 2")
    ]
    mock_worker._bulk_save_articles = MagicMock(return_value=saved_models)
    
    # Mock queueing method
    mock_worker.domain.queue_articles_bulk = MagicMock()

    # Inputs for the method
    sem = asyncio.Semaphore(1)
    http_session = AsyncMock()
    np_data = {"name": "Test Paper", "id": 123}

    # --- ACT ---
    await mock_worker._process_wrapper(sem, http_session, np_data)

    # --- ASSERT ---
    # 1. Check Domain Call
    mock_worker.domain.process_newspaper.assert_called_once_with(http_session, np_data)
    
    # 2. Check Save Call
    mock_worker._bulk_save_articles.assert_called_once_with(mock_session, raw_articles)
    
    # 3. Check Queue Call (Crucial Fix Verification)
    mock_worker.domain.queue_articles_bulk.assert_called_once_with(mock_session, saved_models)

@pytest.mark.asyncio
async def test_process_wrapper_no_articles(mock_worker):
    """
    Verifies behavior when no articles are found.
    """
    # --- ARRANGE ---
    mock_session = MagicMock()
    mock_worker.SessionLocal.return_value.__enter__.return_value = mock_session
    
    # Domain returns empty list
    mock_worker.domain.process_newspaper = AsyncMock(return_value=[])
    mock_worker._bulk_save_articles = MagicMock(return_value=[])
    mock_worker.domain.queue_articles_bulk = MagicMock()

    sem = asyncio.Semaphore(1)
    http_session = AsyncMock()
    np_data = {"name": "Test Paper", "id": 123}

    # --- ACT ---
    await mock_worker._process_wrapper(sem, http_session, np_data)

    # --- ASSERT ---
    mock_worker._bulk_save_articles.assert_called_once_with(mock_session, [])
    # Queue should NOT be called if nothing was saved
    mock_worker.domain.queue_articles_bulk.assert_not_called()
    # Session should rollback if nothing saved (based on logic: else: session.rollback())
    mock_session.rollback.assert_called_once()

def test_bulk_save_articles_deduplication(mock_worker):
    """
    Tests that _bulk_save_articles filters out duplicates based on hash.
    """
    mock_session = MagicMock()
    
    # Input: One new article, one existing
    articles_data = [
        {"title": "New", "link": "l1", "hash": "new_hash", "summary": "s", "newspaper_id": 1, "published": None, "rank": 1, "content": "c"},
        {"title": "Old", "link": "l2", "hash": "old_hash", "summary": "s", "newspaper_id": 1, "published": None, "rank": 1, "content": "c"}
    ]
    
    # Mock DB Query: "old_hash" exists
    mock_session.scalars.return_value.all.return_value = ["old_hash"]
    
    # --- ACT ---
    result = mock_worker._bulk_save_articles(mock_session, articles_data)
    
    # --- ASSERT ---
    assert len(result) == 1
    assert result[0].url_hash == "new_hash"
    
    # Verify only the new model was added
    mock_session.add_all.assert_called_once()
    added_models = mock_session.add_all.call_args[0][0]
    assert len(added_models) == 1
    assert added_models[0].url_hash == "new_hash"