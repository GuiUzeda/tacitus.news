import pytest
import asyncio
import sys
import os
from unittest.mock import MagicMock, AsyncMock, patch

# Ensure the service root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.harvester import HarvesterWorker

@pytest.fixture
def mock_worker_deps():
    """Mocks all external dependencies for HarvesterWorker init."""
    with patch("workers.harvester.create_engine"), \
         patch("workers.harvester.sessionmaker"), \
         patch("workers.harvester.Settings"), \
         patch("workers.harvester.HarvestingDomain"), \
         patch("workers.harvester.GardnerService"), \
         patch("workers.harvester.aiohttp.ClientSession"):
        yield

@pytest.mark.asyncio
async def test_harvester_run_loop_execution(mock_worker_deps):
    """
    Tests the main infinite loop of the HarvesterWorker.
    Uses a side_effect on asyncio.sleep to break the loop after one iteration.
    """
    # We patch asyncio.sleep specifically to control the loop exit
    with patch("workers.harvester.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        worker = HarvesterWorker()
        
        # 1. Mock finding newspapers
        worker._get_active_newspapers = MagicMock(return_value=[
            {"name": "Test Paper", "id": 1, "feeds": []}
        ])
        
        # 2. Mock the processing wrapper (so we don't need to mock domain/http details deep down)
        worker._process_wrapper = AsyncMock()
        
        # 3. Mock the Splitter cycle
        worker.splitter.run_cycle = AsyncMock()
        
        # 4. Mock Session for Splitter
        mock_session = MagicMock()
        worker.SessionLocal = MagicMock()
        worker.SessionLocal.return_value.__enter__.return_value = mock_session

        # 5. Define Loop Break
        # The worker calls sleep() at the end of the loop. 
        # We raise an exception there to stop the 'while True'.
        class StopLoop(Exception): pass
        mock_sleep.side_effect = StopLoop()

        # ACT
        try:
            await worker.run()
        except StopLoop:
            pass

        # ASSERT
        # Verify flow
        worker._get_active_newspapers.assert_called()
        worker._process_wrapper.assert_called()
        worker.splitter.run_cycle.assert_called_once_with(mock_session)
        
        # Verify sleep was called (scheduling logic)
        mock_sleep.assert_called()

@pytest.mark.asyncio
async def test_harvester_run_loop_no_newspapers(mock_worker_deps):
    """
    Tests that the worker sleeps for 300s if no newspapers are found.
    """
    with patch("workers.harvester.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        worker = HarvesterWorker()
        
        # Return empty list
        worker._get_active_newspapers = MagicMock(return_value=[])
        
        class StopLoop(Exception): pass
        mock_sleep.side_effect = StopLoop()

        try:
            await worker.run()
        except StopLoop:
            pass

        # Should sleep 300s
        mock_sleep.assert_called_with(300)
        # Should NOT process
        assert not worker._process_wrapper.called