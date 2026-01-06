import asyncio
import fcntl
import sys
import os
from loguru import logger
from sqlalchemy import select, func

# Import your classes
from news_producer import NewsProducer
from news_consumer import NewsConsumer
from models import ArticlesQueueModel, JobStatus

# Lock file location - ensures system-wide uniqueness
LOCK_FILE = "/tmp/news_aggregator_main.lock"

def acquire_lock():
    """
    Tries to acquire an exclusive lock on the lock file.
    If it fails, it means another instance is running.
    """
    try:
        # Open the file (create if not exists)
        lock_file_handle = open(LOCK_FILE, 'w')
        
        # Try to lock it. 
        # LOCK_EX = Exclusive Lock (only one process)
        # LOCK_NB = Non-Blocking (fail immediately if locked)
        fcntl.flock(lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        return lock_file_handle
    except IOError:
        return None

async def wait_for_consumer_completion(consumer: NewsConsumer):
    """
    Keeps the Consumer running until the database queue is empty.
    """
    logger.info("Consumer mode: Processing until empty...")
    
    # Start the worker loop in the background
    worker_task = asyncio.create_task(consumer.run())
    
    # Check the database every 10 seconds to see if we are done
    while True:
        await asyncio.sleep(10)
        
        with consumer.SessionLocal() as session:
            # Count pending or processing jobs
            remaining = session.scalar(
                select(func.count(ArticlesQueueModel.id))
                .where(ArticlesQueueModel.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]))
            )
            
            # Also check if the internal worker is actually doing something (inflight)
            if remaining == 0 and consumer.inflight_tokens == 0:
                logger.info("Queue drained. Stopping Consumer.")
                worker_task.cancel() # Stop the infinite loop
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
                break
            else:
                logger.info(f"Consumer working... {remaining} jobs remaining.")

async def main():
    # 1. ACQUIRE LOCK
    lock = acquire_lock()
    if not lock:
        logger.warning("Another instance of main.py is already running. Skipping this hour.")
        sys.exit(0)

    try:
        logger.info("=== STARTING BATCH PROCESS ===")

        # 2. RUN PRODUCER
        # This will fetch feeds, save articles, and populate the queue
        logger.info(">>> STEP 1: PRODUCER")
        producer = NewsProducer()
        await producer.run()
        
        # 3. RUN CONSUMER
        # This will process the queue until it hits 0
        logger.info(">>> STEP 2: CONSUMER")
        consumer = NewsConsumer()
        
        # We use a helper to stop the consumer when done
        await wait_for_consumer_completion(consumer)

        logger.success("=== BATCH PROCESS COMPLETED ===")

    except Exception as e:
        logger.error(f"Critical Error in main process: {e}")
    finally:
        # Release lock (optional, as OS does it on exit, but good practice)
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()
        os.remove(LOCK_FILE)

if __name__ == "__main__":
    asyncio.run(main())