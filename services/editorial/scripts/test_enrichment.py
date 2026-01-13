import sys
import os
import asyncio
import uuid
import aiohttp
from loguru import logger

# Add service root to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from news_events_lib.models import ArticleModel, JobStatus
from core.models import ArticlesQueueModel, ArticlesQueueName
from domain.enriching import EnrichingDomain

async def main():
    print("🧪 Enrichment Process Tester")
    url = input("Enter Article URL to test: ").strip()
    if not url:
        print("❌ URL is required.")
        return

    # 1. Initialize Domain
    # We use 1 worker to keep it simple and low resource
    print("⏳ Initializing EnrichingDomain (loading AI models)...")
    domain = EnrichingDomain(max_cpu_workers=1)
    
    # Warmup is optional but good to verify models load correctly
    try:
        await asyncio.to_thread(domain.warmup)
    except Exception as e:
        print(f"❌ Failed to load models: {e}")
        return

    # 2. Create Mock Data
    # We simulate an article that came from the Harvester with no title
    article = ArticleModel(
        id=uuid.uuid4(),
        title="No Title",  # Simulate missing title to trigger extraction
        original_url=url,
        summary=None,
        contents=[],       # Empty content, forcing fetch
        embedding=None,
        interests=None,
        entities=None
    )
    
    job = ArticlesQueueModel(
        id=1,
        article=article,
        status=JobStatus.PENDING,
        queue_name=ArticlesQueueName.ENRICH
    )

    # 3. Run the Process
    print(f"🚀 Processing: {url}")
    
    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession(headers=domain.headers) as http_session:
        # This is the core method we want to test
        await domain._process_single_job(loop, http_session, job)

    # 4. Display Results
    print("\n" + "="*50)
    print("📊 ENRICHMENT RESULTS")
    print("="*50)
    
    if job.status == JobStatus.PENDING and job.queue_name == ArticlesQueueName.CLUSTER:
        print("✅ Status: SUCCESS (Moved to CLUSTER)")
    else:
        print(f"❌ Status: {job.status}")
        print(f"⚠️  Message: {job.msg}")

    print("-" * 50)
    print(f"🏷️  Extracted Title:   {article.title}")
    print(f"📝 Extracted sutitle: {article.subtitle}")
    print(f"📝 Extracted Summary: {article.summary}")
    print(f"📅 Published Date:     {article.published_date}")
    print(f"🔗 Original URL:      {article.original_url}")
    
    print("-" * 50)
    if article.entities:
        print(f"🧠 Entities ({len(article.entities)}): {', '.join(article.entities[:10])}...")
    else:
        print("🧠 Entities: None")
        
    if article.interests:
        print(f"🎯 Interests: {list(article.interests.keys())}")
    
    if article.embedding:
        print(f"🔢 Embedding: Generated (Size: {len(article.embedding)})")
    else:
        print("🔢 Embedding: None")

    if article.contents:
        content_preview = article.contents[0].content[:300].replace('\n', ' ')
        print("-" * 50)
        print(f"📄 Content Preview:\n{content_preview}...")
    else:
        print("📄 Content: None")

    # Cleanup
    domain.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest cancelled.")