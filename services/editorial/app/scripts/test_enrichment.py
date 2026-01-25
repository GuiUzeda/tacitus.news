import asyncio
import uuid

import aiohttp
from app.core.models import ArticlesQueueModel, ArticlesQueueName  # noqa: E402
from app.workers.enricher.domain import EnrichingDomain  # noqa: E402
from news_events_lib.models import ArticleContentModel  # noqa: E402
from news_events_lib.models import ArticleModel, JobStatus


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
        contents=[],  # Empty content, forcing fetch
        embedding=None,
        interests=None,
        entities=None,
    )

    job = ArticlesQueueModel(
        id=1,
        article=article,
        status=JobStatus.PENDING,
        queue_name=ArticlesQueueName.ENRICH,
    )

    # 3. Run the Process
    print(f"🚀 Processing: {url}")

    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession(headers=domain.headers) as http_session:
        # Step 1: Fetch & CPU
        # _process_single_job_fetch_and_cpu returns (job, result_dict, needs_refilter)
        job, result, needs_refilter = await domain._process_single_job_fetch_and_cpu(
            loop, http_session, job
        )

        if not result:
            print(f"❌ Fetch/CPU Failed: {job.msg}")
            domain.shutdown()
            return

        # Step 2: LLM Analysis (Simulating Batch Phase)
        print("🧠 Running LLM Analysis...")

        # Prepare article for LLM (as done in process_batch)
        article.title = result.get("extracted_title") or article.title
        if not article.contents:
            article.contents = [ArticleContentModel(content=result["content"])]
        else:
            article.contents[0].content = result["content"]

        # Call LLM directly
        try:
            llm_text = f"{article.title}\n\n{article.contents[0].content}"
            llm_outputs = await domain.llm.analyze_articles_batch([llm_text])
            if llm_outputs:
                result["llm_output"] = llm_outputs[0]
        except Exception as e:
            print(f"⚠️ LLM Failed: {e}")

    # 4. Display Results
    print("\n" + "=" * 50)
    print("📊 ENRICHMENT RESULTS")
    print("=" * 50)

    # Apply result to article object for display (simulating _apply_enrichment_result without DB)
    if (
        not article.title or article.title.lower() in ["no title", "unknown"]
    ) and result.get("extracted_title"):
        article.title = result["extracted_title"]

    article.embedding = result["embedding"]
    article.interests = result["interests"]
    article.entities = result["entities"]

    llm_out = result.get("llm_output")
    if llm_out:
        article.summary = llm_out.summary
        article.stance = llm_out.stance
        article.clickbait_score = llm_out.clickbait_score
        # Merge entities
        if llm_out.entities:
            # Flatten entities from LLM
            llm_entities = [
                item for sublist in llm_out.entities.values() for item in sublist
            ]
            article.entities = list(set((article.entities or []) + llm_entities))

    print(f"🏷️  Title:         {article.title}")
    print(f"📝 Summary:       {article.summary}")
    print(f"📅 Published:     {result.get('published_date')}")
    print(f"🔗 Original URL:      {article.original_url}")

    print("-" * 50)
    if article.entities:
        print(
            f"🧠 Entities ({len(article.entities)}): {', '.join(article.entities[:10])}..."
        )
    else:
        print("🧠 Entities: None")

    if article.interests:
        print(f"🎯 Interests: {list(article.interests.keys())}")

    if article.embedding:
        print(f"🔢 Embedding: Generated (Size: {len(article.embedding)})")
    else:
        print("🔢 Embedding: None")

    if llm_out:
        print("-" * 50)
        print(f"🤖 LLM Stance:    {article.stance}")
        print(f"🎣 Clickbait:     {article.clickbait_score}")
        print(f"🔑 Key Points:    {llm_out.key_points}")

    if article.contents:
        content_preview = article.contents[0].content[:300].replace("\n", " ")
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
