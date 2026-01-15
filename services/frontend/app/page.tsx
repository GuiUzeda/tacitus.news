import { Masthead } from "@/components/layouts/masthead";
import { FeedLayout } from "@/features/feed/components/feed-layout";
import { getFeedEvents } from "@/features/feed/api";

// 1. This is a Server Component by default
export default async function HomePage() {
  // 2. Fetch Data (Runs on Server)
  const events = await getFeedEvents();

  // 3. Transform Data
  const heroEvent = events[0];
  const keyDevelopments = events.slice(1, 6);
  const wireEvents = events.slice(6);
  const blindspotEvent = events.find((e) => e.isBlindspot);

  // 4. Render
  return (
    <div className="min-h-screen bg-white text-zinc-950 selection:bg-zinc-900 selection:text-white">
      <Masthead />
      <main className="container mx-auto max-w-screen-xl px-4 py-8 md:px-8">
        <FeedLayout
          hero={heroEvent}
          wire={wireEvents}
          keyDevs={keyDevelopments}
          blindspot={blindspotEvent}
        />
      </main>
    </div>
  );
}
