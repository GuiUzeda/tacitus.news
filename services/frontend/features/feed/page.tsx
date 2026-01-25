import { Masthead } from "@/components/layouts/masthead"; // Ensure this exists
import { HeroEvent } from "@/features/feed/components/hero-event";
import { WireTable } from "@/features/feed/components/wire-table";
import { BlindspotCard } from "@/features/feed/components/blindspot-card";
import { getFeedEvents, getFeedEventsBlindspot } from "@/features/feed/api";

export default async function FeedPage() {
  const events = await getFeedEvents();
  const blindspotEvents = await getFeedEventsBlindspot();

  // Robust slicing even if few events exist
  const heroEvent = events[0];

  const wireEvents = events.slice(1);

  return (
    <div className="min-h-screen bg-white text-zinc-950">
      <Masthead />
      <main className="container mx-auto max-w-screen-xl px-4 py-8 md:px-8">
        <div className="grid ">
          <div>{heroEvent && <HeroEvent event={heroEvent} />}</div>

          <div className="grid lg:grid-cols-[1.5fr_1fr] gap-20">
            {wireEvents.length > 0 && (
              <div className="mt-12">
                <WireTable events={wireEvents} />
              </div>
            )}
            {blindspotEvents && (
              <div className="my-12 ">
                <BlindspotCard events={blindspotEvents} />
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
