import { Masthead } from "@/components/layouts/masthead"; // Ensure this exists
import { HeroEvent } from "@/features/feed/components/hero-event";
import { WireTable } from "@/features/feed/components/wire-table";
import { KeyDevelopments } from "@/features/feed/components/key-developments";
import { BlindspotCard } from "@/features/feed/components/blindspot-card";
import { getFeedEvents } from "@/features/feed/api";

export default async function HomePage() {
  const events = await getFeedEvents();

  // Robust slicing even if few events exist
  const heroEvent = events[0];
  const keyDevelopments = events.slice(1, 6);
  const wireEvents = events.slice(6);
  const blindspotEvent = events.find((e) => e.isBlindspot);

  return (
    <div className="min-h-screen bg-white text-zinc-950">
      <Masthead />
      <main className="container mx-auto max-w-screen-xl px-4 py-8 md:px-8">
        
        {heroEvent && <HeroEvent event={heroEvent} />}
        
        {blindspotEvent && (
           <div className="my-12">
             <BlindspotCard event={blindspotEvent} />
           </div>
        )}

        {keyDevelopments.length > 0 && (
          <div className="mt-12">
            <KeyDevelopments events={keyDevelopments} />
          </div>
        )}

        {wireEvents.length > 0 && (
          <div className="mt-12">
             <WireTable events={wireEvents} />
          </div>
        )}
      </main>
    </div>
  );
}