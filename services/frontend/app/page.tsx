import { 
  Masthead, 
  HeroEvent, 
  KeyDevelopments, 
  WireTable, 
  BlindspotCard, 
  Event
} from "../components/feed/feed-components";
import { Separator } from "@/components/ui/separator";

// --- Mock Data ---

const MOCK_EVENTS: Event[] = [
  {
    id: "1",
    title: "Federal Reserve Announces Surprise Rate Cut Amid Global Volatility",
    summary: "In a move that shocked markets, the Fed cut rates by 50bps citing 'irregularities' in Asian markets. The S&P 500 rallied 3% immediately following the announcement.",
    category: "ECONOMY",
    time: "14:02",
    impact: 92,
    sourceCount: 142,
    clickbait: 0.1,
    biasDistribution: { left: 30, center: 40, right: 30 },
  },
  {
    id: "2",
    title: "Senate Passes Controversial Infrastructure Bill",
    summary: "The 60-40 vote clears the way for $1.2T in spending. Opposition leaders claim the bill contains 'poison pill' amendments regarding digital privacy.",
    category: "POLITICS",
    time: "13:45",
    impact: 78,
    sourceCount: 89,
    clickbait: 0.2,
    biasDistribution: { left: 60, center: 20, right: 20 }, // Left leaning coverage
  },
  {
    id: "3",
    title: "Oil Prices Surge as Tanker Seized in Strait of Hormuz",
    summary: "Geopolitical tensions flare as a Norwegian-flagged vessel is detained. Brent Crude touched $95/barrel in early trading.",
    category: "WORLD",
    time: "12:30",
    impact: 85,
    sourceCount: 210,
    clickbait: 0.0,
    biasDistribution: { left: 45, center: 45, right: 10 },
  },
  {
    id: "4",
    title: "Tech Giant Unveils Quantum Processor Prototype",
    summary: "The 1000-qubit chip claims to solve protein folding problems in seconds. Experts remain skeptical about error correction rates.",
    category: "TECH",
    time: "11:15",
    impact: 65,
    sourceCount: 45,
    clickbait: 0.4,
    biasDistribution: { left: 33, center: 33, right: 33 },
  },
  {
    id: "5",
    title: "Union Strikes Halt Major Port Operations on East Coast",
    summary: "Negotiations stalled over automation clauses. Supply chain analysts predict 2-week delays for consumer goods.",
    category: "ECONOMY",
    time: "10:00",
    impact: 72,
    sourceCount: 67,
    clickbait: 0.1,
    biasDistribution: { left: 80, center: 10, right: 10 }, // Heavy Left bias
  },
  {
    id: "6",
    title: "New Report Challenges Climate Models on Ocean Acidification",
    summary: "A controversial study suggests resilience in coral systems is higher than previously thought. Environmental groups call the methodology flawed.",
    category: "WORLD",
    time: "09:45",
    impact: 55,
    sourceCount: 23,
    clickbait: 0.3,
    biasDistribution: { left: 10, center: 20, right: 70 }, // Right leaning (Blindspot candidate?)
    isBlindspot: true,
  },
  // Wire Items
  { id: "7", title: "Local Elections: Early Exit Polls Show Shift", summary: "", category: "POLITICS", time: "13:55", impact: 45, sourceCount: 12, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "8", title: "SpaceX Starship Launch Delayed by Weather", summary: "", category: "TECH", time: "13:40", impact: 30, sourceCount: 50, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "9", title: "Crypto Markets Flash Crash: Bitcoin down 5%", summary: "", category: "ECONOMY", time: "13:15", impact: 48, sourceCount: 34, clickbait: 0.6, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "10", title: "Supreme Court to Hear Digital Assets Case", summary: "", category: "POLITICS", time: "12:50", impact: 60, sourceCount: 22, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "11", title: "Major Merger Announced in Pharma Sector", summary: "", category: "ECONOMY", time: "12:10", impact: 52, sourceCount: 18, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
];

export default function Home() {
  const heroEvent = MOCK_EVENTS[0];
  const keyDevelopments = MOCK_EVENTS.slice(1, 6);
  const wireEvents = MOCK_EVENTS.slice(6);
  const blindspotEvent = MOCK_EVENTS.find(e => e.isBlindspot);

  return (
    <div className="min-h-screen bg-white text-zinc-950 selection:bg-zinc-900 selection:text-white">
      <Masthead />

      <main className="container mx-auto max-w-screen-xl px-4 py-8 md:px-8">
        <div className="grid gap-12 lg:grid-cols-[1.5fr_1fr]">
          
          {/* LEFT COLUMN: Hero + Wire */}
          <div className="space-y-12">
            <section>
              <HeroEvent event={heroEvent} />
            </section>

            <Separator className="bg-zinc-100" />

            <section>
              <WireTable events={wireEvents} />
            </section>
          </div>

          {/* RIGHT COLUMN: Key Developments + Blindspots */}
          <div className="space-y-8">
            {blindspotEvent && (
              <BlindspotCard event={blindspotEvent} />
            )}
            
            <KeyDevelopments events={keyDevelopments} />
          </div>

        </div>
      </main>
    </div>
  );
}
