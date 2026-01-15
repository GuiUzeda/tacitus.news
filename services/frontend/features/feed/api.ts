import { apiClient } from "@/lib/api-client";
import { FeedEvent } from "./types";

const MOCK_EVENTS: FeedEvent[] = [
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
    biasDistribution: { left: 60, center: 20, right: 20 },
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
  { id: "4", title: "Tech Giant Unveils Quantum Processor Prototype", summary: "The 1000-qubit chip claims to solve protein folding problems in seconds.", category: "TECH", time: "11:15", impact: 65, sourceCount: 45, clickbait: 0.4, biasDistribution: { left: 33, center: 33, right: 33 } },
  { id: "5", title: "Union Strikes Halt Major Port Operations on East Coast", summary: "Negotiations stalled over automation clauses.", category: "ECONOMY", time: "10:00", impact: 72, sourceCount: 67, clickbait: 0.1, biasDistribution: { left: 80, center: 10, right: 10 } },
  { id: "6", title: "New Report Challenges Climate Models on Ocean Acidification", summary: "A controversial study suggests resilience in coral systems is higher than previously thought.", category: "WORLD", time: "09:45", impact: 55, sourceCount: 23, clickbait: 0.3, biasDistribution: { left: 10, center: 20, right: 70 }, isBlindspot: true },
  { id: "7", title: "Local Elections: Early Exit Polls Show Shift", summary: "", category: "POLITICS", time: "13:55", impact: 45, sourceCount: 12, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "8", title: "SpaceX Starship Launch Delayed by Weather", summary: "", category: "TECH", time: "13:40", impact: 30, sourceCount: 50, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "9", title: "Crypto Markets Flash Crash: Bitcoin down 5%", summary: "", category: "ECONOMY", time: "13:15", impact: 48, sourceCount: 34, clickbait: 0.6, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "10", title: "Supreme Court to Hear Digital Assets Case", summary: "", category: "POLITICS", time: "12:50", impact: 60, sourceCount: 22, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
  { id: "11", title: "Major Merger Announced in Pharma Sector", summary: "", category: "ECONOMY", time: "12:10", impact: 52, sourceCount: 18, clickbait: 0, biasDistribution: { left: 50, right: 50, center: 0 } },
];

export const getFeedEvents = async (): Promise<FeedEvent[]> => {
  try {
    const data = await apiClient<any>("/v1/events?order_by=-hot_score&status=published&limit=50&is_active=true");
    if (Array.isArray(data)) return data;
    if (data && Array.isArray(data.items)) return data.items;
    return MOCK_EVENTS;
  } catch (error) {
    console.warn("API Fetch failed, using mock data.", error);
    return MOCK_EVENTS;
  }
};
