export interface FeedEvent {
  id: string;
  title: string;
  summary: string;
  category: "POLITICS" | "ECONOMY" | "WORLD" | "TECH";
  time: string;
  impact: number;
  sourceCount: number;
  clickbait: number;
  biasDistribution: { left: number; center: number; right: number };
  isBlindspot?: boolean;
  subtitle?: string
}