export interface FeedEvent {
  id: string;
  title: string;
  summary: string;
  category: string;
  time: string;
  impact: number;
  sources: { [key: string]: { [key: string]: string } };
  clickbait: number;
  biasDistribution: { left: number; center: number; right: number };
  isBlindspot?: boolean;
  subtitle?: string;
  sinceLastUpdate: number;
  stanceDistribution: { left: number; center: number; right: number };
  articles: number;
  blindSpotSide?: string;
  isInternational: boolean;
  createdAt: string;
  lastArticleDate: string;
  mainTopics: string[];
}

export interface NewsEvent {
  id: string;
  title: string;
  subtitle: string;
  category_tag: string;
  last_updated_at: string;
  ai_impact_score: number;
  article_counts_by_bias: { [key: string]: number };
  stance_distribution: { [key: string]: { [key: string]: number } };
  clickbait_distribution: { [key: string]: number };
  publisher_insights?: string[];
  sources_snapshot: { [key: string]: { [key: string]: string } };
  stance: number;
  article_count: number;
  is_blind_spot: boolean;
  blind_spot_side?: string;
  is_internationa: boolean;
  created_at: string;
  last_article_date: string;
  main_topic_counts: { [key: string]: number };
}
