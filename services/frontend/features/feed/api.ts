import { apiClient } from "@/lib/api-client";
import { FeedEvent, NewsEvent } from "./types";

export const getFeedEvents = async (): Promise<FeedEvent[]> => {
  try {
    const data = await apiClient<{ items: NewsEvent[] }>(
      "/v1/events?order_by=-hot_score&status=published&is_active=true&is_blind_spot=false&page=1&size=50",
    );

    const feedEvents: FeedEvent[] = data.items.map((newsEvent) => ({
      id: newsEvent.id,
      title: newsEvent.title,
      summary: newsEvent.subtitle, // Assuming subtitle maps to summary
      category: newsEvent.category_tag, // Type assertion needed
      time: newsEvent.last_updated_at,
      impact: newsEvent.ai_impact_score,
      sources: newsEvent.sources_snapshot,
      clickbait: newsEvent.clickbait_distribution.total, // Assuming a 'total' field or similar
      biasDistribution: {
        left:
          newsEvent.article_counts_by_bias.left ||
          0 + newsEvent.article_counts_by_bias.leans_left ||
          0 + newsEvent.article_counts_by_bias.far_left ||
          0,
        center: newsEvent.article_counts_by_bias.center || 0,
        right:
          newsEvent.article_counts_by_bias.right ||
          0 + newsEvent.article_counts_by_bias.leans_right ||
          0 + newsEvent.article_counts_by_bias.far_right ||
          0,
      },
      isBlindspot: newsEvent.is_blind_spot,
      subtitle: newsEvent.subtitle,
      sinceLastUpdate:
        (Date.now() - new Date(newsEvent.last_article_date).getTime()) / 1000,
      stanceDistribution: {
        left:
          newsEvent.stance_distribution.left?.total ||
          0 + newsEvent.stance_distribution.leans_left?.total ||
          0 + newsEvent.stance_distribution.far_left?.total ||
          0,
        center: newsEvent.stance_distribution.center?.total || 0,
        right:
          newsEvent.stance_distribution.right?.total ||
          0 + newsEvent.stance_distribution.leans_right?.total ||
          0 + newsEvent.stance_distribution.far_right?.total ||
          0,
      },
      articles: newsEvent.article_count,
      blindSpotSide: newsEvent.blind_spot_side,
      isInternational: newsEvent.is_internationa,
      createdAt: newsEvent.created_at,
      lastArticleDate: newsEvent.last_article_date,
      mainTopics: Object.entries(newsEvent.main_topic_counts)
        .sort(([, countA], [, countB]) => countB - countA)
        .slice(0, 5)
        .map(([topic]) => topic),
    }));
    return feedEvents;
  } catch (error) {
    console.warn("API Fetch failed, using mock data.", error);
    return [];
  }
};
export const getFeedEventsBlindspot = async (): Promise<FeedEvent[]> => {
  try {
    const data = await apiClient<{ items: NewsEvent[] }>(
      "/v1/events?order_by=-hot_score&status=published&is_active=true&is_blind_spot=true&page=1&size=10",
    );

    const feedEvents: FeedEvent[] = data.items.map((newsEvent) => ({
      id: newsEvent.id,
      title: newsEvent.title,
      summary: newsEvent.subtitle, // Assuming subtitle maps to summary
      category: newsEvent.category_tag, // Type assertion needed
      time: newsEvent.last_updated_at,
      impact: newsEvent.ai_impact_score,
      sources: newsEvent.sources_snapshot,
      clickbait: newsEvent.clickbait_distribution.total, // Assuming a 'total' field or similar
      biasDistribution: {
        left:
          newsEvent.article_counts_by_bias.left ||
          0 + newsEvent.article_counts_by_bias.leans_left ||
          0 + newsEvent.article_counts_by_bias.far_left ||
          0,
        center: newsEvent.article_counts_by_bias.center || 0,
        right:
          newsEvent.article_counts_by_bias.right ||
          0 + newsEvent.article_counts_by_bias.leans_right ||
          0 + newsEvent.article_counts_by_bias.far_right ||
          0,
      },
      isBlindspot: newsEvent.is_blind_spot,
      subtitle: newsEvent.subtitle,
      sinceLastUpdate:
        (Date.now() - new Date(newsEvent.last_article_date).getTime()) / 1000,
      stanceDistribution: {
        left:
          newsEvent.stance_distribution.left?.total ||
          0 + newsEvent.stance_distribution.leans_left?.total ||
          0 + newsEvent.stance_distribution.far_left?.total ||
          0,
        center: newsEvent.stance_distribution.center?.total || 0,
        right:
          newsEvent.stance_distribution.right?.total ||
          0 + newsEvent.stance_distribution.leans_right?.total ||
          0 + newsEvent.stance_distribution.far_right?.total ||
          0,
      },
      articles: newsEvent.article_count,
      blindSpotSide: newsEvent.blind_spot_side,
      isInternational: newsEvent.is_internationa,
      createdAt: newsEvent.created_at,
      lastArticleDate: newsEvent.last_article_date,
      mainTopics: Object.entries(newsEvent.main_topic_counts)
        .sort(([, countA], [, countB]) => countB - countA)
        .slice(0, 5)
        .map(([topic]) => topic),
    }));
    return feedEvents;
  } catch (error) {
    console.warn("API Fetch failed, using mock data.", error);
    return [];
  }
};
