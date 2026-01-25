// Detect if we are on the Server (Docker) or Client (Browser)
const BASE_URL =
  typeof window === "undefined"
    ? process.env.API_INTERNAL_URL // http://backend:8000
    : process.env.NEXT_PUBLIC_API_URL; // https://tacitus.news/api

export const apiClient = async <T>(
  endpoint: string,
  options?: RequestInit,
): Promise<T> => {
  const res = await fetch(`${BASE_URL}${endpoint}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
    // Next.js 14 Caching Strategy
    cache: options?.cache || "no-store", // Default to real-time for news
  });

  if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
  return res.json();
};
