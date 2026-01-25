import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export const getBiasColor = (
  bias: { left: number; right: number } | undefined,
) => {
  if (!bias) return "border-l-zinc-300";
  const left = bias.left || 0;
  const right = bias.right || 0;
  if (left > right + 15) return "border-l-red-600";
  if (right > left + 15) return "border-l-blue-600";
  return "border-l-zinc-300";
};

export const getImpactColor = (score: number) => {
  if (score > 75)
    return "text-red-600 border-red-600 bg-red-600  shadow-[0_0_15px_rgba(220,38,38,0.3)]";
  if (score >= 50) return "text-zinc-500 border-zinc-500 bg-zinc-500";
  return "text-zinc-400 border-zinc-300 bg-zinc-300";
};

// conver since last update in seconds to days or hours in pt-br
export const formatTimeSinceUpdate = (seconds: number): string => {
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0) {
    return `${days} dia${days > 1 ? "s" : ""}`;
  }
  if (hours > 0) {
    return `${hours} hora${hours > 1 ? "s" : ""}`;
  }
  if (minutes > 0) {
    return `${minutes} minuto${minutes > 1 ? "s" : ""}`;
  }
  return `${seconds} segundo${seconds !== 1 ? "s" : ""}`;
};
