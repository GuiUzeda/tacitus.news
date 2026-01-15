import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export const getBiasColor = (bias: { left: number; right: number } | undefined) => {
  if (!bias) return "border-l-zinc-300";
  const left = bias.left || 0;
  const right = bias.right || 0;
  if (left > right + 15) return "border-l-red-600";
  if (right > left + 15) return "border-l-blue-600";
  return "border-l-zinc-300";
};

export const getImpactColor = (score: number) => {
  if (score > 80) return "text-red-600 border-red-600 shadow-[0_0_15px_rgba(220,38,38,0.3)]";
  if (score >= 50) return "text-zinc-900 border-zinc-900";
  return "text-zinc-400 border-zinc-300";
};