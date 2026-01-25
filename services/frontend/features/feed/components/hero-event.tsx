import React from "react";
import { Activity } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { FeedEvent } from "../types";
import { getImpactColor } from "@/lib/utils";

export const HeroEvent = ({ event }: { event: FeedEvent }) => {
  const isHighImpact = event.impact > 80;

  return (
    <Card className="group relative overflow-hidden rounded-none border-none shadow-none">
      <CardContent className="p-0 pt-6">
        <div className="grid gap-6 md:grid-cols-[1fr_auto]">
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Badge
                variant="outline"
                className="rounded-none border-zinc-900 px-2 py-0.5 font-mono text-xs font-normal uppercase text-zinc-900"
              >
                #{event.category}
              </Badge>
              <span className="font-mono text-xs text-zinc-500">
                {event.time} UTC
              </span>
              {isHighImpact && (
                <span className="flex items-center gap-1 font-mono text-xs font-bold text-red-600">
                  <Activity className="h-3 w-3" /> CRITICAL
                </span>
              )}
            </div>

            <h1 className="font-serif text-4xl font-bold leading-[1.1] tracking-tight text-zinc-950 sm:text-5xl md:text-6xl">
              {event.title}
            </h1>
            <p className="max-w-2xl font-sans text-lg leading-relaxed text-zinc-600">
              {event.subtitle}
            </p>

            <div className="flex items-center gap-4 pt-2">
              <div className="flex -space-x-2">
                {/* Abstract representation of sources */}
                {Object.values(event.sources || {})
                  .slice(0, 4)
                  .map((source: { [key: string]: string }, i) => (
                    <div
                      key={i}
                      className="h-6 w-6 rounded-full border border-white bg-zinc-200 overflow-hidden"
                      title={source.name}
                    >
                      {/* {source.icon && (

                                <img src={source.icon} alt={source.name} className="h-full w-full object-cover" />
                              )} */}
                    </div>
                  ))}
                {Object.values(event.sources || {}).length > 4 && (
                  <div className="flex h-6 w-6 items-center justify-center rounded-full border border-white bg-zinc-100 text-[9px] font-bold text-zinc-500">
                    +{Object.values(event.sources || {}).length - 4}
                  </div>
                )}
              </div>
              <span className="font-mono text-xs text-zinc-400">
                {event.articles} FOTNTE{event.articles > 1 ? "S" : ""}
              </span>
            </div>
          </div>

          {/* The Hook: Impact Score */}
          <div className="flex flex-col items-center justify-start pt-2 flex-grow justify-between">
            <div
              className={cn(
                "flex h-24 w-24 items-center justify-center rounded-full border-[6px] bg-white font-mono text-4xl font-bold tracking-tighter transition-all",
                getImpactColor(event.impact),
              )}
            >
              {event.impact}
            </div>
            <span className="mt-2 font-mono text-[10px] font-bold uppercase tracking-widest text-zinc-400">
              Impact
            </span>
            <div
              className="flex h-1.5 w-16 overflow-hidden rounded-full bg-zinc-100"
              title={`L:${event.biasDistribution.left || 0} C:${event.biasDistribution.center || 0} R:${event.biasDistribution.right || 0}`}
            >
              <div
                className="bg-blue-500"
                style={{ flex: event.biasDistribution.left }}
              />
              <div
                className="bg-zinc-400"
                style={{ flex: event.biasDistribution.center || 0 }}
              />
              <div
                className="bg-red-500"
                style={{ flex: event.biasDistribution.right }}
              />
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
