import React from "react";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { FeedEvent } from "../types";
import { getBiasColor } from "@/lib/utils";

export const KeyDevelopments = ({ events }: { events: FeedEvent[] }) => {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 pb-2">
        <div className="h-2 w-2 bg-zinc-900" />
        <h3 className="font-mono text-sm font-bold uppercase tracking-widest text-zinc-500">
          Key Developments
        </h3>
      </div>

      <div className="grid gap-4">
        {events.map((event) => (
          <Card
            key={event.id}
            className={cn(
              "rounded-sm border-0 border-l-4 bg-zinc-50/50 shadow-sm transition-colors hover:bg-zinc-50",
              getBiasColor(event.biasDistribution),
            )}
          >
            <CardHeader className="p-4">
              <div className="flex items-start justify-between gap-4">
                <div className="space-y-1.5">
                  <div className="flex items-center gap-2">
                    <Badge
                      variant="secondary"
                      className="rounded-sm px-1.5 py-0 text-[10px] font-normal text-zinc-600"
                    >
                      {event.category}
                    </Badge>
                    <span className="font-mono text-[10px] text-zinc-400">
                      {event.time}
                    </span>
                  </div>
                  <CardTitle className="font-serif text-lg font-medium leading-tight text-zinc-900">
                    {event.title}
                  </CardTitle>
                </div>
                <div className="flex flex-col items-end gap-1">
                  <span className="font-mono text-xl font-bold text-zinc-900">
                    {event.impact}
                  </span>
                </div>
              </div>
            </CardHeader>
          </Card>
        ))}
      </div>
    </div>
  );
};
