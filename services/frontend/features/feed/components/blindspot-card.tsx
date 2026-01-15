import React from "react";
import { EyeOff } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { FeedEvent } from "../types";

export const BlindspotCard = ({ event }: { event: FeedEvent }) => {
  // Determine who is ignoring it.
  // If Left is high and Right is low -> Ignored by Right.
  // If Right is high and Left is low -> Ignored by Left.
  const ignoredBy = event.biasDistribution.left < 15 ? "LEFT" : "RIGHT";
  const side = ignoredBy === "LEFT" ? "RIGHT" : "LEFT";

  return (
    <Card className="rounded-sm border-2 border-dashed border-zinc-900 bg-zinc-50 shadow-none">
      <CardContent className="flex items-center gap-4 p-4">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-zinc-900 text-white">
          <EyeOff className="h-5 w-5" />
        </div>
        <div className="space-y-1">
          <h4 className="font-mono text-xs font-bold uppercase tracking-wider text-zinc-500">
            Blindspot: {side}
          </h4>
          <p className="font-serif text-base font-medium leading-tight text-zinc-900">
            {event.title} <span className="text-zinc-400">— Ignored by the {ignoredBy}.</span>
          </p>
        </div>
      </CardContent>
    </Card>
  );
};