import React from "react";
import { EyeOff, Clock, Activity, Menu, Search } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";
import { Table, TableBody, TableCell, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

// --- Types ---

export interface Event {
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
}

// --- Helpers ---

const getBiasColor = (bias: { left: number; right: number }) => {
  // Red = Left, Blue = Right (Per instructions)
  if (bias.left > bias.right + 15) return "border-l-red-600";
  if (bias.right > bias.left + 15) return "border-l-blue-600";
  return "border-l-zinc-300";
};

const getImpactColor = (score: number) => {
  if (score > 80) return "text-red-600 border-red-600 shadow-[0_0_15px_rgba(220,38,38,0.3)]";
  if (score >= 50) return "text-zinc-900 border-zinc-900";
  return "text-zinc-400 border-zinc-300";
};

// --- Components ---

export const Masthead = () => {
  const today = new Date().toLocaleDateString("en-US", {
    weekday: "short",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).toUpperCase();

  return (
    <header className="sticky top-0 z-50 w-full border-b border-zinc-200 bg-white/95 backdrop-blur supports-[backdrop-filter]:bg-white/60">
      <div className="container flex h-14 max-w-screen-xl items-center justify-between px-4 md:px-8">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <Menu className="h-5 w-5" />
            <h1 className="font-serif text-2xl font-bold tracking-tighter text-zinc-950">
              TACITUS
            </h1>
          </div>
          <Separator orientation="vertical" className="h-6" />
          <span className="hidden font-mono text-xs text-zinc-500 sm:inline-block">
            {today}
          </span>
        </div>

        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <span className="hidden text-[10px] font-bold uppercase tracking-wider text-zinc-500 sm:inline-block">
              High Impact Only
            </span>
            <Switch id="impact-mode" />
          </div>
          <Separator orientation="vertical" className="h-6" />
          <Search className="h-5 w-5 text-zinc-900" />
        </div>
      </div>
    </header>
  );
};

export const HeroEvent = ({ event }: { event: Event }) => {
  const isHighImpact = event.impact > 80;

  return (
    <Card className="group relative overflow-hidden rounded-none border-none shadow-none">
      <CardContent className="p-0 pt-6">
        <div className="grid gap-6 md:grid-cols-[1fr_auto]">
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Badge variant="outline" className="rounded-none border-zinc-900 px-2 py-0.5 font-mono text-xs font-normal uppercase text-zinc-900">
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
              {event.summary}
            </p>

            <div className="flex items-center gap-4 pt-2">
              <div className="flex -space-x-2">
                {/* Abstract representation of sources */}
                {[...Array(Math.min(event.sourceCount, 4))].map((_, i) => (
                  <div
                    key={i}
                    className="h-6 w-6 rounded-full border border-white bg-zinc-200"
                  />
                ))}
                {event.sourceCount > 4 && (
                  <div className="flex h-6 w-6 items-center justify-center rounded-full border border-white bg-zinc-100 text-[9px] font-bold text-zinc-500">
                    +{event.sourceCount - 4}
                  </div>
                )}
              </div>
              <span className="font-mono text-xs text-zinc-400">
                {event.sourceCount} SOURCES ANALYZED
              </span>
            </div>
          </div>

          {/* The Hook: Impact Score */}
          <div className="flex flex-col items-center justify-start pt-2">
            <div
              className={cn(
                "flex h-24 w-24 items-center justify-center rounded-full border-[6px] bg-white font-mono text-4xl font-bold tracking-tighter transition-all",
                getImpactColor(event.impact)
              )}
            >
              {event.impact}
            </div>
            <span className="mt-2 font-mono text-[10px] font-bold uppercase tracking-widest text-zinc-400">
              Impact
            </span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

export const KeyDevelopments = ({ events }: { events: Event[] }) => {
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
              getBiasColor(event.biasDistribution)
            )}
          >
            <CardHeader className="p-4">
              <div className="flex items-start justify-between gap-4">
                <div className="space-y-1.5">
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary" className="rounded-sm px-1.5 py-0 text-[10px] font-normal text-zinc-600">
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

export const WireTable = ({ events }: { events: Event[] }) => {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 pb-2 border-b border-zinc-100">
        <Activity className="h-4 w-4 text-zinc-400" />
        <h3 className="font-mono text-sm font-bold uppercase tracking-widest text-zinc-500">
          The Wire
        </h3>
      </div>

      <Table>
        <TableBody>
          {events.map((event) => (
            <TableRow key={event.id} className="group border-b-zinc-100 hover:bg-zinc-50">
              <TableCell className="w-[80px] py-3 font-mono text-xs text-zinc-400">
                {event.time}
              </TableCell>
              <TableCell className="w-[100px] py-3">
                <Badge variant="outline" className="rounded-none border-zinc-200 text-[10px] font-normal text-zinc-500 group-hover:border-zinc-900 group-hover:text-zinc-900">
                  {event.category}
                </Badge>
              </TableCell>
              <TableCell className="py-3 font-sans text-sm font-medium text-zinc-700 group-hover:text-zinc-900">
                {event.title}
              </TableCell>
              <TableCell className="py-3 text-right font-mono text-xs font-bold text-zinc-300 group-hover:text-zinc-900">
                {event.impact}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
};

export const BlindspotCard = ({ event }: { event: Event }) => {
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