import React from "react";
import { Separator } from "@/components/ui/separator";
import { FeedEvent } from "./types";
import { HeroEvent } from "./components/hero-event";
import { WireTable } from "./components/wire-table";
import { KeyDevelopments } from "./components/key-developments";
import { BlindspotCard } from "./components/blindspot-card";

interface FeedLayoutProps {
  hero: FeedEvent;
  wire: FeedEvent[];
  keyDevs: FeedEvent[];
  blindspot?: FeedEvent;
}

export function FeedLayout({
  hero,
  wire,
  keyDevs,
  blindspot,
}: FeedLayoutProps) {
  return (
    <div className="grid gap-12 lg:grid-cols-[1.5fr_1fr]">
      {/* LEFT COLUMN: Hero + Wire */}
      <div className="space-y-12">
        <section>
          <HeroEvent event={hero} />
        </section>

        <Separator className="bg-zinc-100" />

        <section>
          <WireTable events={wire} />
        </section>
      </div>

      {/* RIGHT COLUMN: Key Developments + Blindspots */}
      <div className="space-y-8">
        {blindspot && <BlindspotCard event={blindspot} />}
        <KeyDevelopments events={keyDevs} />
      </div>
    </div>
  );
}
