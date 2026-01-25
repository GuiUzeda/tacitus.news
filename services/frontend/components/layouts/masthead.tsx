import React from "react";
import { Menu, Search } from "lucide-react";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";

export const Masthead = () => {
  const today = new Date()
    .toLocaleDateString("en-US", {
      weekday: "short",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    })
    .toUpperCase();

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
