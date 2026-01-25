import React from "react";
import { Activity } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableRow } from "@/components/ui/table";
import { FeedEvent } from "../types";
import { cn, formatTimeSinceUpdate, getImpactColor } from "@/lib/utils";

import { BiasDistributuion } from "./bias-distribution";

export const WireTable = ({ events }: { events: FeedEvent[] }) => {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 pb-2 border-b border-zinc-100">
        <Activity className="h-4 w-4 text-zinc-400" />
        <h3 className="font-mono text-sm font-bold uppercase tracking-widest text-zinc-500">
          The Wire
        </h3>
      </div>

      <Table className="table-fixed">
        <TableBody>
          {events.map((event) => {
            const leftBias = event.biasDistribution?.left || 0;
            const centerBias = event.biasDistribution?.center || 0;
            const rightBias = event.biasDistribution?.right || 0;

            const leftStance = event.stanceDistribution?.left || 0;
            const centerStance = event.stanceDistribution?.center || 0;
            const rightStance = event.stanceDistribution?.right || 0;

            return (
              <TableRow
                key={event.id}
                className="group border-b-zinc-100 hover:bg-zinc-50"
              >
                <TableCell className="py-3 font-sans text-sm font-medium text-zinc-500 group-hover:text-zinc-700 break-words whitespace-normal">
                  <div className="flex flex-col space-y-0">
                    <div className="pl-9 -mb-1 font-serif text-lg font-bold tracking-tight leading-[1.3]">
                      <span>{event.title}</span>
                    </div>
                    <div className="flex flex-row items-center ">
                      <div
                        className={cn(
                          "flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-xs opacity-60 transition-opacity group-hover:opacity-100",
                          getImpactColor(event.impact),
                          "text-white",
                        )}
                      >
                        {event.impact}
                      </div>
                      <div className="my-0 h-1 w-full bg-zinc-100  overflow-hidden">
                        <div
                          className={cn(
                            "h-full transition-all opacity-60 group-hover:opacity-100",
                            getImpactColor(event.impact),
                          )}
                          style={{ width: `${event.impact}%` }}
                        />
                      </div>
                    </div>

                    <div className="pl-9 flex flex-col -mt-1">
                      {event.subtitle && (
                        <span className="text-sm font-normal text-zinc-600">
                          {event.subtitle}
                        </span>
                      )}
                      <div className="py-2 font-mono text-xs text-zinc-400 justify-center h-full">
                        <Badge
                          variant="outline"
                          className="rounded-none border-zinc-200 text-[10px] font-normal text-zinc-500 group-hover:border-zinc-900 group-hover:text-zinc-900 "
                        >
                          {event.category}
                        </Badge>{" "}
                        <span className="group-hover:text-zinc-900">
                          {new Date(event.createdAt)
                            .toLocaleDateString("pt-BR")
                            .replace(/\//g, "/")}{" "}
                          - Atualizado{" "}
                          {formatTimeSinceUpdate(event.sinceLastUpdate)} atrás
                        </span>
                      </div>
                    </div>
                  </div>
                </TableCell>

                <TableCell className="w-[200px]  py-3 font-mono text-xs text-zinc-400">
                  <div className="flex flex-col space-y-10 justify-between items-center ">
                    {BiasDistributuion(
                      leftBias,
                      centerBias,
                      rightBias,
                      leftStance,
                      centerStance,
                      rightStance,
                    )}
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
};
