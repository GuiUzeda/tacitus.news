import React from "react";
import { Activity } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableRow } from "@/components/ui/table";
import { FeedEvent } from "../types";
import { formatTimeSinceUpdate } from "@/lib/utils";

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
          {events.map((event) => (
            <TableRow key={event.id} className="group border-b-zinc-100 hover:bg-zinc-50">

              <TableCell className="py-3 font-sans text-sm font-medium text-zinc-700 group-hover:text-zinc-900 break-words whitespace-normal">
                <div className="flex flex-col">
                  <span>{event.title}</span>
                  {event.subtitle && (
                    <span className="text-xs font-normal text-zinc-500 mt-0.5">{event.subtitle}</span>
                  )}
                  <div className="py-2 font-mono text-xs text-zinc-400 justify-center h-full">
                    <Badge variant="outline" className="rounded-none border-zinc-200 text-[10px] font-normal text-zinc-500 group-hover:border-zinc-900 group-hover:text-zinc-900 ">
                      {event.category}
                    </Badge> {new Date(event.time).toLocaleDateString("pt-BR").replace(/\//g, "/")} - {formatTimeSinceUpdate(event.sinceLastUpdate)} atrás
                  </div>
                </div>
              </TableCell>


              <TableCell className="w-[200px] py-3 font-mono text-xs text-zinc-400">
                <div className="flex flex-col space-y-2 justify-center items-center h-full">

                  <div>
                    <div className="flex h-1.5 w-16 overflow-hidden rounded-full bg-zinc-100" title={`L:${event.biasDistribution.left || 0} C:${event.biasDistribution.center || 0} R:${event.biasDistribution.right || 0}`}>
                      <div className="bg-blue-500" style={{ flex: event.biasDistribution.left }} />
                      <div className="bg-zinc-400" style={{ flex: event.biasDistribution.center || 0 }} />
                      <div className="bg-red-500" style={{ flex: event.biasDistribution.right }} />
                    </div>
                  </div>
                  <div className="flex items-center gap-4 pt-2">
                    <div className="flex flex-col">
                      <div>
                        <div className="flex -space-x-2">
                          {/* iterate over sources */}
                          {Object.values(event.sources || {}).slice(0, 4).map((source: any, i) => (
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
                      </div>
                      <div><span className="font-mono text-xs text-zinc-400">
                        {event.articles} ARTIGO{event.articles > 1 ? "S" : ""}
                      </span></div>

                    </div>
                  </div>
                </div>


              </TableCell>
              <TableCell className="w-[60px] space-y-4 py-2 text-center font-mono text-xs font-bold text-zinc-300 group-hover:text-zinc-900">


                <div className="text-zinc-500">

                  {event.impact}
                </div>
                <div>IMPACTO</div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
};