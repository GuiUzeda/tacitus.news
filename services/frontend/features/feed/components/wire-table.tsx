import React from "react";
import { Activity } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableRow } from "@/components/ui/table";
import { FeedEvent } from "../types";

export const WireTable = ({ events }: { events: FeedEvent[] }) => {
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