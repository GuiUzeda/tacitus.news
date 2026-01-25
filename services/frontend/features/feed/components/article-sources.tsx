import Image from "next/image";
export function ArticleSources(event: FeedEvent) {
  return (
    <div className="flex items-center gap-4">
      <div className="flex flex-col">
        <div>
          <div className="flex -space-x-2">
            {/* iterate over sources */}
            {Object.values(event.sources || {})
              .slice(0, 4)
              .map((source, i) => (
                <div
                  key={event.id + String(i)}
                  className="h-6 w-6 rounded-full border border-white bg-zinc-200 overflow-hidden"
                  title={source.name}
                >
                  {source.icon && (
                    <Image
                      src={source.icon}
                      alt={source.name}
                      className="h-full w-full object-cover"
                    />
                  )}
                </div>
              ))}
            {Object.values(event.sources || {}).length > 4 && (
              <div className="flex h-6 w-6 items-center justify-center rounded-full border border-white bg-zinc-100 text-[9px] font-bold text-zinc-500">
                +{Object.values(event.sources || {}).length - 4}
              </div>
            )}
          </div>
        </div>
        <div>
          <span className="font-mono text-xs text-zinc-400">
            {event.articles} ARTIGO{event.articles > 1 ? "S" : ""}
          </span>
        </div>
      </div>
    </div>
  );
}
