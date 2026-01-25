export function BiasDistributuion(
  leftBias: number,
  centerBias: number,
  rightBias: number,
  leftStance: number,
  centerStance: number,
  rightStance: number,
) {
  return (
    <div>
      <div
        className="flex h-12 w-30 border-zinc-600 rounded-none overflow-hidden relative"
        title={`Bias: L${leftBias} C${centerBias} R${rightBias} | Stance: L${leftStance.toFixed(2)} C${centerStance.toFixed(2)} R${rightStance.toFixed(2)}`}
      >
        <div className="absolute top-1/2 w-full h-px bg-zinc-300 z-0" />

        <div
          style={{ flex: leftBias }}
          className="h-full relative border-r border-white/50 last:border-0"
        >
          <div
            className="absolute w-full bg-red-500 transition-all opacity-80 hover:opacity-100"
            style={{
              height: `${Math.max(Math.min(Math.abs(leftStance), 1) * 50, 10)}%`,
              bottom: leftStance > 0 ? "50%" : "auto",
              top: leftStance <= 0 ? "50%" : "auto",
            }}
          />
        </div>

        <div
          style={{ flex: centerBias }}
          className="h-full relative border-r border-white/50 last:border-0"
        >
          <div
            className="absolute w-full bg-zinc-400 transition-all opacity-80 hover:opacity-100"
            style={{
              height: `${Math.max(Math.min(Math.abs(centerStance), 1) * 50, 10)}%`,
              bottom: centerStance > 0 ? "50%" : "auto",
              top: centerStance <= 0 ? "50%" : "auto",
            }}
          />
        </div>

        <div
          style={{ flex: rightBias }}
          className="h-full relative border-r border-white/50 last:border-0"
        >
          <div
            className="absolute w-full bg-blue-500 transition-all opacity-80 hover:opacity-100"
            style={{
              height: `${Math.max(Math.min(Math.abs(rightStance), 1) * 50, 10)}%`,
              bottom: rightStance > 0 ? "50%" : "auto",
              top: rightStance <= 0 ? "50%" : "auto",
            }}
          />
        </div>
      </div>
    </div>
  );
}
