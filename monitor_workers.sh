#!/bin/bash

# Ensure we are in the script's directory (project root)
cd "$(dirname "$0")"

WORKERS=(
  "worker-harvester"
  "worker-filter"
  "worker-enricher"
  "worker-cluster"
  "worker-merger"
  "worker-reviewer"
  "worker-enhancer"
  "worker-publisher"
)

# The command to run. We add a 'read' at the end so the window stays open if the container restarts/crashes.
CMD_TEMPLATE="docker compose logs -f --tail=50 SERVICE_NAME; echo '--- Stream exited ---'; read"

echo "🚀 Spawning log monitors for ${#WORKERS[@]} workers..."

if command -v gnome-terminal &> /dev/null; then
    # Gnome Terminal: Opens one window with multiple tabs
    GNOME_ARGS=()
    for SERVICE in "${WORKERS[@]}"; do
        CMD=${CMD_TEMPLATE/SERVICE_NAME/$SERVICE}
        GNOME_ARGS+=(--tab --title="$SERVICE" -- bash -c "$CMD")
    done
    gnome-terminal "${GNOME_ARGS[@]}"

elif command -v xfce4-terminal &> /dev/null; then
    # XFCE Terminal: Opens one window with multiple tabs
    XFCE_ARGS=()
    for SERVICE in "${WORKERS[@]}"; do
        CMD=${CMD_TEMPLATE/SERVICE_NAME/$SERVICE}
        XFCE_ARGS+=(--tab --title="$SERVICE" --command="bash -c '$CMD'")
    done
    xfce4-terminal "${XFCE_ARGS[@]}"

elif command -v konsole &> /dev/null; then
    # KDE Konsole
    for SERVICE in "${WORKERS[@]}"; do
        CMD=${CMD_TEMPLATE/SERVICE_NAME/$SERVICE}
        konsole --new-tab -p tabtitle="$SERVICE" -e /bin/bash -c "$CMD" &
    done

else
    # Fallback (xterm or similar)
    for SERVICE in "${WORKERS[@]}"; do
        CMD=${CMD_TEMPLATE/SERVICE_NAME/$SERVICE}
        xterm -T "$SERVICE" -e "bash -c \"$CMD\"" &
    done
fi