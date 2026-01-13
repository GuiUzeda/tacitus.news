#!/bin/bash

# Ensure we are in the script's directory (project root)
cd "$(dirname "$0")"

SESSION="tacitus_logs"

# Check if tmux is installed
if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Please install tmux to use this script."
    exit 1
fi

# Check if session exists
if tmux has-session -t $SESSION 2>/dev/null; then
    echo "Session $SESSION already exists. Attaching..."
    tmux attach -t $SESSION
    exit 0
fi

echo "🚀 Creating Tmux Grid for Tacitus Workers..."

# Helper to generate the command string
# We use 'bash -c' to ensure the shell stays open if the container exits/restarts
get_cmd() {
    echo "bash -c 'docker compose logs -f --tail=50 $1; echo \"--- Stream exited ---\"; read'"
}

# --- ROW 1 (Top) ---
# 1. Start Session with Top-Left (Harvester)
tmux new-session -d -s $SESSION -x 200 -y 50 "$(get_cmd worker-harvester)"

# 2. Create Row 2 (Bottom) by splitting vertically (50% height)
# We temporarily put Reviewer here (Bottom-Left)
tmux split-window -v -l '50%' "$(get_cmd worker-reviewer)"

# 3. Fill Row 1 (Top)
# Go back to Top-Left
tmux select-pane -t 0
# Split remaining space to the right to create 4 equal columns
tmux split-window -h -l '75%' "$(get_cmd worker-filter)"
tmux split-window -h -l '66%' "$(get_cmd worker-cluster)"
tmux split-window -h -l '50%' "$(get_cmd worker-merger)"

# --- ROW 2 (Bottom) ---
# Navigate to Bottom-Left. 
# We go to Top-Left (0) then Down.
tmux select-pane -t 0
tmux select-pane -D
# Split remaining space to the right to create 4 equal columns
tmux split-window -h -l '75%' "$(get_cmd worker-enhancer)"
tmux split-window -h -l '66%' "$(get_cmd worker-publisher)"
# We have 7 workers. The 8th slot is perfect for the Backend.
tmux split-window -h -l '50%' "$(get_cmd backend)"



# Final Polish
tmux rename-window 'Tacitus Grid'
tmux select-pane -t 0

# Attach to the session
tmux attach -t $SESSION