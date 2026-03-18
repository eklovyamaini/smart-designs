#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  stop.sh — Stop all services
#
#  Sends SIGTERM to each tracked PID, waits up to 5 s, then SIGKILL if needed.
#  Also falls back to port-based cleanup in case PID files are missing.
#
#  Usage:  ./stop.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE_SA="$REPO_ROOT/.pids/smart_agents.pid"
PIDFILE_D2C="$REPO_ROOT/.pids/doc_to_confluence.pid"

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[stop]${NC}   $*"; }
warn()  { echo -e "${YELLOW}[stop]${NC}   $*"; }

# ── Helper: stop one service ──────────────────────────────────────────────────
stop_service() {
  local name="$1"
  local pidfile="$2"
  local port="$3"
  local stopped=false

  # ── Attempt 1: PID file ───────────────────────────────────────────────────
  if [[ -f "$pidfile" ]]; then
    local pid
    pid=$(<"$pidfile")
    # Also check child processes (uvicorn --reload spawns a worker child)
    if kill -0 "$pid" 2>/dev/null || ps -o pid= -g "$pid" &>/dev/null; then
      info "Stopping $name (PID $pid)…"
      kill -TERM "$pid" 2>/dev/null || true

      # Wait up to 5 seconds for graceful exit
      local waited=0
      while kill -0 "$pid" 2>/dev/null && (( waited < 5 )); do
        sleep 1
        (( waited++ ))
      done

      if kill -0 "$pid" 2>/dev/null; then
        warn "$name did not exit in 5 s — sending SIGKILL"
        kill -KILL "$pid" 2>/dev/null || true
      fi

      stopped=true
      info "$name stopped"
    else
      warn "$name PID file found but process $pid is not running"
    fi
    rm -f "$pidfile"
  fi

  # ── Attempt 2: port-based fallback (catches processes started manually) ───
  local port_pids
  port_pids=$(lsof -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null || true)
  if [[ -n "$port_pids" ]]; then
    for ppid in $port_pids; do
      if [[ "$stopped" == false ]] || ! grep -qx "$ppid" <<< ""; then
        warn "Found leftover process on port $port (PID $ppid) — killing"
        kill -TERM "$ppid" 2>/dev/null || true
        sleep 1
        kill -0 "$ppid" 2>/dev/null && kill -KILL "$ppid" 2>/dev/null || true
      fi
    done
  fi

  if [[ "$stopped" == false ]] && [[ -z "$port_pids" ]]; then
    info "$name was not running"
  fi
}

# ── Stop services (reverse startup order) ────────────────────────────────────
stop_service "doc_to_confluence" "$PIDFILE_D2C" 8001
stop_service "smart_agents"      "$PIDFILE_SA"  8000

# Clean up empty pids directory
rmdir "$REPO_ROOT/.pids" 2>/dev/null || true

echo ""
info "All services stopped."
