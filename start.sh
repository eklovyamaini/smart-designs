#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  start.sh — Start all services
#
#  Services:
#    • smart_agents frontend   → http://localhost:8000
#    • doc_to_confluence UI    → http://localhost:8001
#
#  Logs:
#    /tmp/smart_agents.log
#    /tmp/doc_to_confluence.log
#
#  Usage:  ./start.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE_SA="$REPO_ROOT/.pids/smart_agents.pid"
PIDFILE_D2C="$REPO_ROOT/.pids/doc_to_confluence.pid"

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[start]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[start]${NC}  $*"; }
error() { echo -e "${RED}[start]${NC}  $*" >&2; }

# ── Resolve uvicorn: prefer miniconda, fall back to PATH ─────────────────────
# The project depends on packages (langchain_ollama, etc.) installed in the
# miniconda base environment.  The system/Homebrew Python may not have them.
MINICONDA_UVICORN="$HOME/miniconda3/bin/uvicorn"
if [[ -x "$MINICONDA_UVICORN" ]]; then
  UVICORN="$MINICONDA_UVICORN"
else
  UVICORN="$(command -v uvicorn 2>/dev/null || echo "")"
fi

if [[ -z "$UVICORN" ]]; then
  error "uvicorn not found. Install it with: pip install uvicorn[standard]"
  exit 1
fi
info "Using uvicorn: $UVICORN"

mkdir -p "$REPO_ROOT/.pids"

# ── Helper: start one service ─────────────────────────────────────────────────
start_service() {
  local name="$1"       # display name
  local module="$2"     # uvicorn module:app string
  local port="$3"       # port number
  local pidfile="$4"    # PID file path
  local logfile="$5"    # log file path

  # Check if already running
  if [[ -f "$pidfile" ]]; then
    local old_pid
    old_pid=$(<"$pidfile")
    if kill -0 "$old_pid" 2>/dev/null; then
      warn "$name is already running (PID $old_pid) on port $port — skipping"
      return 0
    else
      warn "$name PID file stale — removing"
      rm -f "$pidfile"
    fi
  fi

  # Check if the port is already in use by something else
  if lsof -iTCP:"$port" -sTCP:LISTEN -t &>/dev/null; then
    warn "Port $port already in use — $name may already be running (not started)"
    return 0
  fi

  info "Starting $name on http://localhost:$port"
  info "  Log → $logfile"

  nohup "$UVICORN" "$module" \
    --host 127.0.0.1 \
    --port "$port" \
    --reload \
    --reload-dir "$REPO_ROOT" \
    >> "$logfile" 2>&1 &

  local pid=$!
  echo "$pid" > "$pidfile"
  info "$name started (PID $pid)"
}

# ── Start services ────────────────────────────────────────────────────────────
cd "$REPO_ROOT"

start_service \
  "smart_agents" \
  "smart_agents.frontend.main:app" \
  8000 \
  "$PIDFILE_SA" \
  "/tmp/smart_agents.log"

start_service \
  "doc_to_confluence" \
  "doc_to_confluence.frontend.main:app" \
  8001 \
  "$PIDFILE_D2C" \
  "/tmp/doc_to_confluence.log"

echo ""
info "All services started."
echo -e "  ${GREEN}smart_agents     →  http://localhost:8000${NC}"
echo -e "  ${GREEN}doc_to_confluence →  http://localhost:8001${NC}"
echo ""
info "Tail logs with:"
echo "  tail -f /tmp/smart_agents.log"
echo "  tail -f /tmp/doc_to_confluence.log"
echo ""
info "Stop all services with:  ./stop.sh"
