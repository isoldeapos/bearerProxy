#!/usr/bin/env bash
# run-proxy.sh - supervise hermes-bearer-proxy.js and restart it on crash.
#
# Usage:
#   ./run-proxy.sh
#
# Behavior:
#   - Restarts the proxy whenever it exits for any reason (crash, OOM, etc.)
#   - Exponential backoff on crash loops: if the proxy dies within
#     MIN_UPTIME_SECS of starting, the restart delay doubles (1s -> 2s -> ...
#     up to MAX_DELAY_SECS). A run longer than MIN_UPTIME_SECS resets it.
#   - Ctrl+C (or SIGTERM to this script) stops both the proxy and the loop.
#   - Appends restart events to hermes-bearer-proxy.supervisor.log next to
#     this script; the proxy's own stdout/stderr still go to the terminal.
#
# Env vars (UPSTREAM_TIMEOUT_MS, UPSTREAM_RETRIES, ...) pass through to node:
#   UPSTREAM_TIMEOUT_MS=15000 ./run-proxy.sh
#
# To supervise a bun-compiled executable instead of the .js:
#   PROXY_BIN=./hermes-bearer-proxy ./run-proxy.sh

set -u

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXY_JS="$DIR/hermes-bearer-proxy.js"
LOG="$DIR/hermes-bearer-proxy.supervisor.log"

if [[ -n "${PROXY_BIN:-}" ]]; then
  start_cmd=("$PROXY_BIN")
else
  start_cmd=(node "$PROXY_JS")
fi

MIN_UPTIME_SECS=10
MAX_DELAY_SECS=60
delay=1

child_pid=""

log() {
  local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg"
  echo "$msg" >> "$LOG"
}

shutdown() {
  log "supervisor: shutting down"
  if [[ -n "$child_pid" ]] && kill -0 "$child_pid" 2>/dev/null; then
    kill "$child_pid" 2>/dev/null
    wait "$child_pid" 2>/dev/null
  fi
  exit 0
}
trap shutdown INT TERM

if [[ -z "${PROXY_BIN:-}" && ! -f "$PROXY_JS" ]]; then
  echo "error: $PROXY_JS not found" >&2
  exit 1
fi

log "supervisor: starting (${start_cmd[*]})"

# Tells the proxy it's supervised: after a successful auto-update it just
# exits and this loop restarts it as the new version (instead of spawning a
# second copy itself).
export HERMES_SUPERVISED=1

while true; do
  start_ts=$(date +%s)
  "${start_cmd[@]}" &
  child_pid=$!
  wait "$child_pid"
  exit_code=$?
  child_pid=""
  uptime=$(( $(date +%s) - start_ts ))

  if (( uptime >= MIN_UPTIME_SECS )); then
    delay=1
  else
    delay=$(( delay * 2 ))
    (( delay > MAX_DELAY_SECS )) && delay=$MAX_DELAY_SECS
  fi

  log "proxy exited (code=$exit_code, uptime=${uptime}s) - restarting in ${delay}s"
  sleep "$delay"
done
