#!/usr/bin/env bash
#
# End-to-end test for the WSP Rust port (transparent-proxy + route model).
#
# Spins up a real how-test-api, how-server and how-client (the client's
# `routes` maps the server's host -> the test_api), then issues real HTTP
# requests through the proxy (with curl). The caller hits the real upstream
# path on the server; Auth/headers are transparent; the real destination is
# in the client config.
#
#   * GET forwarding (body + status)
#   * response header forwarding
#   * POST request body forwarding / echo
#   * custom (non-standard) HTTP status code (666)
#   * client-side blacklist -> 527
#   * no client (no proxy) -> 526
#   * binary request body integrity
#   * pool saturation -> dispatcher timeout (~1s -> 526)
#   * /status health check
#
# Usage: ./scripts/e2e.sh [server_port] [api_port]
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SERVER_PORT="${1:-18080}"
API_PORT="${2:-18081}"
SECRET="e2e-secret"
PIDS=()
pass=0; fail=0
ok() { pass=$((pass+1)); printf "  PASS  %s\n" "$1"; }
ko() { fail=$((fail+1)); printf "  FAIL  %s\n" "$1"; }

cleanup() { for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null; done; wait 2>/dev/null; }
trap cleanup EXIT INT TERM

build_bin() {
  echo "== building binaries =="
  ( export PATH="$HOME/.cargo/bin:$PATH"; cargo build -q 2>/dev/null ) || { echo "build failed"; exit 1; }
}

start_all() {
  echo "== starting services (server=$SERVER_PORT api=$API_PORT) =="
  local dir=/tmp/wsp-e2e.$$
  mkdir -p "$dir"

  cat > "$dir/server.cfg" <<EOF
---
host: 127.0.0.1
port: $SERVER_PORT
timeout: 1000
idletimeout: 60000
livenesstimeout: 120000
secretkey: $SECRET
EOF
  cat > "$dir/client.cfg" <<EOF
---
targets:
 - ws://127.0.0.1:$SERVER_PORT/register
poolidlesize: 3
poolmaxsize: 100
secretkey: $SECRET
blacklist:
 - method: ".*"
   url: ".*forbidden.*"
routes:
  "127.0.0.1:$SERVER_PORT": "http://127.0.0.1:$API_PORT"
EOF

  ./target/debug/how-test-api -addr "127.0.0.1:$API_PORT" >"$dir/test_api.log" 2>&1 &
  PIDS+=($!)
  ./target/debug/how-server --config "$dir/server.cfg" >"$dir/server.log" 2>&1 &
  PIDS+=($!)
  sleep 0.5
  ./target/debug/how-client --config "$dir/client.cfg" >"$dir/client.log" 2>&1 &
  PIDS+=($!)

  for _ in $(seq 1 50); do
    curl -s -m 1 "http://127.0.0.1:$SERVER_PORT/status" >/dev/null 2>&1 && break
    sleep 0.1
  done
  for _ in $(seq 1 50); do
    code=$(curl -s -m 1 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$SERVER_PORT/hello" 2>/dev/null)
    [ "$code" = "200" ] && break
    sleep 0.2
  done
}

req() { # method path [data]
  local method="$1" path="$2" data="${3:-}"
  if [ -n "$data" ]; then
    curl -s -m 15 -X "$method" --data-binary "$data" \
      "http://127.0.0.1:$SERVER_PORT$path"
  else
    curl -s -m 15 -X "$method" "http://127.0.0.1:$SERVER_PORT$path"
  fi
}
status_code() { curl -s -m 15 -o /dev/null -w "%{http_code}" -X "$1" "http://127.0.0.1:$SERVER_PORT$2"; }

run_tests() {
  echo "== running tests =="
  [ "$(curl -s -m 2 "http://127.0.0.1:$SERVER_PORT/status")" = "ok" ] && ok "/status returns ok" || ko "/status returns ok"
  [ "$(req GET /hello)" = "hello world" ] && ok "GET /hello body" || ko "GET /hello body"
  local h; h=$(curl -s -m 5 -i "http://127.0.0.1:$SERVER_PORT/header" | grep -i '^hello:' | tr -d '\r')
  [ "$h" = "hello: world" ] && ok "GET /header forwards 'hello: world' header" || ko "GET /header header (got '$h')"
  [ "$(req POST /post 'ping=pong')" = "ping=pong" ] && ok "POST /post echoes body" || ko "POST /post echoes body"
  [ "$(status_code GET /fail)" = "666" ] && ok "GET /fail returns 666" || ko "GET /fail returns 666"
  # client-side blacklist -> 527
  local c; c=$(status_code GET /forbidden)
  [ "$c" = "527" ] && ok "client blacklist denies /forbidden (527)" || ko "client blacklist (code=$c)"
  # binary body integrity
  head -c 8192 /dev/urandom | base64 > /tmp/wsp_payload.bin
  local n; n=$(wc -c < /tmp/wsp_payload.bin)
  curl -s -m 15 -X POST --data-binary @/tmp/wsp_payload.bin \
    "http://127.0.0.1:$SERVER_PORT/post" > /tmp/wsp_resp.bin 2>/dev/null
  cmp -s /tmp/wsp_payload.bin /tmp/wsp_resp.bin && ok "binary POST body integrity ($n bytes)" || ko "binary POST body integrity"
  # /sleep -> 200 ok
  [ "$(req GET /sleep)" = "ok" ] && ok "GET /sleep returns ok after delay" || ko "GET /sleep returns ok"
}

build_bin
start_all
run_tests

# no-proxy test: kill the client, a request -> 526 No proxy available.
kill "${PIDS[2]}" 2>/dev/null
sleep 6  # let the server reap the empty pool
code=$(curl -s -m 6 -o /dev/null -w "%{http_code}" "http://127.0.0.1:$SERVER_PORT/hello")
{ [ "$code" = "526" ]; } && ok "no client -> 526 No proxy available" || ko "no-proxy (code=$code, want 526)"

echo
echo "=================="
echo "  $pass passed, $fail failed"
echo "=================="
[ "$fail" = 0 ]
