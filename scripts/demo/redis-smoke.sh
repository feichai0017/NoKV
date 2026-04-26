#!/usr/bin/env bash
# Host-side smoke test for the Docker Compose Redis-compatible gateway.
set -euo pipefail
LC_ALL=C

ADDR="127.0.0.1:6380"
TIMEOUT=60
KEY=""

usage() {
  cat <<'EOF'
Usage: scripts/demo/redis-smoke.sh [options]

Options:
  --addr HOST:PORT       Redis gateway address (default: 127.0.0.1:6380)
  --timeout SECONDS      Readiness and command timeout (default: 60)
  --key KEY              Key prefix to write/read (default: unique smoke key)
  -h, --help             Show this help
EOF
}

log() {
  echo "redis-smoke: $*"
}

die() {
  echo "redis-smoke: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --addr)
      ADDR="${2:-}"
      shift 2
      ;;
    --timeout)
      TIMEOUT="${2:-}"
      shift 2
      ;;
    --key)
      KEY="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

[[ "$ADDR" == *:* ]] || die "invalid --addr $ADDR"
[[ "$TIMEOUT" =~ ^[0-9]+$ ]] || die "--timeout must be an integer number of seconds"
(( TIMEOUT > 0 )) || die "--timeout must be > 0"

HOST=${ADDR%:*}
PORT=${ADDR##*:}
[[ -n "$HOST" && -n "$PORT" && "$HOST" != "$ADDR" ]] || die "invalid --addr $ADDR"
[[ "$PORT" =~ ^[0-9]+$ ]] || die "invalid port in --addr $ADDR"

if [[ -z "$KEY" ]]; then
  KEY="nokv:smoke:${EPOCHREALTIME:-$(date +%s)}:$$"
fi

log "waiting for $ADDR"
deadline=$((SECONDS + TIMEOUT))
while (( SECONDS < deadline )); do
  if (echo >/dev/tcp/"$HOST"/"$PORT") >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
(( SECONDS < deadline )) || die "timed out waiting for $ADDR"

open_gateway() {
  exec 3<>"/dev/tcp/$HOST/$PORT" || die "connect $ADDR failed"
}

close_gateway() {
  exec 3>&- || true
  exec 3<&- || true
}

send_resp() {
  local arg
  printf '*%d\r\n' "$#" >&3
  for arg in "$@"; do
    printf '$%d\r\n%s\r\n' "${#arg}" "$arg" >&3
  done
}

read_line() {
  local line
  IFS= read -r -t "$TIMEOUT" -u 3 line || die "timed out reading response from $ADDR"
  printf '%s' "${line%$'\r'}"
}

expect_simple() {
  local expected=$1
  local line
  line=$(read_line)
  [[ "$line" == "+$expected" ]] || die "expected +$expected, got $line"
}

read_bulk() {
  local header size data crlf
  header=$(read_line)
  [[ "$header" =~ ^\$-?[0-9]+$ ]] || die "expected bulk string, got $header"
  size=${header#\$}
  (( size >= 0 )) || die "expected value for $KEY, got nil"
  IFS= read -r -N "$size" -t "$TIMEOUT" -u 3 data || die "timed out reading bulk body from $ADDR"
  IFS= read -r -N 2 -t "$TIMEOUT" -u 3 crlf || die "timed out reading bulk terminator from $ADDR"
  [[ "$crlf" == $'\r\n' ]] || die "invalid bulk terminator"
  printf '%s' "$data"
}

key_at() {
  printf '%s:%02d' "$KEY" "$1"
}

value_at() {
  printf 'ok:%s:%02d' "$KEY" "$1"
}

open_gateway
trap close_gateway EXIT

log "connected to $ADDR"
log "PING"
send_resp PING
expect_simple PONG
log "PONG"

log "writing 20 keys with prefix=$KEY"
for i in $(seq 1 20); do
  key=$(key_at "$i")
  value=$(value_at "$i")
  log "SET $key"
  send_resp SET "$key" "$value"
  expect_simple OK
done

log "reading 5 sample keys"
for i in 1 5 10 15 20; do
  key=$(key_at "$i")
  expected=$(value_at "$i")
  log "GET $key"
  send_resp GET "$key"
  actual=$(read_bulk)
  [[ "$actual" == "$expected" ]] || die "GET $key mismatch: expected $expected, got $actual"
  log "OK $key=$actual"
done

log "ok wrote=20 read=5 prefix=$KEY addr=$ADDR"
