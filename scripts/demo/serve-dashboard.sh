#!/usr/bin/env bash
# Serves scripts/demo/dashboard.html on http://localhost:8080.
# The dashboard polls expvar endpoints on localhost:9xxx exposed by the
# docker-compose cluster.
set -euo pipefail

PORT="${1:-8080}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

cat <<EOF
NoKV dashboard will be served at http://localhost:${PORT}/dashboard.html

Make sure the cluster is running first:
  docker compose up -d --build

Then open the URL in your browser. Press Ctrl-C to stop.
EOF

cd "$SCRIPT_DIR"
if command -v python3 >/dev/null 2>&1; then
  exec python3 -m http.server "$PORT" --bind 127.0.0.1
elif command -v python >/dev/null 2>&1; then
  exec python -m SimpleHTTPServer "$PORT"
else
  echo "no python found — open scripts/demo/dashboard.html directly in your browser" >&2
  exit 1
fi
