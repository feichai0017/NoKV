#!/usr/bin/env bash
# Serves scripts/demo/dashboard.html on http://localhost:18080 with
# small proxy endpoints so the page can issue redis-cli commands and
# trigger docker stop/start for failover drills.
#
# Bound to 127.0.0.1 only. Don't expose this to an untrusted network
# without an authenticated upstream (cloudflared + Access, nginx + basic
# auth, etc.) — /api/redis and /api/docker run commands against the host.
set -euo pipefail

PORT="${1:-18080}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

cat <<EOF
NoKV dashboard will be served at http://localhost:${PORT}/dashboard.html

Make sure the cluster is running first:
  docker compose up -d --build

Dashboard endpoints served by this process:
  GET  /dashboard.html                       static page
  GET  /api/expvar/<port>                   localhost:<port>/debug/vars proxy
  POST /api/redis   { "cmd": "...", "port": 6380 }
  POST /api/docker/<stop|start|restart>/<nokv-*>

Press Ctrl-C to stop.
EOF

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

exec python3 "$SCRIPT_DIR/dashboard_server.py" "$PORT"
