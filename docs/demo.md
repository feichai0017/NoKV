# Cluster Demo & Live Dashboard

One-command demo of the full 333 HA topology (3 meta-root + 3 coordinator +
3 store + 1 Redis gateway) with a live browser dashboard.

## One-shot startup

```bash
# Build images + start every service + run bootstrap once
docker compose up -d --build

# Wait a few seconds, then verify
redis-cli -p 6380 ping                    # → PONG
redis-cli -p 6380 set demo hello
redis-cli -p 6380 get demo                # → hello
```

That's it. `docker compose down -v` wipes data volumes too.

## Exposed ports (all bound to 127.0.0.1)

| Service | Port | Purpose |
|---|---|---|
| Redis gateway | `6380` | RESP protocol — `redis-cli -p 6380 ...` |
| Redis expvar | `9300` | `/debug/vars` JSON |
| Meta-root-1 gRPC | `2380` | `nokv ccc-audit --root-peer 1=127.0.0.1:2380 ...` |
| Meta-root-2 gRPC | `2381` | |
| Meta-root-3 gRPC | `2382` | |
| Meta-root-1 expvar | `9380` | `/debug/vars` JSON |
| Meta-root-2 expvar | `9381` | |
| Meta-root-3 expvar | `9382` | |
| Coordinator-1 gRPC | `2379` | |
| Coordinator-2 gRPC | `2390` | |
| Coordinator-3 gRPC | `2391` | |
| Coordinator-1 expvar | `9100` | |
| Coordinator-2 expvar | `9101` | |
| Coordinator-3 expvar | `9102` | |
| Store-1 expvar | `9200` | |
| Store-2 expvar | `9201` | |
| Store-3 expvar | `9202` | |

### Live audit from the host

```bash
# Project rooted state through the CCC audit vocabulary
nokv ccc-audit \
  --root-peer 1=127.0.0.1:2380 \
  --root-peer 2=127.0.0.1:2381 \
  --root-peer 3=127.0.0.1:2382
```

Or point any tool using `raft_config.example.json` at `--scope host`
to pick up `127.0.0.1:2379,2390,2391` and `2380/2381/2382` from the
config file automatically — no extra flags.

## Live dashboard

There is a single-page dashboard at `scripts/demo/dashboard.html` that polls
every expvar endpoint (10 total) every 1.5 s and renders a live view of the
three planes:

- **Truth plane** — which meta-root is raft leader, committed index, allocator
  fences, descriptor / pending-change counts.
- **Control plane** — which coordinator currently holds the CCC lease, lease
  generation, root lag, degraded mode, active vs standby tag on every coord.
- **Execution plane** — stores heap usage, goroutine count.
- **Gateway** — Redis expvar counters.

### Run it

```bash
scripts/demo/serve-dashboard.sh    # → http://localhost:8080/dashboard.html
```

Opens a tiny Python static server at `http://localhost:8080`. The dashboard
page fetches the 10 expvar endpoints directly on localhost. If your browser
aggressively blocks cross-port localhost fetches, use a Chromium-based
browser or launch Chrome with `--disable-web-security` for the demo.

Alternative: open `scripts/demo/dashboard.html` directly via `file://` —
works on Safari and most browsers for `localhost:*` targets.

### What a healthy cluster looks like

- exactly one meta-root card shows the green **raft leader** badge
- exactly one coordinator card shows the purple **lease holder** badge and a
  cert_generation that stays stable unless you kill it
- the status line at the top reads "all 10 services reachable"
- all stores / gateway show non-zero uptime

### Failure drills

Demo-friendly things to try while the dashboard is open:

```bash
# Coordinator failover (lease takeover in ~1-3s)
docker stop nokv-coordinator-1
# Watch a different coordinator grab the lease, cert_generation increment

docker start nokv-coordinator-1
# Returning coord re-joins as standby

# Meta-root leader failover (raft election + lease takeover, ~17s)
docker stop nokv-meta-root-1
# Watch the raft leader badge move to another peer,
# then lease churn briefly, then settle.
```

## Public demo via Cloudflare Tunnel

The tunnel config in `docker-compose.yml` (commented) points cloudflared at
the redis port + expvar ports. To go live publicly:

1. `cloudflared tunnel create nokv-demo`
2. `export CLOUDFLARE_TUNNEL_TOKEN=$(cloudflared tunnel token ...)`
3. Uncomment the `cloudflared` service block
4. `docker compose up -d cloudflared`

Remember: exposing raw `/debug/vars` leaks internal state. For public access
put an nginx proxy in front that whitelists specific fields, or gate access
behind Cloudflare Access.

## Related docs

- [docs/config.md](config.md) — `raft_config.example.json` schema (two-layer
  model: address directory vs bootstrap seed)
- [docs/ccc-audit.md](ccc-audit.md) — the CCC audit tool behind the dashboard's
  "closure witness" row
- [docs/coordinator.md](coordinator.md) — CCC lease lifecycle
- [docs/rooted_truth.md](rooted_truth.md) — meta-root internals
