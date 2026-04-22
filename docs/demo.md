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

Using symmetric port blocks so the three replicas of each role land on three
consecutive numbers — easy to remember, easy to script against.

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
| Coordinator-1 gRPC | `2390` | |
| Coordinator-2 gRPC | `2391` | |
| Coordinator-3 gRPC | `2392` | |
| Coordinator-1 expvar | `9100` | |
| Coordinator-2 expvar | `9101` | |
| Coordinator-3 expvar | `9102` | |
| Store-1 expvar | `9200` | |
| Store-2 expvar | `9201` | |
| Store-3 expvar | `9202` | |

### Why are meta-root gRPC ports exposed?

Meta-root (`2380/2381/2382`) is exposed so host-side tools like
`nokv ccc-audit` and `nokv-config` can query rooted state directly for
debugging. All `/debug/vars` endpoints also expose the meta-root's state
summary (leader, committed index, generation) for the dashboard.

**For production, don't expose meta-root publicly.** The gRPC API accepts
`ApplyCoordinatorLease` and `ApplyCoordinatorClosure` which are
lease-gated but still structurally sensitive. To opt out, delete the
`ports:` block under `meta-root-1`, `meta-root-2`, `meta-root-3` in
`docker-compose.yml` — the dashboard loses the "Truth plane" cards (they
become "unreachable") but the cluster keeps working since coordinator and
`nokv-redis` dial meta-root over the docker network, not through host
ports.

Same applies to coordinator gRPC (`2390/2391/2392`): convenient for
host-side client experiments, don't expose publicly.

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
scripts/demo/serve-dashboard.sh    # → http://localhost:18080/dashboard.html
```

Under the hood this runs `scripts/demo/dashboard_server.py`, a small Python
HTTP server with three routes:

| Route | Purpose |
|---|---|
| `GET /dashboard.html` | the static page |
| `POST /api/redis`    | `{"cmd": "..."}` → runs `redis-cli -p 6380 <cmd>` and returns `{stdout, stderr, returncode}` |
| `POST /api/docker/<stop\|start\|restart>/<nokv-*>` | wraps `docker <action> <container>` for the failover buttons |

The expvar endpoints themselves now send `Access-Control-Allow-Origin: *`
so the dashboard page can fetch them cross-origin without any proxying.

**Security**: bound to `127.0.0.1` only. `/api/redis` runs arbitrary Redis
commands and `/api/docker` stops/starts containers whose name starts with
`nokv-`. Do not expose this dashboard port publicly without an
authenticated tunnel (Cloudflare Access, nginx + basic auth).

### What you see on the page

- **Top:** cluster-wide status pill ("10/10 reachable · HH:MM:SS")
- **Topology diagram:** 3 truth-plane meta-root peers ↔ 3 control-plane
  coordinators ↔ 3 execution-plane stores, with the raft leader highlighted
  in blue, the CCC lease holder in purple, and pulsing lines showing the
  active control-flow edge (lease holder → raft leader, holder → stores,
  gateway → holder).
- **Per-service cards:** one card per service showing leader state, lease
  generation, committed index, allocator fences, heap stats.
- **Event timeline:** auto-populated from expvar diffs — lease handoffs,
  raft elections, descriptor-count changes, node up/down transitions. Use
  this to watch the CCC lifecycle live.
- **Failure drills:** one button per meta-root / coordinator container to
  stop or start it without leaving the browser.
- **Redis terminal:** type commands like `SET demo "hello"`, `GET demo`,
  `MGET a b c`, `DBSIZE` and see the raw `redis-cli` output. Each command
  also lands in the event timeline.

### Healthy cluster invariants

- exactly one meta-root card shows the blue **raft leader** badge
- exactly one coordinator card shows the purple **lease holder** badge and
  a `cert_generation` that stays stable unless you kill something
- topology diagram: one blue circle on the left column, one purple on
  the middle column
- event timeline scrolls slowly with committed-index bumps under load

### Failure drills — interactive

The dashboard's **Failure drills** panel has buttons to stop/start each
meta-root and coordinator container directly. Click one, watch:

- **Stop the active coordinator** → lease held-by-self flag flips off at
  the killed peer, a different coord picks up the lease in 1–3 s, the
  purple ring in the topology diagram moves, and the event timeline logs
  the handoff.
- **Stop the raft leader meta-root** → raft election visible as the blue
  ring disappears briefly and lands on a surviving peer; coord lease may
  churn through one generation before settling (~17 s total recovery).
- **Start the stopped container** → it rejoins quietly as a standby.

The same drills run from the terminal if you prefer: `docker stop
nokv-coordinator-1`, `docker stop nokv-meta-root-1`, etc.

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
