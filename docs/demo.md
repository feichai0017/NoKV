# Cluster Demo

One-command demo of the full 333 HA topology (3 meta-root + 3 coordinator +
3 store + 1 fsmeta gateway).

## One-shot startup

```bash
# Pull image + start every service + run bootstrap once
docker compose up -d
docker compose logs -f
```

`docker compose down -v` wipes the data volumes too.

## Exposed ports (all bound to 127.0.0.1)

Symmetric port blocks so the three replicas of each role land on three
consecutive numbers — easy to remember, easy to script against.

| Service | Port | Purpose |
|---|---|---|
| Meta-root-1 gRPC | `2380` | `nokv eunomia-audit --root-peer 1=127.0.0.1:2380 ...` |
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
| FSMeta gRPC | `8090` | filesystem metadata service |
| FSMeta expvar | `9400` | `/debug/vars` JSON |

### Why are meta-root gRPC ports exposed?

Meta-root (`2380/2381/2382`) is exposed so host-side tools like
`nokv eunomia-audit` and `nokv-config` can query rooted state directly for
debugging.

**For production, don't expose meta-root publicly.** The gRPC API accepts
`ApplyTenure` and `ApplyHandover` which are lease-gated but still
structurally sensitive. To opt out, delete the `ports:` block under
`meta-root-1`, `meta-root-2`, `meta-root-3` in `docker-compose.yml`. The
cluster keeps working since coordinator and fsmeta dial meta-root over the
docker network, not through host ports.

Same applies to coordinator gRPC (`2390/2391/2392`): convenient for
host-side client experiments, don't expose publicly.

### Live audit from the host

```bash
# Project rooted state through the Eunomia audit vocabulary
nokv eunomia-audit \
  --root-peer 1=127.0.0.1:2380 \
  --root-peer 2=127.0.0.1:2381 \
  --root-peer 3=127.0.0.1:2382
```

Or point any tool using `raft_config.example.json` at `--scope host` to
pick up `127.0.0.1:2379,2390,2391` and `2380/2381/2382` from the config
file automatically — no extra flags.

## Failure drills

Run them straight from the terminal:

- **Stop the active coordinator** — `docker stop nokv-coordinator-1`. The
  Eunomia lease moves to a standby in 1–3 s; watch the era bump on the
  surviving coordinators' `/debug/vars`.
- **Stop the raft leader meta-root** — `docker stop nokv-meta-root-1`. Raft
  election lands on a surviving peer; coord lease may churn through one
  era before settling (~17 s total recovery).
- **Start the stopped container** — `docker start nokv-coordinator-1`. It
  rejoins quietly as a standby.

## Related docs

- [docs/config.md](config.md) — `raft_config.example.json` schema (two-layer
  model: address directory vs bootstrap seed)
- [docs/eunomia-audit.md](eunomia-audit.md) — the Eunomia audit tool
- [docs/coordinator.md](coordinator.md) — Eunomia lease lifecycle
- [docs/rooted_truth.md](rooted_truth.md) — meta-root internals
