# NoKV GCP Benchmark Deployment

This directory deploys NoKV to Google Compute Engine for fsmeta smoke tests and
formal lab-grade benchmarks.

The deployment is intentionally split into two modes:

| Mode | Goal | Default VM shape | Placement | Output |
| --- | --- | --- | --- | --- |
| Smoke | Validate the deployment path end to end | `e2-standard-2` for every role | no compact placement | pass/fail plus a small CSV |
| Distributed smoke | Validate a small distributed-correctness slice with fault injection | `e2-standard-2` for every role | no compact placement | pass/fail plus per-phase CSVs and failure diagnostics |
| Formal benchmark | Produce performance evidence | C4 layout below | same zone, ideally compact or otherwise controlled placement | benchmark CSVs and run metadata |

Smoke results are not performance evidence. They only prove that infra creation,
image pull, startup scripts, NoKV service wiring, benchmark execution, result
copyback, and destroy all work.

Distributed smoke is also not performance evidence. It adds active checks for
meta-root leadership, coordinator grant handoff, store execution restart state,
and a small workload after selected node faults. The fault-smoke path preserves
failed clusters by default so operators can inspect the live system.

## Topology

The default benchmark topology is 11 VMs:

| Role | Count | Plane | Static IPs | Main process | Formal default |
| --- | ---: | --- | --- | --- | --- |
| meta-root | 3 | truth plane | `10.42.0.11-13` | `nokv meta-root` | `c4-standard-2` |
| coordinator | 3 | coordinator/control plane | `10.42.0.21-23` | `nokv coordinator` | `c4-standard-2` |
| store | 3 | execution/storage plane | `10.42.0.31-33` | `nokv serve` | `c4-standard-4-lssd` |
| gateway | 1 | fsmeta API gateway | `10.42.0.41` | `nokv-fsmeta` | `c4-standard-4` |
| loadgen | 1 | benchmark driver | `10.42.0.51` | benchmark container | `c4-standard-4` |

There are 9 core NoKV control/data-plane service processes: 3 meta-root, 3
coordinator, and 3 store. Gateway and loadgen are separate VMs by design, but
they are not counted as core cluster nodes.

## Architecture Notes

- meta-root is the truth plane. It has its own 3-peer raft group and should not
  be colocated with coordinator VMs in formal evidence runs.
- coordinator is the grant/control plane. It has 3 grant candidates with duties
  `alloc_id,tso,region_lookup`.
- store is the execution/storage plane. Formal runs use local SSD machine types
  for stores because store latency and disk behavior are part of the evidence.
- gateway runs only `nokv-fsmeta`. It is isolated so API/cache CPU does not
  distort store or coordinator measurements.
- loadgen runs in the same zone as the cluster. Local workstation latency from
  Sydney, SSH, or IAP does not enter the benchmark data path.
- VM IPs are static inside `10.42.0.0/24` because raft/store/fsmeta configs are
  generated before startup.
- Runtime and benchmark images are pushed to Artifact Registry and then pinned
  by digest in `deploy/gcp/.last-image.env`. VMs do not pull mutable tags.
- `deploy/gcp/generated/eunomia-signing-key.txt` is created by
  `create-cluster.sh`; `run-fsmeta-benchmark.sh` passes that same key to
  loadgen. If the benchmark uses a different key, coordinator evidence
  validation fails with `authority evidence signature mismatch`.

## Defaults

| Setting | Default |
| --- | --- |
| Project | `nokv-benchmark` |
| Region | `australia-southeast2` |
| Zone | `australia-southeast2-b` |
| Artifact Registry | `australia-southeast2-docker.pkg.dev/nokv-benchmark/nokv-lab` |
| OS image | `debian-12` from `debian-cloud`, `X86_64` |
| External VM IPv4 | disabled |
| Outbound VM access | Cloud NAT |
| Operator SSH/SCP | IAP tunnel |
| Boot disk | 30 GB |
| Compact placement | disabled by default |

Region affects benchmark evidence through in-cloud placement, VM availability,
quota, and potentially cross-zone/network characteristics. The operator's local
latency is not part of benchmark measurement because loadgen is inside the same
zone as the cluster.

## Cold Start Windows

The scripts use bounded readiness windows instead of relying on one fixed sleep.
These windows are wall-clock guards and are not benchmark measurement time.

| Variable | Default | Purpose |
| --- | ---: | --- |
| `GCP_BENCHMARK_START_GRACE_SECONDS` | 60 | Wait after VM creation before SSHing to loadgen |
| smoke override | 90 | e2 smoke allows more time for apt, Docker, and image pull |
| `GCP_TRANSPORT_RETRIES` | 30 | IAP/SSH/SCP retry attempts |
| `GCP_TRANSPORT_RETRY_DELAY_SECONDS` | 10 | Delay between transport retries |
| `GCP_LOADGEN_DOCKER_READY_TIMEOUT_SECONDS` | 600 | Wait for Docker on loadgen |
| `GCP_SERVICE_READY_TIMEOUT_SECONDS` | 900 | Wait for meta-root, coordinator, store, and gateway ports |
| `GCP_SERVICE_READY_RETRY_DELAY_SECONDS` | 2 | Delay between service port probes |
| `NOKV_META_ROOT_TICK_INTERVAL` | `1000ms` | Raft tick interval for the 3-peer meta-root quorum |
| `NOKV_FSMETA_STABILIZE_SECONDS` | profile-specific | Wait after ports are ready for raft leaders and grants |

Meta-root Raft timing is intentionally conservative for GCP smoke. The
replicated root uses `ElectionTick=10`, `HeartbeatTick=1`, and a `1000ms`
default tick interval. The GCP deployment still passes
`--tick-interval=$NOKV_META_ROOT_TICK_INTERVAL` explicitly so smoke runs can
raise the interval without rebuilding images. That makes the default
production-like GCP election timeout baseline 10 seconds.

Benchmark profile stabilization defaults:

| Profile | Stabilize | Benchmark timeout | Intended use |
| --- | ---: | --- | --- |
| smoke | 15s | 5m | deployment path check |
| median | 60s | 25m | normal formal slice |
| long | 120s | 120m | longer evidence run |

## Prerequisites

Local tools:

```bash
gcloud version
docker version
docker buildx version
git --version
```

GCP requirements:

- Billing enabled on `GCP_PROJECT`.
- `gcloud auth login` and permission to create Compute Engine resources.
- Docker authentication to Artifact Registry; `build-push-images.sh` runs
  `gcloud auth configure-docker`.
- Enough regional quota for the selected machine families. Smoke uses 11
  `e2-standard-2` VMs, so it needs 22 E2 vCPUs. The formal C4 plan needs more
  C4 quota and C4 local-SSD availability.

Check quota before formal runs:

```bash
gcloud compute regions describe australia-southeast2 \
  --project=nokv-benchmark \
  --format="table(quotas.metric,quotas.limit,quotas.usage)"
```

## Low-Cost Smoke

Build and push immutable images:

```bash
deploy/gcp/build-push-images.sh
```

Run smoke and always destroy resources:

```bash
deploy/gcp/smoke-and-destroy.sh
```

When no config file argument is passed, `smoke-and-destroy.sh` still reads
`deploy/gcp/config.env` for project/region overrides if that file exists, but it
overrides all VM roles to `e2-standard-2` and disables compact placement. This
keeps smoke cheap and avoids C4 quota/capacity blocking deployment validation.

Use a custom smoke size only when needed:

```bash
GCP_SMOKE_MACHINE_TYPE=e2-standard-4 deploy/gcp/smoke-and-destroy.sh
```

Passing an explicit config file to `smoke-and-destroy.sh` means "use that config
as written". Do that only when you intentionally want a non-default smoke run.

## Distributed Smoke

Run the distributed smoke path with success cleanup and failure preservation:

```bash
deploy/gcp/distributed-smoke-and-destroy.sh
```

When no config file argument is passed, `distributed-smoke-and-destroy.sh` uses
the same low-cost shape as normal smoke: 11 `e2-standard-2` VMs, no compact
placement, no external IPv4, Cloud NAT for pulls, and IAP for operator SSH/SCP.
The wrapper destroys resources after a successful run by default. On failure it
keeps the cluster and prints the matching `destroy-cluster.sh` command so the
same VMs can be inspected before cleanup.

The distributed smoke sequence is intentionally small:

1. Wait for all service and metrics ports.
2. Assert exactly one meta-root leader through `/debug/vars`.
3. Assert exactly one active coordinator grant holder and zero Eunomia guarantee
   violations through `/debug/vars`.
4. Assert each live store reports `execution -json` restart state `ready`, with
   non-zero region/raft-group counts and no missing raft pointers.
5. Run a baseline `mixed` fsmeta smoke workload.
6. Stop the current meta-root leader, wait for election, rerun all control-plane
   assertions with the stopped meta-root skipped, and run `mixed`.
7. Restore meta-root, wait for all services, and rerun all control-plane
   assertions.
8. Stop the current coordinator grant holder, wait for grant handoff, rerun all
   control-plane assertions with the stopped coordinator skipped, and run
   `mixed,negative-lookup` using only live coordinator addresses for benchmark
   bootstrap.
9. Restore the coordinator, wait for all services, and rerun all control-plane
   assertions.
10. Stop one store, wait for raft failover, rerun all control-plane assertions
    with the stopped store skipped, and run
    `mixed,hotspot-fanin,negative-lookup`.
11. Restore the store, wait for all services, and rerun all control-plane
    assertions.
12. Restart gateway, wait for all services, rerun all control-plane assertions,
    run `negative-lookup`, then run final control-plane assertions and `mixed`.

This is a distributed-correctness smoke, not a complete Jepsen-style validation.
It covers the current deployment's expected failure handling path with bounded
single-node faults. It does not prove arbitrary partitions, clock faults,
Byzantine behavior, correlated VM failures, or long-running compaction and GC
interactions.

Useful knobs:

```bash
NOKV_META_ROOT_TICK_INTERVAL=1500ms deploy/gcp/distributed-smoke-and-destroy.sh
NOKV_DISTRIBUTED_SMOKE_STORE_FAULT_ID=2 deploy/gcp/distributed-smoke-and-destroy.sh
NOKV_DISTRIBUTED_SMOKE_BENCH_STABILIZE_SECONDS=15 deploy/gcp/distributed-smoke-and-destroy.sh
NOKV_DISTRIBUTED_SMOKE_KEEP_CLUSTER=true deploy/gcp/distributed-smoke-and-destroy.sh
NOKV_DISTRIBUTED_SMOKE_DESTROY_ON_FAILURE=true deploy/gcp/distributed-smoke-and-destroy.sh
NOKV_DISTRIBUTED_SMOKE_LOG_TAIL=1200 deploy/gcp/distributed-smoke-and-destroy.sh
```

To run against an existing cluster without auto-destroy:

```bash
deploy/gcp/distributed-smoke.sh deploy/gcp/config.env
```

If distributed smoke fails, `distributed-smoke.sh` collects local diagnostics
under `deploy/gcp/results/<run_id>/diagnostics/` before exit. Each node snapshot
includes Docker state, container logs, the service `/debug/vars` endpoint when
available, and startup-script journal output. The destroy wrapper preserves the
cluster on failure unless `NOKV_DISTRIBUTED_SMOKE_DESTROY_ON_FAILURE=true` is
set.

## Formal Benchmark

Create a config file:

```bash
cp deploy/gcp/config.env.example deploy/gcp/config.env
```

Edit at least:

- `GCP_PROJECT`
- `GCP_REGION`
- `GCP_ZONE`
- role machine types
- `GCP_USE_COMPACT_PLACEMENT`

Recommended formal baseline:

```bash
GCP_META_ROOT_MACHINE_TYPE=c4-standard-2
GCP_COORDINATOR_MACHINE_TYPE=c4-standard-2
GCP_STORE_MACHINE_TYPE=c4-standard-4-lssd
GCP_GATEWAY_MACHINE_TYPE=c4-standard-4
GCP_LOADGEN_MACHINE_TYPE=c4-standard-4
GCP_USE_COMPACT_PLACEMENT=false
```

Set `GCP_USE_COMPACT_PLACEMENT=true` only after checking capacity. In previous
attempts, compact placement could leave VMs waiting in `STAGING`; that burns
time without producing useful benchmark data. For formal evidence, compact or
otherwise controlled placement is useful, but only if capacity is available.

Run:

```bash
deploy/gcp/build-push-images.sh deploy/gcp/config.env
deploy/gcp/create-cluster.sh deploy/gcp/config.env
NOKV_FSMETA_PROFILE=median deploy/gcp/run-fsmeta-benchmark.sh deploy/gcp/config.env
deploy/gcp/destroy-cluster.sh deploy/gcp/config.env --delete-infra
```

For repeated runs on an already warm cluster:

```bash
GCP_BENCHMARK_START_GRACE_SECONDS=0 \
NOKV_FSMETA_PROFILE=median \
deploy/gcp/run-fsmeta-benchmark.sh deploy/gcp/config.env
```

Always destroy manually after formal runs:

```bash
deploy/gcp/destroy-cluster.sh deploy/gcp/config.env --delete-infra
```

## Artifacts

Ignored local artifacts:

| Path | Meaning |
| --- | --- |
| `deploy/gcp/.last-image.env` | pinned runtime and benchmark image digests |
| `deploy/gcp/generated/raft_config.gcp.json` | generated static cluster config |
| `deploy/gcp/generated/eunomia-signing-key.txt` | signing key shared by cluster and loadgen |
| `deploy/gcp/generated/startup-*.sh` | generated VM startup scripts |
| `deploy/gcp/results/<run_id>/...` | copied benchmark CSVs |
| `deploy/gcp/results/<run_id>/diagnostics/...` | distributed-smoke failure diagnostics |

Inspect the latest smoke CSV:

```bash
find deploy/gcp/results -name 'fsmeta_*.csv' -print | sort | tail -1
```

## Operational Checks

List benchmark resources:

```bash
gcloud compute instances list \
  --project=nokv-benchmark \
  --filter="name~'nokv-bench'" \
  --format="table(name,zone,status,machineType.basename(),networkInterfaces[0].networkIP)"
```

Confirm no leftover resources after smoke:

```bash
gcloud compute instances list --project=nokv-benchmark --filter="name~'nokv-bench'"
gcloud compute networks list --project=nokv-benchmark --filter="name~'nokv-bench'"
gcloud compute routers list --project=nokv-benchmark --regions=australia-southeast2 --filter="name~'nokv-bench'"
```

Inspect a node:

```bash
gcloud compute ssh nokv-bench-gateway-1 \
  --project=nokv-benchmark \
  --zone=australia-southeast2-b \
  --tunnel-through-iap \
  --command='sudo docker ps -a; sudo docker logs --tail=120 nokv-fsmeta 2>&1'
```

Useful container names:

| VM role | Container |
| --- | --- |
| meta-root | `nokv-meta-root` |
| coordinator | `nokv-coordinator` |
| store | `nokv-store` |
| gateway | `nokv-fsmeta` |

Startup script logs:

```bash
sudo journalctl -u google-startup-scripts.service -n 120 --no-pager
```

## Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| IAP SSH says `Failed to lookup instance` right after VM create | GCE/IAP metadata propagation lag | Current scripts retry; increase `GCP_TRANSPORT_RETRIES` if needed |
| VM stuck in `STAGING` with compact placement | zone capacity cannot satisfy collocation | Disable compact for smoke; choose another zone or wait/request capacity for formal |
| E2 rejects `onHostMaintenance=TERMINATE` | scheduling flags are valid only for compact/special cases | Current scripts apply those flags only when compact placement is enabled |
| store bootstrap gets `/mnt/nokv/store-* permission denied` | no local SSD path was root-owned on e2 | Current startup script runs `chmod 0777 /mnt/nokv` for non-local-SSD smoke |
| gateway logs `dirpage-cache ... permission denied` | cache dirs were root-owned | Current startup script chmods gateway cache dirs |
| benchmark fails with `authority evidence signature mismatch` | loadgen signing key differs from cluster key | Keep `deploy/gcp/generated/eunomia-signing-key.txt` from `create-cluster.sh`; current run script passes it to loadgen |
| benchmark copies results but test failed | remote command return code was not propagated | Current retry wrapper returns non-zero for non-transport failures |
| `nokv execution -json` panics about missing Eunomia key | runtime CLI containers need the same key as services | Distributed smoke passes `NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY` into the runtime CLI probe |
| distributed smoke fails after stopping meta-root leader and remaining peers report repeated elections or `leader_id=0` | meta-root raft did not stabilize inside the smoke window, commonly because the tick interval is too aggressive for low-cost VM jitter | keep the default `NOKV_META_ROOT_TICK_INTERVAL=1000ms` or raise it, then inspect meta-root logs for repeated split votes |
| distributed smoke fails in `meta-root-leader-down` with `stale witness era` | meta-root re-elected, but coordinator/client witness era state became stale after the leader fault | inspect coordinator TSO/grant logs, root event propagation, and client witness-era refresh around the leader stop; this is not an image pull or bootstrap failure |
| readiness times out on `10.42.0.41:8090` | gateway failed or coordinator dependency failed | inspect `nokv-fsmeta` logs, then coordinator logs |
| readiness times out on `10.42.0.31-33:20160` | store bootstrap or serve failed | inspect `nokv-store` logs and startup logs |
| C4 formal run fails quota check | region lacks C4 quota | request quota or choose a region/zone with enough C4 quota and capacity |

## Cost Controls

- Prefer `smoke-and-destroy.sh` until the deployment path is stable.
- Smoke defaults to e2, no compact placement, no external IPv4.
- `smoke-and-destroy.sh` has an exit trap and calls `destroy-cluster.sh
  --delete-infra` even when smoke fails.
- `distributed-smoke-and-destroy.sh` destroys resources after success, but keeps
  failed clusters by default for fault diagnosis. Destroy them manually after
  collecting evidence, or set `NOKV_DISTRIBUTED_SMOKE_DESTROY_ON_FAILURE=true`
  when live debugging is not needed.
- Formal runs do not auto-destroy because humans may need to inspect logs and
  run multiple profiles. Destroy manually as soon as evidence is collected.
- Avoid compact placement during smoke. Capacity waits can cost wall-clock time
  without improving deployment-path validation.
- No external VM IPv4 is the default. This avoids the 8-address quota limit and
  reduces exposed surface area. VMs pull images through Cloud NAT and operators
  connect through IAP.

## Handoff Notes

- Last validated local smoke: 2026-05-13 in `australia-southeast2-b`, 11
  `e2-standard-2` VMs, no compact placement, no external IPv4, result CSV had
  31 rows and 0 operation errors.
- Runtime image digest from that run:
  `australia-southeast2-docker.pkg.dev/nokv-benchmark/nokv-lab/nokv@sha256:8dfab48421b49598a4fa4e3d961893d78ead5e9cd4071bacd719df56414107f9`
- Benchmark image digest from that run:
  `australia-southeast2-docker.pkg.dev/nokv-benchmark/nokv-lab/nokv-bench@sha256:b8606596e810b13728816964c23414e71411a543581ecab9fac4eb0ca4982543`
- First distributed smoke attempt on 2026-05-13 passed baseline deployment
  checks and `mixed`, then failed after stopping the meta-root leader. The two
  remaining meta-root peers repeatedly entered elections and reported
  `leader_id=0`; logs showed split votes rather than a new leader. Resources
  were destroyed after the failure.
- Distributed Peras fault-smoke handoff, 2026-05-13 UTC:
  - Branch: `gcp-benchmark` at `d3fa59ba` (`feat(deploy): enable Peras smoke
    defaults`).
  - Peras was intentionally enabled by default for both local Compose and GCP:
    store witnesses use `--peras-witness=true`; fsmeta uses
    `--peras-visible-commit=true`, holder `fsmeta-holder-1`, witness stores
    `1,2,3`, and witness quorum `2`.
  - Runtime image:
    `australia-southeast2-docker.pkg.dev/nokv-benchmark/nokv-lab/nokv@sha256:9a135af52837cb53b64780a3fe4c20e2b6a57c96ef9de2edce242d819aa340d3`.
  - Benchmark image:
    `australia-southeast2-docker.pkg.dev/nokv-benchmark/nokv-lab/nokv-bench@sha256:84f779c683c7c1a06debd8ddd3953d869b2efd25da90464ae15b418b9709a164`.
  - Before this run, a previous warm 11-VM smoke cluster was still running. It
    was destroyed first with `destroy-cluster.sh --delete-infra`, including
    instances, firewalls, router/NAT, subnet, and network.
  - The fresh run used `distributed-smoke-and-destroy.sh` with no explicit
    config, so it created 11 `e2-standard-2` VMs in
    `australia-southeast2-b`, no compact placement, no external IPv4, Cloud NAT
    for pulls, and IAP for operator SSH/SCP.
  - Initial GCP deployment checks passed: loadgen pulled the digest-pinned
    runtime and benchmark images, all service and metrics ports became ready,
    meta-root had exactly one leader, coordinator grant had exactly one holder
    with zero Eunomia guarantee violations, and all three stores reported
    `execution -json` restart state `ready` with 35 regions and 35 raft groups.
  - Baseline `mixed` workload passed. The copied CSV is
    `deploy/gcp/results/distributed-20260513T151915Z/distributed-20260513T151915Z/baseline/fsmeta_baseline_20260513T152117Z.csv`
    and contains 27 operation rows with zero operation errors.
  - The meta-root leader before fault injection was `meta-root-2`. After
    stopping it, the first post-fault assertion briefly observed two live peers
    self-reporting as leaders: `meta-root-1` with `leader_id=1` and
    `meta-root-3` with `leader_id=3`. A retry 2 seconds later converged to one
    live leader, `meta-root-1`.
  - The next workload, `meta-root-leader-down`, failed after 96.94 seconds with:
    `run mixed: rpc error: code = OutOfRange desc = nokv: stale_epoch:
    coordinator client: stale witness era: tso era=11 retired_floor=21`.
    No `meta-root-leader-down` CSV was produced; only the successful baseline
    CSV was copied back before cleanup.
  - The destroy trap ran after failure and deleted all new smoke VMs and network
    resources. A final `gcloud compute instances/networks/firewall-rules list`
    check for `nokv-bench` returned no residual resources.
  - Suggested next investigation: collect coordinator and gateway logs around
    the `meta-root-2` stop/re-election window before another run. The failure
    is after successful deployment and baseline traffic, so focus on
    TSO/witness-era advancement, root event propagation, and client refresh
    behavior under meta-root leader replacement.
- Formal C4 benchmark has not been validated yet in this project. Re-check C4
  quota and zone capacity before running it.
