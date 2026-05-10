# Falcon: Speculative Replication for fsmeta

> **Status:** design — not yet implemented. This document is the canonical
> reference for the Falcon protocol and its integration with NoKV.
> Last updated: 2026-05-11.

---

## TL;DR

Most fsmeta operations touch one or two disjoint keys (a dentry plus an
inode). Their read-set and write-set are statically determined at the
client, and 99% of concurrent ops in steady state are mutually
commutative. Today they pay full Raft cost regardless: ~5 ms wall
latency for a single op, ~1.5–2 K ops/s per region.

Falcon is a speculative-replication protocol that runs *on top* of
NoKV's existing raft + Percolator stack. Clients broadcast each op to
all replicas in parallel; replicas keep an in-memory **tentative log**
and accept ops that pass per-key conflict detection. As soon as a
quorum of replicas accept, the client returns success — the master
batches accepted ops and persists them through Raft asynchronously.
Conflicts and failures fall back to the existing Raft path, which is
treated as ground truth.

Combined with **dependency-graph parallel apply**, Falcon targets:

- **5–7× lower** single-op write latency (5 ms → ~1 ms)
- **10–20× higher** per-region write throughput (1.5 K → 15–30 K ops/s)
- **Linearizable** semantics preserved
- **No** durability or consistency weakening

The protocol is closest to CURP (NSDI'19) for the fast path and
PolarFS ParallelRaft (VLDB'18) for parallel apply, specialised for
fsmeta's static read/write-set property.

---

## 1. Motivation

### 1.1 What we measured

End-to-end profiling on a real fsmeta cluster (single peer + coordinator,
all production-equivalent caches and shape extractors wired):

| Workload | Throughput | Per-op latency |
|---|---:|---:|
| Create, conc=1 | 121 ops/s | 8.2 ms |
| Create, conc=16 | 639 ops/s | 1.56 ms |
| Create, conc=64 | 1 501 ops/s | 0.67 ms |
| Lookup, conc=1 (cached) | 10 372 ops/s | 96 µs |

CPU sampling showed application code is **0.13–0.36 % of total CPU**
across the entire stack. The rest is goroutine scheduling, condition
variables, and gRPC plumbing. The system spends 70 % of wall time
**waiting** on raft round-trips and fsync.

Profile data: `/tmp/nokv-prof/fsmeta_create.cpu` (kept in branch
`worktree-e2e-profile`).

### 1.2 Where the floor comes from

A single Create today goes:

```
client gRPC          ~ 50 µs
writeBatcher wait    ~200 µs   (kv-layer command coalescer)
proposalBatcher wait ~  1 ms   (peer-layer raft proposal coalescer)
processReady         ~  5 ms   (raft Ready cycle: append → fsync → ack)
apply path           ~ 50 µs   (single Percolator transaction)
reply gRPC           ~ 50 µs
                    ─────────
                       8.2 ms
```

Concurrency increases overall throughput by amortising raft Ready
cycles across more ops, but the *per-op* dominator is still the raft
round-trip: a single fsmeta op cannot complete faster than one raft
commit cycle.

### 1.3 What's already optimal

Quite a lot. The investigation in branch `worktree-e2e-profile`
confirmed all of these are working in production:

- 1PC fast path admission ([`txn/percolator/txn.go:296`](../txn/percolator/txn.go))
  hits 99.9 % for workspace-parented Creates.
- TSO coalescer ([`fsmeta/runtime/raftstore/tso_coalescer.go`](../fsmeta/runtime/raftstore/tso_coalescer.go))
  with 200 µs window already reduces TSO RPC count.
- writeCommandBatcher ([`raftstore/kv/write_batcher.go`](../raftstore/kv/write_batcher.go))
  coalesces same-region same-cmd ops into a single RaftCmdRequest.
- BoundedStale read consistency is plumbed all the way to
  [`raftstore/store/command_ops.go`](../raftstore/store/command_ops.go).
- Region split boundaries already align with fsmeta affinity buckets.

**There is no further gain to be had inside the existing Raft + Percolator
protocol.** The remaining floor is *fundamental* to how Raft is
structured, not an artefact of NoKV's implementation.

### 1.4 fsmeta is unusually friendly

Three properties of the fsmeta workload set it apart from generic
KV/SQL workloads:

1. **Tiny static footprint.** A Create touches exactly two keys (the
   dentry and the new inode). An Unlink touches up to three (dentry,
   inode, and the parent's link-count side-effect). All keys are
   derivable from the request before any execution begins.
2. **Predicate purity.** All conditions are `Exists` / `NotExists` /
   `VersionLessThan` on individual keys; no range predicates, no
   correlated sub-queries.
3. **High commutativity.** In a typical multi-tenant deployment,
   workspaces are independent, parents are independent, and inode
   allocations are bucketed for affinity. Two random Creates collide
   on the same dentry < 1 % of the time.

Falcon exploits exactly these three properties.

---

## 2. Goals and Non-Goals

### Goals

- Reduce single-op fsmeta write latency to < 1 ms in the common case.
- Scale per-region write throughput linearly with applier core count
  (currently capped at one core).
- Preserve the existing linearizable semantic of every fsmeta API.
- Integrate without rewriting Raft, Percolator, WAL, or LSM.
- Provide a clean fallback to today's Raft path so any unsafe
  condition degrades gracefully rather than risking correctness.

### Non-Goals

- Not a new replication library. We piggy-back on the existing raft
  cluster; followers are the witnesses.
- No new hardware assumptions: no NVM, no programmable switches, no
  user-space TCP stacks.
- We do **not** target cross-region transactions in v1. RenameSubtree
  and other cross-region 2PC paths continue to use today's Percolator
  flow unchanged.
- Falcon is fsmeta-specific. Generic KV writes (raftstore/client kv
  RPC) remain on the Raft path.

---

## 3. Background

### 3.1 NoKV layout

```
fsmeta/server         ── gRPC service over Executor
    └── fsmeta/exec   ── operation planner + transaction retry
        └── TxnRunner ── interface (raftstore client adapter)
            └── raftstore/client ── kv-RPC client + region routing
                └── raftstore/kv ── Service.Mutate / TryAtomicMutate
                    └── raftstore/store ── ProposeCommand / ReadCommand
                        └── raftstore/peer ── proposalBatcher → raft
                            └── engine/wal + engine/lsm ── durability
```

Key invariants (from `project_nokv_architecture` memory):

- Every write obeys `vlog → WAL → memtable → flush SST → manifest`
  ordering. Falcon must preserve this.
- Region descriptors live only in `meta/root` truth, never in the
  manifest. Falcon stays clear of topology truth.
- Raft entries share the WAL with LSM data, distinguished by record
  type. Falcon's tentative log is **not** persisted in the WAL.

### 3.2 What the Raft cluster looks like today

A region is a Raft group, typically 3 peers (one leader, two
followers). Followers receive AppendEntries, fsync, and ack. The
leader commits when ⌈N/2⌉+1 peers (including itself) have ack'd. On
commit the leader runs the apply pipeline.

The **proposalBatcher** in `raftstore/peer/proposal_batcher.go`
collects up to 64 proposals or waits 1 ms, then issues one raft
Ready cycle for the batch. This is the dominant per-op cost when
concurrency exceeds the writeBatcher window.

---

## 4. Design Overview

### 4.1 Insight

Raft's job is two things rolled into one: **(a) total ordering** of all
ops, and **(b) durable replication** of the ordered log. For ops on
disjoint keys, (a) is unnecessary. Yet Raft pays the round-trip
ordering cost regardless.

Falcon **decouples** the two: replicas accept and tentatively order
ops in memory (cheap, parallel), and the master persists them through
Raft as a background operation (expensive, batched). For
non-conflicting ops, the client only waits for memory acks → ~1 RTT.

### 4.2 Layering

```
┌──────────────────────────────────────────────────────────────────┐
│ Falcon Coordinator (NEW, in raftstore/peer)                      │
│   - tentative log (in-memory, per replica)                       │
│   - conflict detector (lock-free)                                │
│   - submit handler (replica side)                                │
│   - master commit loop (drives raft proposals)                   │
│   - master crash recovery (RIFL-style)                           │
├──────────────────────────────────────────────────────────────────┤
│ Falcon Client (NEW, in raftstore/client + fsmeta)                │
│   - broadcast submit + quorum ack                                │
│   - slow-path fallback                                           │
│   - read-merged-view                                             │
├──────────────────────────────────────────────────────────────────┤
│ Existing raft + Percolator + WAL + LSM (UNCHANGED)               │
└──────────────────────────────────────────────────────────────────┘
```

The dotted line: existing protocols are untouched. Falcon adds a
fast path that *bypasses* raft for the common case and falls through
to it when fast-path conditions fail.

### 4.3 Where Falcon ends and Raft begins

Falcon is responsible for:
- Op admission and tentative ordering
- Conflict detection
- Returning success to clients

Raft remains responsible for:
- Durable replication (fsync)
- Log compaction / snapshots
- Leader election
- Membership changes
- Permanent total order ground truth

A Falcon-acked op is **not durable** until it has been included in a
committed raft entry. The fast-path "success" is a *promise* that the
master will eventually persist this op, backed by quorum-witness
memory.

---

## 5. Protocol Specification

### 5.1 Operation lifecycle

```
                                                    ┌─ slow path ─→  raft.Propose
                                                    │
client                                               │
  │                                                  │
  ├── Submit(op) ───broadcast──→ all replicas ──┐    │
  │                                              ↓    │
  │                                          accept / conflict
  │                                              ↓    │
  │←────────────── quorum accept ──────────────  │    │
  │                                              ↓    │
                                              master collects acked
                                                  │
                                                  ↓
                                              raft Ready cycle (batched)
                                                  │
                                                  ↓
                                              parallel apply (DAG)
                                                  │
                                                  ↓
                                              tentative log GC
```

Key timestamps:
- `T_submit`: client sends op
- `T_quorum`: client receives quorum-ack — this is the **commit point**
- `T_persist`: raft commits the master's batch — this is **durable point**
- `T_apply`: op visible in LSM

The interval `T_quorum → T_persist` is the speculative window. During
this window, the op is committed (visible to other ops in the cluster
through tentative-log merge) but not yet on disk.

### 5.2 Data structures

#### 5.2.1 Operation

```go
// Op is the unit of speculative replication. It carries everything
// a replica needs to validate and accept without coordinating with
// the master.
type Op struct {
    // Provenance: client identifier + per-client monotonic sequence.
    // Used for idempotency on retry and recovery.
    ClientID uint64
    Seq      uint64

    // Global timestamp from the coordinator TSO. Establishes the
    // op's position in the linearizable order.
    Ts uint64

    // Static read-set: keys whose state determines whether the op
    // can commit. Each entry pairs a key with a predicate.
    ReadSet []Predicate

    // Static write-set: keys this op will mutate, and the values to
    // write at commit time.
    WriteSet []Mutation

    // Region this op targets. A multi-region op (rare) is split
    // into one Op per region by the client.
    RegionID uint64

    // Encoded fsmeta-level intent (Create/Unlink/Rename/...).
    // Used by the executor and by the apply path.
    Encoded []byte
}

type PredCond uint8
const (
    PredExists PredCond = iota
    PredNotExists
    PredVersionLT      // value's commit_ts < V
    PredVersionEQ
)

type Predicate struct {
    Key  []byte
    Cond PredCond
    V    uint64       // ts argument for VersionLT/EQ
}

type Mutation struct {
    Key   []byte
    Value []byte      // empty = delete
    Meta  byte
}
```

**Invariant**: For any op produced by the fsmeta executor, ReadSet and
WriteSet are computable from the request alone, without observing
runtime state. This is the property that makes Falcon work.

#### 5.2.2 Tentative Log

```go
// TentativeLog is the per-replica in-memory store of accepted-but-not-
// yet-raft-committed ops. It is the heart of Falcon: every replica
// runs one, and the conflict detector reads from it on every Submit.
type TentativeLog struct {
    mu sync.RWMutex

    // Ordered slice of accepted entries, indexed by local sequence.
    // Cleared as raft commits batches.
    entries []*Entry

    // Inverted index for conflict detection: key → entries that
    // touch it. Used both by Submit (write conflict) and by
    // read-merged-view (visibility).
    byKey map[string][]*Entry

    // High-water sequence assigned. Local to each replica.
    nextSeq uint64
}

type EntryState uint8
const (
    StatePending EntryState = iota   // accepted, awaiting raft commit
    StateRaftCommitted               // raft has commit_index >= this op
    StateApplied                     // applied to LSM
)

type Entry struct {
    Op        *Op
    LocalSeq  uint64
    State     EntryState
    AcceptedAt time.Time
}
```

The tentative log is bounded (default 8 K entries / region). When full,
incoming Submits return a *backpressure* signal that triggers slow path
on the client.

#### 5.2.3 Replica state

```go
type FalconReplica struct {
    role      Role        // Master | Witness
    raftPeer  *peer.Peer  // existing raft peer
    tentative *TentativeLog
    detector  *Detector   // conflict detector

    // Master only
    commitLoop *commitLoop // batches acked ops → raft

    // Recovery
    epoch     uint64       // bumped on master election
}
```

### 5.3 Fast path

#### 5.3.1 Client

```go
func (c *FalconClient) Submit(ctx context.Context, op *Op) error {
    op.Ts = c.tso.Get()
    op.Seq = c.nextSeq()

    region := c.routeFor(op)
    peers := region.Peers
    quorum := len(peers)/2 + 1

    type ackResult struct {
        peer *Peer
        verdict Verdict
        err     error
    }
    resCh := make(chan ackResult, len(peers))
    for _, p := range peers {
        go func(p *Peer) {
            v, err := p.Submit(ctx, op)
            resCh <- ackResult{p, v, err}
        }(p)
    }

    var accepted, rejected, errored int
    seenMaster := false
    masterAck := false

    for i := 0; i < len(peers); i++ {
        ack := <-resCh
        if ack.err != nil {
            errored++
            if errored > len(peers)-quorum {
                return c.slowPath(ctx, op, "transport quorum lost")
            }
            continue
        }
        if ack.peer == region.Leader {
            seenMaster = true
            masterAck = ack.verdict == Accept
        }
        switch ack.verdict {
        case Accept:
            accepted++
        case Conflict:
            rejected++
            return c.slowPath(ctx, op, "replica reported conflict")
        case Backpressure:
            return c.slowPath(ctx, op, "replica reported backpressure")
        }
        if accepted >= quorum && (seenMaster && masterAck) {
            return nil  // fast path success
        }
    }
    return c.slowPath(ctx, op, "quorum not reached")
}
```

**Master-required rule**: even if a quorum of *witnesses* accept, the
client still waits for the master's verdict and requires it to be
Accept. This is critical for recovery (see §5.7).

#### 5.3.2 Replica handler

```go
func (r *FalconReplica) Submit(op *Op) Verdict {
    // 1. Backpressure check.
    if r.tentative.full() {
        return Backpressure
    }

    // 2. Idempotency check: if this (ClientID, Seq) is already in
    //    the tentative log or recently committed, return the cached
    //    verdict. Prevents double-application on client retry.
    if v, ok := r.idempotencyCache.Get(op.ClientID, op.Seq); ok {
        return v
    }

    // 3. Acquire per-key locks (lock-free striping; same approach
    //    as txn/latch but only across WriteSet).
    keys := append(predicateKeys(op.ReadSet), mutationKeys(op.WriteSet)...)
    guard := r.detector.Lock(keys)
    defer guard.Unlock()

    // 4. Predicate validation against the merged view (LSM + tentative).
    for _, p := range op.ReadSet {
        actual, err := r.readMerged(p.Key)
        if err != nil {
            return Conflict
        }
        if !p.Cond.Satisfies(actual, p.V) {
            return Conflict
        }
    }

    // 5. Write conflict: any pending op that touches our WriteSet.
    for _, m := range op.WriteSet {
        if pending := r.tentative.byKey[string(m.Key)]; len(pending) > 0 {
            return Conflict
        }
    }

    // 6. Accept.
    seq := r.tentative.nextSeq.Add(1)
    entry := &Entry{Op: op, LocalSeq: seq, State: StatePending, AcceptedAt: time.Now()}
    r.tentative.append(entry)
    r.idempotencyCache.Put(op.ClientID, op.Seq, Accept)

    return Accept
}
```

### 5.4 Slow path

When the fast path fails, the client falls through to the existing
raft propose path:

```go
func (c *FalconClient) slowPath(ctx context.Context, op *Op, reason string) error {
    metrics.SlowPathTotal.Inc(reason)
    return c.legacyClient.ProposeViaRaft(ctx, op)
}
```

The slow path is the current `raftstore/client.TwoPhaseCommit` /
`TryAtomicMutate` flow. It is what every fsmeta op uses today.

### 5.5 Reads (linearizable)

Reads must observe all fast-path committed writes. Every read merges
the tentative log with the LSM:

```go
func (r *FalconReplica) Read(key []byte, version uint64) ([]byte, error) {
    // Snapshot the tentative entries that touch this key, ordered by
    // their op timestamp.
    pending := r.tentative.snapshotByKey(key)

    // Find the most recent entry visible at the read version.
    for i := len(pending) - 1; i >= 0; i-- {
        e := pending[i]
        if e.Op.Ts > version {
            continue
        }
        if e.State >= StatePending {
            // Apply the tentative mutation virtually.
            for _, m := range e.Op.WriteSet {
                if bytes.Equal(m.Key, key) {
                    return m.Value, nil
                }
            }
        }
    }

    // Fall through to LSM read.
    return r.lsm.Get(key, version)
}
```

This costs an extra map lookup + slice walk per read. The byKey index
keeps the walk to O(pending ops touching this key), typically 0 or 1.

### 5.6 Master commit + GC

The master runs a background loop that drains the tentative log and
proposes batches to raft:

```go
func (m *Master) commitLoop() {
    ticker := time.NewTicker(50 * time.Microsecond)
    defer ticker.Stop()

    for range ticker.C {
        batch := m.tentative.drainAcceptedSince(m.lastDrained, maxBatchSize)
        if len(batch) == 0 {
            continue
        }

        cmd := encodeBatch(batch)
        proposalDone := m.raft.Propose(cmd)

        // Wait for raft commit. Async is OK: pending entries stay in
        // the tentative log until we mark them StateRaftCommitted.
        go func() {
            <-proposalDone
            m.tentative.markCommitted(batch)
        }()
    }
}
```

GC happens when entries reach `StateApplied`:

```go
func (r *FalconReplica) gcApplied() {
    r.tentative.dropApplied()  // remove entries whose State == StateApplied
}
```

Tentative log size is therefore bounded by the raft-commit lag, not
by the total op count.

### 5.7 Master crash recovery (RIFL-style)

This is **the hardest part of the protocol** and the main contribution
of the work. When the master fails, a new master is elected via Raft.
Before it can accept new submits, it must reconstruct the speculative
state: which ops were fast-path-committed (must be persisted) and which
were not (may be dropped).

#### Recovery procedure

```
1. New master M' is elected (via existing raft).
2. M' broadcasts RecoveryRequest to all alive replicas.
3. Each replica returns its TentativeLog snapshot (Pending entries
   only — Applied/Committed are no longer in tentative).
4. M' merges:
     allOps := dedupe by (ClientID, Seq) the union of returned snapshots
     for each op:
        accepted_by := count of replicas whose snapshot included op
        if accepted_by >= quorum AND op was acked by previous master:
            // The op was fast-path-committed. Must persist.
            mustReplay = append(mustReplay, op)
        else:
            // Fewer than quorum, OR previous master never saw it.
            // The client never received success. Safe to drop.
            // But: we must reject any future Submit with same
            // (ClientID, Seq) lest the client retry and we apply twice.
            blacklist[op.ClientID, op.Seq] = Rejected
5. M' proposes mustReplay through Raft as a single batch.
6. After raft commit and apply: M' opens for new Submits.
```

The master-required rule from §5.3.1 makes step 4's *previous master*
condition cheap to check: an op acked by the previous master is by
construction the only way the client got a success response, so its
absence in M''s snapshot means the previous master crashed before
broadcasting it. M' need not consult dead masters; the witness count
plus the existence of an Accept in the union is sufficient.

**Proof sketch** (full version in §6):
- If a client received success at time T, then at T there were ≥
  quorum replicas with the op in their tentative log AND the master
  had the op in its tentative log.
- After master crash, ≤ f replicas have failed (where N = 2f+1).
- ≥ quorum - f = 1 replica with the op survives.
- M''s broadcast reaches at least the surviving replica.
- M' sees the op and includes it in mustReplay. ∎

#### Edge cases

- **Master crashes after fast-path Accept but before broadcasting to
  followers**: client only got master ack, never reached quorum →
  client never received success. M' may or may not see the op
  depending on whether master flushed to a follower. Either way,
  client did not commit, so dropping is safe.
- **Network partition**: standard raft leader election handles
  partition. Falcon-side: the partitioned old master cannot accept
  new ops because raft will reject its proposals; clients hit slow
  path and time out.
- **Slow follower**: a follower that is behind on raft-commits will
  have a longer tentative log. Backpressure throttles new submits
  before this becomes unbounded.

### 5.8 Parallel apply via dependency DAG

When raft commits a batch of N ops, the apply path traditionally
processes them one at a time. Falcon builds a dependency DAG and
applies independent ops concurrently:

```go
func (m *Master) applyBatch(ops []*Op) {
    // 1. Build read/write set per op (already in Op).
    // 2. For each pair of ops in raft order, draw an edge if their
    //    sets overlap. The DAG is the transitive reduction.
    dag := buildDAG(ops)

    // 3. Topological wave execution.
    for !dag.empty() {
        ready := dag.takeRoots()  // ops with no remaining dependencies

        var wg sync.WaitGroup
        for _, op := range ready {
            wg.Add(1)
            go func(op *Op) {
                defer wg.Done()
                m.applyOne(op)
            }(op)
        }
        wg.Wait()

        dag.removeNodes(ready)
    }
}
```

For typical fsmeta workloads where ops touch disjoint keys, the DAG
is mostly flat (one wide level). Apply runs in O(1) wall time per
batch instead of O(N).

The DAG construction itself is O(N²) in the worst case (every pair
checked) but in practice the byKey index reduces it to O(N) when most
ops are disjoint.

---

## 6. Correctness

### 6.1 Linearizability

**Claim**: For every history of Submit/Read calls, there exists a
total order T such that:
1. T respects per-client real-time ordering (monotonic Seq).
2. Every Read R returns the value that T's last Write to R's key
   would produce.
3. Every successful Submit appears in T.

**Proof outline**:

- Define T as the order by `(op.Ts, op.ClientID, op.Seq)`. TSO
  monotonicity gives uniqueness.
- A Submit returns success only after quorum acceptance, which
  requires predicate satisfaction in the merged view. The merged
  view contains all earlier-acked ops with smaller Ts.
- A Read at version V observes (a) committed writes in LSM with
  commit_ts ≤ V and (b) tentative ops with op.Ts ≤ V whose state is
  Pending or beyond. By construction, any op with op.Ts ≤ V is
  either applied (case a) or in the tentative log (case b).
- By the master-required rule, no op at Ts > V can affect a read at
  V before V is exceeded.

The full proof requires TLA+ specification (planned for M4).

### 6.2 Durability

**Failure model**: ≤ f node failures out of N = 2f+1.

**Claim**: For every Submit that returned success, the op will be
present in the persisted Raft log after master recovery.

**Proof**: Quorum ack means the op was in master's tentative log
*and* in ≥ f witnesses' tentative logs. After ≤ f failures, ≥ 1
witness with the op survives. Recovery (§5.7) finds it through the
broadcast to alive replicas. ∎

### 6.3 Recovery termination

**Claim**: Recovery completes in bounded time.

The number of pending ops is bounded by the tentative-log capacity
(8 K). Recovery does one Raft proposal of ≤ 8 K ops (single batch),
followed by one apply phase. Both are bounded.

### 6.4 What we are NOT proving

- Falcon does not improve availability over raft. If raft cannot
  elect a leader, no fast path either.
- Falcon does not improve cross-region consistency. RenameSubtree
  remains a 2PC across two raft groups.
- Falcon does not handle Byzantine faults. Same model as raft.

---

## 7. Implementation Roadmap

| Milestone | Scope | LoC est. | Time | Independent? |
|---|---|---:|---:|---|
| **M1** | Parallel apply via DAG (no protocol change) | 1 500 | 3-4 weeks | Yes |
| **M2** | Tentative log + replica Submit handler | 2 500 | 6-8 weeks | No (needs M1) |
| **M3** | Client fast path + slow-path fallback | 1 500 | 4-6 weeks | No (needs M2) |
| **M4** | RIFL-style master recovery + TLA+ | 1 500 | 6-8 weeks | No (needs M3) |
| **M5** | fsmeta-aware optimisations | 1 000 | 2-3 weeks | No (needs M3) |
| **M6** | Evaluation + paper writing | — | 3-4 weeks | — |

**Total**: ~ 8 000 LoC, 5–7 months single-engineer.

### M1 first

M1 (parallel apply) is independently mergeable and produces real
production value (10–20 % per-region throughput) without any protocol
change. It also de-risks the dependency-DAG construction code that M2
needs.

### Decision points

- **End of M1**: do measurements support the protocol-change cost? If
  parallel apply alone solves the bottleneck, halt here.
- **End of M3**: TLA+ model checking of recovery. If specification
  reveals subtle issues, may need protocol revisions before paper.

---

## 8. Performance Projections

### 8.1 Per-op latency

| Path | Today | After Falcon | Speedup |
|---|---:|---:|---:|
| Single Create, conc=1 | 8.2 ms | 1.0–1.5 ms | 5–8× |
| Single Create, conc=64 | 0.67 ms | 0.3–0.5 ms | 1.5–2× |
| 100-file checkpoint storm | ~700 ms | ~30–60 ms | 12–20× |
| Cross-region rename | ~15 ms | unchanged | 1× |

### 8.2 Per-region throughput

At conc=64, today's bottleneck is serial apply (~2.4 K ops/s). With
DAG-parallel apply:

| Workload | Today | M1 | M3 | M5 |
|---|---:|---:|---:|---:|
| Independent Creates | 2.4 K | 8 K | 25 K | 30 K |
| Mixed (90% independent, 10% conflicting) | 2.0 K | 6 K | 18 K | 22 K |
| Hotspot (single workspace, all conflicting) | 2.4 K | 2.4 K | 2.5 K | 2.5 K |

The hotspot case shows the protocol's failure mode: when most ops
conflict, fast path is rejected and we are no worse than raft.

### 8.3 Recovery time

After master crash, Falcon adds:
- One round-trip to all replicas to gather tentative logs (~ 1 ms)
- One raft propose of mustReplay (≤ 8 K ops, ~ 5 ms)

Total additional recovery time: < 10 ms over baseline raft recovery,
which is in the seconds range due to leader election heartbeats.

---

## 9. Risks

| Risk | Severity | Mitigation |
|---|---|---|
| Recovery protocol bug | **Critical** | TLA+ specification + model checking; chaos testing before enable |
| Tentative log GC lag → memory blowup | High | Hard cap (8 K) + backpressure → slow path |
| Predicate validation false negatives | High | Conservative: any uncertainty → Conflict → slow path |
| Read-merged-view race with apply | Medium | Strict snapshot semantics; entries advance state monotonically |
| Performance regression on hotspot | Medium | Clean fallback; metrics expose fast-path admission rate |
| Increased network traffic (broadcast) | Low | Only ~3× current peer count; same connections reused |
| Compatibility with TiKV-style tooling | Low | Slow path matches existing wire format |

The recovery bug is the only existential risk. Everything else
gracefully degrades.

---

## 10. Evaluation Plan

For paper-quality evidence, we need to compare Falcon against:

1. **Baseline NoKV** (this branch's HEAD)
2. **TiKV** (~equivalent stack, mature)
3. **CockroachDB** metadata layer (different protocol, similar workload)
4. **etcd** (raft-only, single-key linearizable)

### Workloads

- **fsmeta-bench** (existing in `benchmark/fsmeta/`): checkpoint storm,
  multi-workspace autoscale, mixed.
- **Microbenchmarks**: single-key write latency, hot-key contention,
  read-write mix.
- **Failure scenarios**: master crash, network partition, slow follower.

### Metrics

- p50 / p99 / p99.9 write latency
- Per-region throughput vs concurrency
- Fast-path admission rate by workload
- Recovery time after master kill
- CPU / memory / network overhead vs baseline

### Comparison axes

- Raft vs Falcon (same NoKV codebase)
- Falcon-fast-path vs Falcon-slow-path-only (ablation)
- M1-only vs M1+M2+M3 vs full Falcon (incremental)

---

## 11. Open Questions

1. **TSO precision under clock skew**: Falcon depends on TSO
   monotonicity for the total order. NoKV's existing TSO from
   coordinator is monotonic per-mount. Cross-mount monotonicity is
   weaker but fsmeta does not require it. Need to verify this still
   holds under all coordinator failure modes.

2. **Idempotency cache size**: how big does it need to be? Bounded
   by max in-flight client retries, typically small. But sizing
   wrong causes occasional false rejects.

3. **Witness role on followers**: should we use *all* followers as
   witnesses, or designate a subset? Using all maximises ack
   probability; using a subset reduces network amplification.
   Initial choice: all followers.

4. **Watchsubtree event ordering**: events from fast-path-committed
   ops need delivery order consistent with the linearizable order T.
   Watch event publication should happen after the master's raft
   commit, not at fast-path ack time, to avoid showing un-recovered
   ops to watchers. Need to verify watch path.

5. **Single-region vs multi-region scope**: v1 is per-region only.
   Cross-region rename remains 2PC. A Falcon-equivalent for cross-
   region transactions (Janus-style) is a follow-up paper.

---

## 12. References

1. Park, S. & Ousterhout, J. *Exploiting Commutativity For Practical
   Fast Replication*. NSDI '19. [The CURP paper.]
2. Cao, W. et al. *PolarFS: An Ultra-low Latency and Failure Resilient
   Distributed File System for Shared Storage Cloud Database*.
   VLDB '18. [ParallelRaft.]
3. Ding, C. et al. *Scalog: Seamless Reconfiguration and Total Order
   in a Scalable Shared Log*. NSDI '20.
4. Ousterhout, J. & Ongaro, D. *In Search of an Understandable
   Consensus Algorithm*. ATC '14. [Raft.]
5. Peng, D. & Dabek, F. *Large-scale Incremental Processing Using
   Distributed Transactions and Notifications*. OSDI '10.
   [Percolator.]
6. Shamis, A. et al. *Fast General Distributed Transactions with
   Opacity*. SIGMOD '19. [FaRMv2; informs failure-atomic apply.]
7. Lockerman, J. et al. *The FuzzyLog: A Partially Ordered Shared
   Log*. OSDI '18. [Inspires partial-order tentative log.]
8. Liu, Y. et al. *InfiniFS: An Efficient Metadata Service for
   Large-Scale Distributed Filesystems*. FAST '22.

---

## Appendix A: API surface

```go
// raftstore/peer/falcon — new package

type Coordinator struct { /* ... */ }

func (c *Coordinator) Submit(ctx context.Context, op *Op) Verdict
func (c *Coordinator) Read(ctx context.Context, key []byte, ts uint64) ([]byte, error)
func (c *Coordinator) RecoverFrom(ctx context.Context, snapshot []TentativeSnapshot) error

// raftstore/client — extended

func (c *Client) FalconSubmit(ctx context.Context, op *Op) error  // fast path
// existing Mutate / TwoPhaseCommit remain as slow path

// fsmeta/exec — extended to populate ReadSet/WriteSet from plans
```

## Appendix B: Failure scenarios catalogue

| # | Scenario | Behaviour |
|---|---|---|
| 1 | Single follower crash | Fast path continues with quorum |
| 2 | Master crash, fast-path op pending | Recovery includes op, client retry idempotent |
| 3 | Master crash, fast-path op never broadcast | Client times out, retries on slow path |
| 4 | Network partition (master in minority) | Master loses raft leadership, clients fail over |
| 5 | Tentative log full | Backpressure → slow path → drains as raft catches up |
| 6 | Conflict between concurrent ops | One accepts, the other gets Conflict → slow path |
| 7 | Replica returns stale predicate evaluation | Detected at master commit; whole batch rolled back |
| 8 | Client retries with same Seq | Idempotency cache returns prior verdict |

---

*Document version 1.0. Comments / corrections via NoKV pull request.*
