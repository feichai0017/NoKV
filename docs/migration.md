# Standalone to Cluster Migration

This document defines the first production-worthy migration path from a
standalone NoKV workdir into distributed mode.

The design is intentionally conservative:

- offline only
- one-shot bootstrap
- no dual-write
- no background auto-repair

The goal is to make an existing standalone workdir become a valid
single-store cluster seed first, then expand it into a replicated cluster.

---

## 1. Goals

1. Reuse the existing standalone storage engine workdir as the distributed
   data plane.
2. Convert one standalone workdir into:
   - one `Store`
   - one full-range `Region`
   - one local `Peer`
   - one valid raft durable state
3. Make the conversion explicit and recoverable.
4. Avoid parallel truth sources during migration.

---

## 2. Non-goals

The first version does not attempt:

- online migration
- dual-write cutover
- automatic region split / rebalance
- compatibility with partially migrated normal service

---

## 3. Existing Building Blocks

The current codebase already provides most of the low-level pieces:

- `cmd/nokv/serve`
  - opens the existing `DB` workdir and layers `raftstore` on top
- `raftstore/meta`
  - persists the store-local peer catalog and per-group raft replay pointers
- `raftstore/store/peer_lifecycle.go`
  - starts peers from local recovery metadata
- `raftstore/engine/wal_storage.go`
  - persists per-group raft durable state inside the shared WAL
- `raftstore/snapshot`
  - provides logical region snapshot export/import over internal entries
- `raftstore/store/membership_service.go`
  - provides the later path from seed region to multi-peer region

What is missing is the migration control protocol and a formal CLI.

---

## 4. Core Invariants

The migration flow must preserve these invariants:

1. Standalone writes must stop before migration starts.
2. The migrated workdir must not silently return to standalone mode.
3. Bootstrap is the only allowed non-apply path that can create initial region
   truth for the migrated directory.
4. Engine manifest remains storage-engine metadata only.
5. Store-local region metadata lives only in `raftstore/meta`.
6. PD is not used to create local truth during bootstrap.

---

## 5. High-Level Flow

```mermaid
flowchart LR
    A["Standalone workdir"] --> B["nokv migrate plan"]
    B --> C["nokv migrate init"]
    C --> D["Seeded workdir"]
    D --> E["nokv serve --pd-addr ..."]
    E --> F["Single-store cluster seed"]
    F --> G["nokv migrate expand"]
    G --> H["Multi-peer cluster"]
```

---

## 6. Workdir Modes

The migrated workdir should expose one explicit mode file under
`raftstore/meta`, for example `MODE.json`.

Only four modes are needed:

- `standalone`
- `preparing`
- `seeded`
- `cluster`

### Semantics

- `standalone`
  - regular standalone engine directory
- `preparing`
  - migration is in progress; standalone service must refuse normal startup
- `seeded`
  - standalone data has been converted into a single-store cluster seed
- `cluster`
  - directory is operating in distributed mode

Library-level opens must treat these modes explicitly:

- ordinary standalone `NoKV.Open` accepts only `standalone`
- `nokv migrate init` explicitly opts into `preparing`
- `nokv serve` explicitly opts into `seeded` and `cluster`
- offline diagnostics may opt into all modes deliberately

The file only needs minimal state:

```json
{
  "mode": "seeded",
  "store_id": 1,
  "region_id": 1,
  "peer_id": 101
}
```

---

## 7. CLI Shape

The first version should use these commands:

- `nokv migrate plan`
- `nokv migrate init`
- `nokv migrate status`
- `nokv migrate expand`
- `nokv migrate remove-peer`
- `nokv migrate transfer-leader`

### `nokv migrate plan`

Read-only preflight check.

Input:

- `--workdir`

Output:

- current mode
- eligibility
- blockers
- warnings
- suggested next command

### `nokv migrate init`

Performs standalone -> seed conversion.

Input:

- `--workdir`
- `--store`
- `--region`
- `--peer`

Output:

- local catalog written
- initial raft state synthesized
- mode changed to `seeded`

### `nokv migrate status`

Returns the current migration mode and seed identifiers.

### `nokv migrate expand`

Expands the seeded region into a replicated region by driving one or more peer
additions and snapshot catch-up through the leader store's admin RPC.

Input:

- `--addr`
- `--region`
- repeated `--target <store>:<peer>[@addr]`
- optional `--wait`
- optional `--poll-interval`

The old single-target `--store/--peer/--target-addr` form has been removed.

### `nokv migrate remove-peer`

Removes one peer from a replicated region through the leader store's admin RPC
and optionally waits until the target store no longer hosts it.

### `nokv migrate transfer-leader`

Transfers region leadership to a specific peer and optionally waits until that
peer becomes leader.

### `scripts/migrate_to_cluster.sh`

For local operator workflows, `scripts/migrate_to_cluster.sh` wraps the full
happy-path sequence:

1. `nokv migrate plan`
2. `nokv migrate init`
3. start PD-lite and the seed/target stores
4. `nokv migrate expand`
5. optional `nokv migrate transfer-leader`
6. optional `nokv migrate remove-peer`

The script is intentionally conservative. It only accepts a standalone seed
workdir plus fresh target store workdirs and delegates all state transitions to
the migration CLI.

---

## 8. Phase 1: `plan`

`nokv migrate plan --workdir <dir>`

This stage must verify:

1. standalone manifest is readable
2. WAL / vlog / SST recovery chain is consistent
3. the workdir is not already seeded or clustered
4. there is no local peer catalog that would conflict with migration
5. the directory is not already poisoned or in fatal recovery state

No state is modified.

---

## 9. Phase 2: `init`

`nokv migrate init --workdir <dir> --store <sid> --region <rid> --peer <pid>`

This stage performs the actual standalone -> seed conversion.

### Step 1: gate the directory

Write mode = `preparing`.

From this point, ordinary standalone startup must reject the workdir.

### Step 2: create the initial local catalog entry

Write one full-range `RegionMeta` into `raftstore/meta`:

- `ID = region`
- `StartKey = nil`
- `EndKey = nil`
- `Epoch.Version = 1`
- `Epoch.ConfVersion = 1`
- `Peers = [{StoreID: sid, PeerID: pid}]`
- `State = running`

This is the only bootstrap-time source of local region truth.

### Step 3: export a logical region snapshot

The current standalone keyspace must first be materialized into a formal region
snapshot artifact. The artifact should be a directory containing:

- `manifest.json`
- `entries.bin`

The seeded artifact lives under:

- `RAFTSTORE_SNAPSHOTS/region-<id>`

The payload in `entries.bin` is a stream of encoded internal entries, not a raw
copy of SST/WAL/value-log files. Value-log backed records must be materialized
into inline values during export so the snapshot does not depend on source-side
value-log offsets.

This gives the system one reusable primitive for:

- standalone -> seed bootstrap
- seed -> add-peer snapshot install
- future restore/reseed flows

### Step 4: synthesize initial raft durable state

The exported snapshot artifact now defines the state machine contents of a
valid single-node raft group.

The initial raft state should be:

- snapshot index = 1
- snapshot term = 1
- hard state term = 1
- hard state commit = 1
- conf state voters = `[peer]`

The durable raft metadata should still be written through the existing raft
storage path rather than via ad hoc files.

### Step 5: persist the local raft replay pointer

Use `raftstore/meta` to persist the group-local replay pointer that corresponds
to the synthesized raft state.

### Step 6: finalize

Write mode = `seeded`.

At this point the directory is ready for:

```bash
nokv serve --workdir <dir> --store-id <sid> --pd-addr <pd>
```

The first successful `nokv serve` over that directory must promote the mode to
`cluster`.

---

## 10. Phase 3: seed startup

After `init`, `cmd/nokv serve` should be able to:

1. open the same engine workdir
2. load the local peer catalog from `raftstore/meta`
3. open the group-local raft durable state
4. start one local peer
5. serve distributed traffic through the normal raftstore path

No special startup fork should be added. The migrated seed should reuse the
normal distributed startup path.

---

## 11. Phase 4: expansion

Once the seed is healthy, normal distributed mechanisms should replicate it.

The intended order is:

1. start empty remote stores
2. call `nokv migrate expand` against the current region leader
3. leader issues `AddPeer`
4. target store bootstraps an empty peer on `MsgSnapshot`
5. target peer imports the logical region snapshot payload
6. target peer applies the corresponding raft durable snapshot metadata
7. wait until the target store reports the new peer as hosted
8. repeat until quorum is established
9. later split and rebalance

This phase reuses:

- `Store.ProposeAddPeer(...)`
- logical region snapshot export/import in `raftstore/snapshot`
- unknown-peer snapshot bootstrap in `raftstore/store/peer_lifecycle.go`
- normal raft snapshot delivery

The current implementation keeps orchestration explicit:

- sequential `AddPeer` rollout through repeated `expand` targets
- explicit `remove-peer`
- explicit `transfer-leader`
- no automatic split or rebalance

---

## 12. Failure Handling

The migration flow must never silently repair or silently fall back.

### During `plan`

- read-only errors are returned directly

### During `init`

If any step after `preparing` fails:

- keep mode as `preparing`
- refuse normal standalone startup
- require explicit operator action:
  - retry `init`, or
  - run a future rollback command

This is intentionally strict. Half-migrated state must not behave like a normal
standalone database.

---

## 13. Minimal Test Matrix

The first implementation should land with these tests:

### Preflight

- plan rejects unreadable or inconsistent standalone state
- plan rejects already seeded workdir

### Init

- init writes the full-range local catalog entry
- init writes initial raft durable state
- init is idempotent or explicitly rejected on rerun

### Startup

- a seeded workdir starts successfully under `nokv serve`
- reads and writes flow through the normal raft path

### Failure semantics

- failure after mode=`preparing` prevents standalone startup
- failure before finalize does not silently produce `seeded`

---

## 14. Naming Rules

To keep the surface clean:

- `manifest` remains reserved for storage-engine metadata
- `catalog` is the `nokv-config` command for local peer catalog bootstrap
- migration commands stay short:
  - `plan`
  - `init`
  - `status`
  - `expand`
  - `remove-peer`
  - `transfer-leader`

This keeps the CLI precise without leaking implementation detail into every
command name.
