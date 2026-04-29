# 2026-04-25 Namespace Authority Events Umbrella

## TL;DR

- 🧭 Topic: a unified rooted-event boundary across namespace / subtree / snapshot / quota primitives.
- 🧱 Core objects: Mount, Subtree, SnapshotEpoch, QuotaFence, StoreMembership.
- 🔁 Call chain: `fsmeta/server -> coordinator/meta-root command -> rooted event -> coordinator/runtime view -> raftstore/fsmeta primitive`.
- 📚 Reference: NoKV Eunomia, `meta/root` rooted truth, DaisyNFS / FSCQ-style verified metadata boundaries, TiKV PD's membership / runtime view layering.

## 1. Conclusion

Namespace primitives must not each invent their own RootEvent. The current code already covers the major domains under this umbrella:

| Domain | Rooted truth | Runtime view | Stage |
|---|---|---|---|
| Store membership | whether a store belongs to the cluster, whether retired | address, heartbeat, capacity, load | done |
| Mount lifecycle | whether mount exists, root inode, schema version | mount admission cache, watch subscription close | done |
| Subtree authority | a subtree's authority era / handoff frontier | watcher fan-out, pending handoff repair | done |
| Snapshot epoch | snapshot ID, read timestamp, covered subtree | snapshot-version reads | done |
| Quota fence | quota limit, fence era, frontier | quota fence cache + data-plane usage counter | done |

Principle:

> `meta/root` only stores facts that **must** be durable, auditable, and that affect authority legality. Address, load, cache, watcher, and high-frequency usage counters do not belong in root; usage counters are data-plane keys, folded into the fsmeta metadata transaction.

## 2. Why we built the umbrella first

Without a unified schema, you end up with five mutually incompatible event styles:

- one for store membership;
- one for mount registry;
- one for snapshot;
- one for quota;
- one for subtree handoff.

Result: `meta/root/state` becomes an event landfill, `coordinator/rootview` writes a separate materialization for every primitive, and the test matrix shatters.

The umbrella's purpose isn't to land every event at once. It's to fix three things first:

1. Which facts may enter rooted truth.
2. Event naming and payload style.
3. How compact state is layered by domain.

## 3. Event naming rules

Use lifecycle verbs, avoid fuzzy state words.

| Recommended | Avoid | Reason |
|---|---|---|
| `StoreJoined` / `StoreRetired` | "temporarily offline" wording | `retired` clearly denotes a terminal membership state |
| `MountRegistered` / `MountRetired` | `MountAdded` / `MountDeleted` | mount is an authority object, not just a map entry |
| `SnapshotEpochPublished` / `SnapshotEpochRetired` | `SnapshotCreated` | the essential thing about a snapshot is a read epoch, not a file object |
| `QuotaFenceUpdated` | `QuotaChanged` | fence denotes an authority boundary |
| `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | `SubtreeMoved` | handoff is a protocol, not a single assignment |

Every event must:

- carry only truth fields in its payload;
- not include runtime address / load / cache;
- have a well-defined idempotent materialization rule;
- allow the current truth to be reconstructed from compact state;
- be independently interpretable by audit tools.

## 4. Compact state layering

`rootstate.Snapshot` should not be a flat field bag. The current direction is to layer it by domain:

```go
type Snapshot struct {
    State State

    Stores   map[uint64]StoreMembership
    Mounts   map[string]MountRecord
    Subtrees map[SubtreeID]SubtreeAuthority
    Snapshots map[SnapshotID]SnapshotEpoch
    Quotas   map[QuotaSubject]QuotaFence

    Descriptors map[uint64]descriptor.Descriptor
    PendingPeerChanges map[uint64]PendingPeerChange
    PendingRangeChanges map[uint64]PendingRangeChange
}
```

We're not requiring this all in one go. Today we've already added `Stores`, `Mounts`, `Subtrees`, `SnapshotEpochs`, `Quotas`, etc. in this direction; future domains continue to follow this layering — don't stuff everything into `State`.

## 5. Store membership events

Minimum shape for store membership:

```go
type StoreMembership struct {
    StoreID uint64
    State   StoreMembershipState
    JoinedAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}
```

Events:

```text
StoreJoined(store_id)
StoreRetired(store_id)
```

Fields **not** in rooted truth:

- `client_addr`
- `raft_addr`
- `last_heartbeat`
- `capacity`
- `available`
- `leader_count`

These are refreshed into the coordinator runtime view by store heartbeat.

## 6. Mount lifecycle events

Mount is fsmeta's namespace root. fsmeta doesn't need full POSIX mount, but it needs a durable mount registry so callers don't all invent their own mount strings.

The current code implements that registry: mount membership lives in `meta/root` rooted truth; `nokv-fsmeta`'s write path performs admission via the coordinator mount view; an unregistered or retired mount is rejected.

`spec/MountLifecycle.tla` covers this lifecycle:

- `MountRegistered` can only turn a never-seen mount into active;
- `MountRetired` is terminal — a retired mount cannot become active again;
- a mount's root inode and schema version are part of rooted identity, not runtime cache.

Recommended events:

```text
MountRegistered(mount_id, root_inode, schema_version)
MountRetired(mount_id, retired_at)
```

Compact record:

```go
type MountRecord struct {
    MountID string
    RootInode uint64
    SchemaVersion uint32
    RegisteredAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}
```

In rooted truth:

- whether the mount exists;
- root inode;
- fsmeta schema version;
- retired status.

Not in rooted truth:

- active client sessions;
- local mount cache;
- mount endpoint / frontend address.

## 7. Subtree authority events

`WatchSubtree` itself does not need every watch subscription written into root. Subscriptions are runtime view.

But subtree authority boundaries do need rooted events; otherwise `RenameSubtree` / `SnapshotSubtree` / `QuotaFence` will each invent their own boundaries.

Current event shape:

```text
SubtreeAuthorityDeclared(mount_id, subtree_root, authority_id, era)
SubtreeHandoffStarted(mount_id, subtree_root, from_authority, to_authority, legacy_frontier)
SubtreeHandoffCompleted(mount_id, subtree_root, authority_id, era, inherited_frontier)
```

The naming maps directly onto Eunomia:

- `authority_id + era` ≈ Tenure;
- `legacy_frontier` ≈ Legacy;
- `handoff completed` ≈ Finality.

`WatchSubtree` filters data-plane apply events by subtree prefix; it does not write each watch event into root. `RenameSubtree` advances the subtree authority frontier through this set of rooted handoff events.

`spec/SubtreeAuthority.tla` models these handoff semantics. The spec doesn't model dentry writes — only the authority records:

- `Primacy`: at most one active authority per subtree;
- `Inheritance`: successor frontier must cover predecessor frontier;
- `Silence`: a sealed authority's replies are no longer admissible;
- `Finality`: a sealed predecessor must be in pending handoff or closed.

The current `RenameSubtree` follows this spec by emitting rooted handoff events rather than inventing a rename-local state machine.

## 8. Snapshot epoch events

`SnapshotSubtree`'s essence isn't to copy data — it's to publish a stable read epoch.

Recommended events:

```text
SnapshotEpochPublished(snapshot_id, mount_id, subtree_root, read_ts, frontier)
SnapshotEpochRetired(snapshot_id)
```

Compact record:

```go
type SnapshotEpoch struct {
    SnapshotID string
    MountID string
    SubtreeRoot uint64
    ReadTS uint64
    Frontier uint64
    PublishedAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}
```

`read_ts` comes from coordinator TSO. `frontier` denotes the metadata frontier covered by the snapshot. Actual data reads still go through Percolator MVCC — we don't write file lists into root.

## 9. Quota fence events

Quota fence is implemented. The event is:

```text
QuotaFenceUpdated(subject, limit_bytes, limit_inodes, era, frontier)
```

Current semantics:

- rooted truth holds the quota limit and fence era;
- the data plane holds the usage counter key;
- the write path packs usage-counter mutation and dentry/inode mutation into the same Percolator transaction;
- gateway restart doesn't lose usage; multiple gateways serialize through Percolator conflict on the same usage key.

Don't write every usage increment/decrement into root. That would pollute authority truth with high-frequency data-plane counters.

## 10. The WatchSubtree vs rooted-event boundary

`WatchSubtree` is fsmeta's headline primitive, but its event stream is not RootEvent.

The two streams must stay separate:

| Stream | Source | Content | Persistence |
|---|---|---|---|
| RootEvent | `meta/root` | authority / lifecycle truth | durable, auditable |
| WatchEvent | raftstore apply hook | file/dentry mutation notifications | recoverable cursor, but not in root |

`WatchSubtree` can borrow the TailSubscription pattern from `meta/root`, but it must not push every file mutation into `meta/root`.

## 11. Implementation status

1. `StoreJoined` / `StoreRetired`: minimal rooted membership — done.
2. `MountRegistered` / `MountRetired`: fsmeta namespace registry — done.
3. `WatchSubtree`: runtime watch stream; no high-frequency root events added; ready / ack / replay all done.
4. `SnapshotEpochPublished` / `SnapshotEpochRetired`: MVCC snapshot epoch — done.
5. `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted`: RenameSubtree authority frontier — done.
6. `QuotaFenceUpdated`: rooted fence + data-plane usage counter — done.

The benefit of this ordering is already visible in code: we validated root-schema extension first, then runtime watch, then read-only snapshot, then handoff and quota.

## 12. Testing rules

Every new RootEvent must come with four classes of tests:

| Test | Location | Purpose |
|---|---|---|
| event constructor / clone | `meta/root/event` | payload doesn't alias |
| wire roundtrip | `meta/wire` | proto encoding doesn't drop fields |
| state materialization | `meta/root/state` | compact state is correct |
| coordinator bootstrap | `coordinator/integration` | runtime view restores from rooted snapshot |

If the event affects data-plane admission, also add a `raftstore/integration` test.

## 13. What we don't do

- Don't treat WatchEvent as RootEvent.
- Don't write runtime address, watcher, session, cache, or load into root.
- Don't keep the mount registry as truth in fsmeta local memory.
- Don't record specific dentry lists in snapshots.
- Don't expand each `RenameSubtree` dentry mutation into rooted events — root only records the authority handoff frontier.

## 14. Definition of done

After this umbrella, any new namespace primitive must answer:

1. Does it have rooted truth?
2. If yes, what's the event name; does the payload contain only truth fields?
3. What is its corresponding runtime view?
4. Does it relate to Eunomia / authority handoff?
5. Where do its four classes of tests live?

Can't answer? Don't write the code.
