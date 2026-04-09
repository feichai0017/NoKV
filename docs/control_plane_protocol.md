# Control-Plane Protocol

This note defines the next-stage protocol design for NoKV's control plane.

It focuses only on the contract between:

- `meta/root`
- `coordinator`

The purpose of this document is not to replace Raft or redesign the data plane.
The purpose is to make NoKV's existing control-plane behavior explicit,
testable, and evolvable.

The control plane should become protocolized around four ideas:

- `Freshness`
- `CatchUp`
- `Transition`
- `DegradedMode`

These four ideas already exist in partial form inside the implementation.
What is missing is a stable vocabulary, explicit invariants, and a rollout plan.

---

## 1. Intent

NoKV already has the right building blocks:

- rooted truth events
- checkpoint + committed tail
- watch-first tail subscription
- rebuildable `coordinator/catalog`
- explicit planned and terminal topology events

Today, these pieces mostly exist as implementation mechanics.
The control plane works, but many important semantics are still implicit:

- when a follower read is fresh enough
- when a follower must reload
- when retained tail catch-up is no longer enough
- what phase a topology change is in
- what "degraded" actually means to callers

The design goal is to turn these implicit behaviors into a formal protocol.

That protocol should be:

- small
- explicit
- observable
- testable
- compatible with the current architecture

---

## 2. Scope

This document covers control-plane protocol semantics only.

It defines the behavior of:

- rooted truth consumption
- control-plane view freshness
- rooted catch-up progression
- topology transition lifecycle
- degraded operating modes

It does **not** redefine:

- Raft replication
- Percolator / 2PC transaction semantics
- store-local recovery metadata
- the `raftstore` execution protocol

This document should be read as:

> durable truth + materialized control-plane view + formal serving contract

---

## 3. Protocol Objects

The naming set should remain compact, stable, and precise.

### 3.1 RootToken

`RootToken` is the rooted truth position already incorporated by some materialized view.

It is the control-plane equivalent of:

- "what truth have I already consumed?"

It should be treated as:

- monotonic
- comparable
- portable across control-plane nodes

`RootToken` is not just an internal storage cursor.
It is the anchor for:

- freshness
- catch-up state
- read eligibility
- transition causality

### 3.2 Freshness

`Freshness` is the serving contract attached to a read.

It answers:

- how fresh did the caller ask for?
- how fresh was the returned answer?

### 3.3 CatchUpState

`CatchUpState` describes how far one Coordinator node has converged on rooted truth.

It answers:

- can this node serve route reads?
- can it satisfy bounded-freshness reads?
- must it reload?
- must it install bootstrap?

### 3.4 Transition

`Transition` is one rooted topology change that moves through a formal lifecycle.

Examples:

- peer addition
- peer removal
- region split
- region merge
- region tombstone

`Transition` is not just a single event.
It is a causally tracked change with:

- identity
- source truth position
- phase
- progress

### 3.5 DegradedMode

`DegradedMode` is the externally visible restriction level of the control plane.

It answers:

- what kind of reads may still be served?
- are rooted writes currently allowed?
- should clients retry elsewhere?
- is the node usable only as a stale view?

---

## 4. Naming Set

The protocol should use one stable vocabulary across:

- API
- code
- logs
- metrics
- tests
- docs

### 4.1 Read classes

- `Strong`
  - requires leader-grade freshness
- `Bounded`
  - allows follower service within explicit lag limits
- `BestEffort`
  - allows stale cache service

These names are short and carry clear serving intent.

### 4.2 Catch-up actions

- `Reload`
  - rebuild catalog from rooted storage
- `Advance`
  - acknowledge rooted tail progress without a full rebuild
- `Bootstrap`
  - install a fresh checkpoint because retained tail is insufficient
- `Reject`
  - deny freshness-sensitive reads until convergence improves

### 4.3 Catch-up states

- `Fresh`
- `Lagging`
- `BootstrapRequired`
- `Recovering`
- `Unavailable`

### 4.4 Transition phases

Current protocol v1 keeps the phase set intentionally small.

- `Planned`
- `Admitted`
- `Completed`
- `Conflicted`
- `Superseded`
- `Cancelled`
- `Aborted`

`Applied`, `Published`, and `Stalled` remain useful future concepts, but they
are not part of the current protocol surface yet.

### 4.5 Degraded modes

- `Healthy`
- `CoordinatorDegraded`
- `RootLagging`
- `RootUnavailable`
- `ViewOnly`

`ViewOnly` is deliberately chosen over more vague names like `ExecutionOnly`.
This document only defines control-plane behavior, so the right question is:

> can this node still expose a stale view?

---

## 5. Freshness Contract

The control plane should stop treating all successful reads as equivalent.

Every control-plane read should:

1. declare the requested freshness class
2. optionally declare a rooted lower bound
3. receive an explicit served freshness result

### 5.1 Why this matters

Today, follower reads are effectively:

> "good enough if the follower recently reloaded and is not too far behind"

That is practical, but not a protocol.

Without a formal freshness contract:

- clients cannot reason about route read quality
- tests cannot assert serving guarantees precisely
- degraded modes remain guesswork
- control-plane correctness is partly hidden in implementation details

### 5.2 Request fields

Control-plane read RPCs should be able to express:

- `freshness`
  - `Strong`, `Bounded`, or `BestEffort`
- `required_root_token`
  - optional lower bound on rooted truth already incorporated
- `max_root_lag`
  - optional bound on acceptable rooted lag

Not every caller will need all three fields.
But the protocol should have room for them.

### 5.3 Response fields

Control-plane read RPCs should return:

- `served_root_token`
- `served_freshness`
- `served_by_leader`
- `degraded_mode`

Optional future fields:

- `root_lag`
- `freshness_reason`

### 5.4 Serving rules

#### `Strong`

Should be served only when:

- the node is rooted leader
- and the serving catalog has incorporated at least the requested `RootToken`

If this is not true, the server should reject rather than silently downgrade.

#### `Bounded`

May be served by a follower when:

- the node is not in `BootstrapRequired`, `Recovering`, or `Unavailable`
- and lag is within declared bounds
- and the served token satisfies `required_root_token` if one was requested

If bounds cannot be satisfied, the server should reject rather than silently serve stale data.

#### `BestEffort`

May be served from the current materialized catalog so long as:

- the catalog exists
- the node is not fully unavailable

This class exists to make stale service explicit instead of accidental.

### 5.5 First rollout target

The first RPC that should adopt this contract is:

- `GetRegionByKey`

That gives the system a clear, high-value place to prove the model before wider rollout.

---

## 6. Rooted Catch-Up Protocol

NoKV already has a good catch-up substrate:

- checkpoint
- committed tail
- watch-first subscription
- bootstrap install when retained tail is insufficient

The next step is to give that behavior a formal state machine.

### 6.1 Catch-up state definitions

#### `Fresh`

The node's materialized catalog is sufficiently close to rooted truth to serve:

- `Bounded`
- `BestEffort`

and, if leader, possibly `Strong`.

#### `Lagging`

The node is behind, but still within retained-tail recovery range.

This means:

- further rooted tail observation may repair the gap
- bootstrap install is not yet mandatory
- some bounded reads may need to be rejected

#### `BootstrapRequired`

The node is too far behind for retained tail replay.

This means:

- a plain reload from retained tail is not sufficient
- a new checkpoint/bootstrap install is required
- freshness-sensitive reads should be rejected

#### `Recovering`

The node is actively rebuilding its materialized control-plane view.

This means:

- catalog may be in transition
- only explicitly allowed stale reads may be served

#### `Unavailable`

The node cannot presently produce a valid control-plane view.

This means:

- no rooted freshness contract can be satisfied
- the server should fail reads except possibly future explicit diagnostics

### 6.2 Catch-up actions

#### `Reload`

Used when rooted truth advanced in a way that requires rebuilding the materialized catalog.

#### `Advance`

Used when rooted tail progressed, but the catalog does not need a full rebuild.

#### `Bootstrap`

Used when the node must install a checkpoint because retained tail can no longer bridge the gap.

#### `Reject`

Used when the node should refuse freshness-sensitive serving until it converges further.

### 6.3 Protocol outputs

The rooted subscription path should eventually expose a structured result like:

- `root_token_before`
- `root_token_after`
- `catch_up_state`
- `catch_up_action`
- `reload_required`
- `bootstrap_required`

### 6.4 Why protocolizing this matters

Without explicit catch-up semantics:

- tests can only assert indirect effects
- follower-read serving policy stays implicit
- degraded-mode logic gets duplicated
- future clients cannot reason about retries properly

This is one of the strongest places for NoKV to become distinctive.

---

## 7. Transition Lifecycle Protocol

NoKV already records rooted topology intent and rooted completion.
That is the start of a lifecycle, not yet the full protocol.

The next stage is to make transition tracking first-class.

### 7.1 Transition identity

Every topology transition should have a stable `TransitionID`.

`TransitionID` should be:

- deterministic
- durable
- safe to log, surface, and test against

It should not require callers to infer identity from:

- region ID
- event kind
- timing

alone.

### 7.2 Transition source

Every transition should record:

- source rooted epoch or token
- target topology intent
- the event that created it

This makes causality explicit:

- what truth position created this transition?
- what later truth position superseded it?

### 7.3 Phase definitions

#### `Planned`

The rooted lifecycle assessment says the transition exists as an intended
topology change, but the operator runtime has not yet admitted it for forward
progress.

This is the phase used by:

- `AssessRootEvent`
- `PublishRootEventResponse.assessment`

#### `Admitted`

The rooted transition is currently pending or open, and the operator runtime has
admitted it for execution progress.

This is the phase used by:

- `ListTransitions`

It is intentionally runtime-facing. It does not appear in
`PublishRootEventResponse.assessment`, because that response reports a
pre-persist lifecycle assessment rather than post-admission runtime state.

#### `Completed`

The rooted lifecycle says the requested transition target is already satisfied.
For a plan event, this usually means the requested topology is already present.

#### `Cancelled`

The rooted lifecycle says the requested transition target was cancelled.

#### `Conflicted`

The rooted lifecycle says a different pending transition already owns progress
for the same target.

#### `Superseded`

The rooted lifecycle says a newer rooted topology already superseded this
transition target.

#### `Aborted`

The rooted lifecycle says an apply or terminal event does not match the current
pending rooted target.

### 7.4 Why lifecycle matters

A formal lifecycle enables:

- clear scheduling decisions
- proper retry/backoff
- stuck transition recovery
- operator runtime clarity
- precise testing around publish boundaries

Without it, the system keeps relying on partial signals scattered across:

- rooted events
- in-memory views
- runtime heuristics

---

## 8. Degraded Semantics

NoKV already has some degraded behavior:

- followers serve stale route views
- route cache may survive Coordinator outages
- scheduler paths may degrade

These behaviors should become explicit protocol states.

### 8.1 Mode definitions

#### `Healthy`

Normal serving mode.

Rooted truth, catalog freshness, and serving guarantees are all within policy.

#### `CoordinatorDegraded`

The Coordinator process is alive, but not all control-plane functions can be performed normally.

Examples:

- partial RPC surface availability
- write restrictions while leadership is unsettled

#### `RootLagging`

Rooted truth exists, but this node's materialized catalog is behind allowed freshness bounds.

This is not full unavailability.
It is a serving restriction mode.

#### `RootUnavailable`

The rooted backend cannot currently provide enough truth to support valid control-plane service.

In this mode:

- truth-sensitive reads fail
- rooted writes fail
- diagnostics may still be exposed

#### `ViewOnly`

The node may still expose a stale materialized catalog, but cannot satisfy freshness-sensitive contracts.

This mode is useful because it makes "stale but useful" explicit.

### 8.2 Why this should be formal

Without explicit degraded modes, callers only see:

- transport failure
- `not leader`
- `route unavailable`

Those errors do not express the actual system state.

A real degraded protocol lets callers answer:

- retry elsewhere?
- retry later?
- accept stale?
- fail fast?

### 8.3 Relationship to freshness

`DegradedMode` and `Freshness` are related but not identical.

- `Freshness` is the contract requested and served for one read
- `DegradedMode` is the broader operating condition of the serving node

A node may be:

- `Healthy` and still reject a `Strong` read because it is not leader
- `RootLagging` and still serve `BestEffort`
- `ViewOnly` and still serve diagnostics

That distinction should remain sharp.

---

## 9. API Direction

The most valuable first implementation step is at the Coordinator RPC boundary.

### 9.1 Read-side API direction

Read APIs should conceptually grow:

- `freshness`
- `required_root_token`
- `max_root_lag`

Read responses should conceptually expose:

- `served_root_token`
- `served_freshness`
- `degraded_mode`
- `served_by_leader`

### 9.2 Write-side API direction

Leader-only writes should remain leader-only.

Write requests should continue to require:

- rooted leadership
- expected cluster epoch where applicable

Write responses should eventually expose:

- `accepted_root_token`
- `transition_id` where topology change is involved

This makes a write result more precise than:

- `accepted = true`

### 9.3 Diagnostics API direction

The control plane will likely also benefit from an explicit diagnostics surface.

Conceptually, that should expose:

- current rooted token
- catalog rooted token
- catch-up state
- degraded mode
- leader identity knowledge
- lag estimate

This may become:

- a dedicated diagnostics RPC
- metrics
- CLI output

or all three.

---

## 10. Storage and Catalog Direction

To support the protocol above, the Coordinator catalog should become rooted-token aware.

At minimum, the materialized control-plane view should track:

- `catalog_root_token`
- `catalog_updated_at`
- `catch_up_state`
- `degraded_mode`

Optional future metadata:

- `root_lag`
- `last_reload_reason`
- `leader_observed`

### 10.1 Ownership rule

This design does **not** change truth ownership.

The ownership line remains:

- `meta/root` owns durable truth
- `coordinator/catalog` owns materialized serving state

The catalog should become more informative, not more authoritative.

### 10.2 Materialization rule

The catalog must remain:

- rebuildable
- discardable
- follower-local

It should never become a second durable truth source.

That is a core invariant.

---

## 11. Invariants

This protocol should preserve the following invariants.

### 11.1 Truth ownership invariant

Only `meta/root` owns durable control-plane truth.

### 11.2 Materialization invariant

`coordinator/catalog` is always derived state, never authority.

### 11.3 Monotonic token invariant

The materialized rooted token of one node must never move backward.

### 11.4 No silent downgrade invariant

If a caller requests `Strong` or bounded freshness and the node cannot satisfy it,
the server should reject rather than silently serve `BestEffort`.

### 11.5 Explicit stale service invariant

If stale service is allowed, the response should say so explicitly.

### 11.6 Transition identity invariant

Every control-plane transition must be referencable as a stable object, not just inferred from event timing.

---

## 12. Rollout Plan

The rollout should stay incremental.

### Phase 1: Freshness

Implement explicit freshness semantics on route reads.

Target outcomes:

- `GetRegionByKey` can express requested freshness
- route responses disclose served freshness and rooted token
- follower-read behavior stops being implicit

### Phase 2: Catch-Up

Make convergence state explicit in rooted storage and Coordinator runtime.

Target outcomes:

- `CatchUpState`
- `CatchUpAction`
- formal bootstrap-required boundary
- rooted lag awareness in serving decisions

### Phase 3: Transition

Introduce stable transition identity and explicit phases.

Target outcomes:

- durable `TransitionID`
- lifecycle-aware operator/runtime view
- explicit phase semantics across:
  - `ListTransitions`
  - `AssessRootEvent`
  - `PublishRootEvent`

### Phase 4: DegradedMode

Expose degraded operating modes through API, metrics, and tests.

Target outcomes:

- explicit degraded semantics
- cleaner retry policy
- cleaner CLI/operator diagnostics

---

## 13. What Not To Do

The following are intentionally out of scope for this line of work:

- inventing a new general-purpose consensus algorithm
- replacing Raft in the mainline system
- redesigning 2PC before control-plane semantics are explicit
- collapsing rooted truth and catalog into one mixed layer
- treating stale follower service as an undocumented optimization

NoKV's control-plane innovation should come from stronger semantics and clearer
ownership, not from unnecessary reinvention of already mature primitives.

---

## 14. Current Practical Naming Guidance

If this protocol starts landing in code, the implementation should prefer:

- `RootToken`
- `Freshness`
- `CatchUpState`
- `CatchUpAction`
- `TransitionID`
- `TransitionPhase`
- `DegradedMode`

Avoid reintroducing weaker names like:

- `state kind`
- `stale mode`
- `sync status`
- `reload reason` as the primary protocol object

Those may still exist as helper fields, but the public model should stay anchored
to the smaller protocol vocabulary above.
