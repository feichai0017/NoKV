# Control-Plane and Execution-Plane Protocols

This note defines NoKV's protocol line for:

- the `control plane`
- the `execution plane`

and the next-stage evolution around them.

The current implementation status is intentionally minimal but no longer just a
design sketch:

- `control-plane protocol v1` is implemented and exposed through Coordinator
  RPCs plus `meta/root` storage semantics
- `execution-plane protocol v1` is implemented as a store-local contract with a
  small admin diagnostics API surface

The point of this document is to keep those two lines coordinated instead of
letting them drift into separate, implicit rule sets.

The control plane focuses on the contract between:

- `meta/root`
- `coordinator`

The execution plane focuses on the contract between:

- `coordinator`
- `raftstore`
- local durable state (`raftstore/localmeta`, raft log, restart replay)

The purpose of this document is not to replace Raft or redesign the data plane.
The purpose is to make NoKV's existing cross-plane behavior explicit, testable,
and evolvable.

The control plane is protocolized around four ideas:

- `Freshness`
- `CatchUp`
- `Transition`
- `DegradedMode`

These four ideas already existed in partial form inside the implementation.
The current work turns them into a stable vocabulary, explicit invariants, and a
clear rollout line.

The execution plane is protocolized around four matching ideas:

- `Admission`
- `ExecutionTarget`
- `PublishBoundary`
- `RestartState`

---

## 0. Current Status

The control plane now has a **minimal implemented v1**.

Implemented and exposed through `pb/coordinator/coordinator.proto`,
`coordinator/server`, `coordinator/rootview`, and tests:

- route-read `Freshness`
- `RootToken`
- `root_lag`
- `DegradedMode`
- `CatchUpState`
- `TransitionID`
- `PublishRootEventResponse.assessment` as a **pre-persist lifecycle assessment**

This means the protocol is no longer only a design direction. It is already the
formal serving contract for key Coordinator APIs.

Not implemented in v1:

- richer transition phases such as `Published` / `Stalled`
- a fuller catch-up action surface exposed through API
- automatic recovery policy derived from protocol state
- broad client-side policy that consumes every protocol field

So the right description today is:

> control-plane protocol v1 is implemented and in use, while richer
> scheduler/runtime policy is not implemented in v1.

The execution plane is in a different state.

Today, `raftstore` has a **minimal implemented v1** inside `raftstore/store`.

Already implemented and exercised through store-local types, `raftstore/admin`,
runtime state, and tests:

- explicit `Admission` classes and reasons on read / write / topology entry
  points
- explicit topology `ExecutionOutcome`
- explicit topology `PublishState`
- explicit `RestartState` derived from `raftstore/localmeta` + raft replay
  pointers
- terminal publish failures retained as visible retry state instead of silent
  drop
- admin diagnostics exposure through `pb/admin/admin.proto` `ExecutionStatus`

Not implemented as first-class execution protocol fields yet:

- request validation and routing
- context propagation
- detailed local leader admission diagnostics
- detailed per-attempt scheduler retry/backoff policy
- metrics for planned truth -> execute -> terminal truth latency
- richer degraded local scheduler states

The current landing is still mostly store-local and spread across:

- `raftstore/store`
- `raftstore/peer`
- `raftstore/raftlog`
- `raftstore/localmeta`

So the right description there is:

> execution-plane protocol v1 now exists as a minimal named runtime contract,
> with store-local state and admin-visible diagnostics, while broader metrics,
> policy, and richer executor states are not implemented in v1.

---

## 1. Intent

NoKV already has the right building blocks:

- rooted truth events
- checkpoint + committed tail
- watch-first tail subscription
- rebuildable `coordinator/catalog`
- explicit planned and terminal topology events

Before v1, these pieces mostly existed as implementation mechanics.
The control plane now has a formal minimum contract, while several policy
extensions remain intentionally outside v1:

- when a follower read is fresh enough
- when a follower must reload
- when retained tail catch-up is no longer enough
- what phase a topology change is in
- what "degraded" actually means to callers

The design goal is to keep turning these implicit behaviors into a formal
protocol.

That protocol should be:

- small
- explicit
- observable
- testable
- compatible with the current architecture

---

## 2. Scope

This document covers both planes, but not at the same implementation depth.

For the control plane, it defines the behavior of:

- rooted truth consumption
- control-plane view freshness
- rooted catch-up progression
- topology transition lifecycle
- degraded operating modes

For the execution plane, it defines the protocol direction for:

- request admission
- transition execution
- terminal truth publication
- restart and local recovery alignment
- degraded local behavior around scheduler / queue / publish boundaries

It does **not** redefine:

- Raft replication
- Percolator / 2PC transaction semantics
- store-local recovery metadata
- storage-engine internals unrelated to distributed lifecycle

This document should be read as two linked contracts:

> control plane = durable truth + materialized view + serving contract

> execution plane = admitted work + local execution + publish/restart contract

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

### 4.4 Degraded modes

- `Healthy`
- `CoordinatorDegraded`
- `RootLagging`
- `RootUnavailable`
- `ViewOnly`

`ViewOnly` is deliberately chosen over more vague names like `ExecutionOnly`.
This section only defines control-plane behavior, so the right question is:

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

NoKV already has a good catch-up foundation:

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
topology change, but the scheduler/control-plane runtime has not yet admitted it for forward
progress.

This is the phase used by:

- `AssessRootEvent`
- `PublishRootEventResponse.assessment`

#### `Admitted`

The rooted transition is currently pending or open, and the scheduler/control-plane runtime has
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
- scheduler/control-plane runtime clarity
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

### 8.4 Current coordinator contract

The current implementation already enforces a concrete degraded-mode contract at
the Coordinator RPC boundary.

#### Metadata reads (`GetRegionByKey`)

- `Freshness=BEST_EFFORT`
  - serves from the local materialized catalog even when `meta/root` is
    currently unavailable
  - returns `degraded_mode=ROOT_UNAVAILABLE` when the rooted snapshot cannot be
    reloaded
  - returns `degraded_mode=ROOT_LAGGING` when the local catalog trails rooted
    truth
- `Freshness=BOUNDED`
  - rejects when `meta/root` is unavailable
  - rejects when `root_lag > max_root_lag`
  - rejects when catch-up is still `BOOTSTRAP_REQUIRED`
- `Freshness=STRONG`
  - rejects on followers
  - rejects whenever `root_lag > 0`
  - rejects when `meta/root` is unavailable

In all cases, successful replies carry the current answerability witness:

- `served_root_token`
- `current_root_token`
- `root_lag`
- `catch_up_state`
- `degraded_mode`
- `serving_class`
- `sync_health`

#### Duty-gated writes (`AllocID`, `TSO`, scheduler decisions)

These do **not** have a degraded fallback.

- the local coordinator must first campaign / renew the rooted lease
- the rooted lease must still be active for the local holder
- the rooted generation must not already be sealed
- the rooted duty mask must admit the requested action

If any of those fail, the request is rejected instead of falling back to stale
local state. This is the current boundary between:

- read-path degradation
- write-path fail-stop admission

#### Lifecycle mutations (`Seal`, `Confirm`, `Close`, `Reattach`)

Lifecycle mutations are stricter than hot-path duty admission:

- they always re-read rooted state from storage before mutating
- they reject any stale-holder / expired-lease / sealed-generation view
- they treat closure completeness as a rooted safety condition, not a best-effort hint

That is why seal / confirm / close / reattach do not use the cached mirror
admission path.

### 8.5 Operational diagnostics

`DiagnosticsSnapshot()` now exports both:

- the current degraded serving state (`root`, `lease`, `audit`, `closure_witness`)
- cumulative CCC counters under `ccc_metrics`

`ccc_metrics` is grouped into:

- `lease_generation_transitions_total`
- `closure_stage_transitions_total`
- `pre_action_gate_rejections_total`
- `ali_violations_total`

The `ali_violations_total` buckets map to the four Authority Lineage
Invariants:

- `authority_uniqueness`
- `successor_coverage`
- `post_seal_inadmissibility`
- `closure_completeness`

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

## 12. Rollout State

The rollout stays incremental, but the first protocol line is already in use.

### Phase 1: Freshness

Status: **implemented**

Delivered outcomes:

- `GetRegionByKey` can express requested freshness
- route responses disclose served freshness and rooted token
- follower-read behavior is no longer implicit

### Phase 2: Catch-Up

Status: **minimal v1 implemented**

Delivered outcomes:

- `CatchUpState`
- formal bootstrap-required boundary
- rooted lag awareness in serving decisions

Still open:

- a wider public `CatchUpAction` surface
- more explicit recovery diagnostics

### Phase 3: Transition

Status: **minimal v1 implemented**

Delivered outcomes:

- durable `TransitionID`
- explicit phase semantics across:
  - `ListTransitions`
  - `AssessRootEvent`
  - `PublishRootEvent`
- publish-time pre-persist lifecycle assessment

Still open:

- richer runtime phases
- stuck / timeout diagnosis

### Phase 4: DegradedMode

Status: **minimal v1 implemented**

Delivered outcomes:

- explicit degraded semantics in route responses
- route-serving rejection under rooted lag / rooted unavailability

Still open:

- broader surfacing through metrics and diagnostics
- tighter client retry policy based on degraded state

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
- `DegradedMode`

For execution-plane work, prefer:

- `Admission`
- `ExecutionTarget`
- `ExecutionOutcome`
- `PublishState`
- `RestartState`

Avoid reintroducing weaker names like:

- `state kind`
- `stale mode`
- `sync status`
- `reload reason` as the primary protocol object

Those may still exist as helper fields, but the public model should stay anchored
to the smaller protocol vocabulary above.

---

## 15. Execution-Plane Protocol

The execution plane is the contract between:

- `raftstore`
- local leader peer runtime
- local durable recovery state
- the control-plane publish boundary

Its job is different from the control plane.

The control plane answers:

- what topology truth exists?
- how fresh is the served view?
- what transition lifecycle is visible globally?

The execution plane answers:

- may this request enter local execution now?
- what target is being executed?
- how far has local execution progressed?
- has terminal truth been published yet?
- what state is safe to recover after restart?

### 15.1 Why this matters

Without an explicit execution-plane protocol, the system keeps important
distributed safety semantics hidden in code paths such as:

- request validation and cancellation
- queue admission and local degradation
- planned truth publication before local execution
- terminal truth publication after local apply
- restart reconciliation between `localmeta`, raft durable state, and Coordinator

Those are not low-level implementation details. They are correctness
boundaries.

### 15.2 Protocol objects

The execution plane should be formalized around the following objects.

#### `Admission`

`Admission` is the local decision about whether one request may enter execution.

It should answer:

- is the local peer leader?
- is the region epoch valid?
- is the peer hosted and runnable?
- is the request cancelled or timed out already?
- is the queue or scheduler allowed to accept more work?

The important design rule is that admission must be explicit, not an accidental
mix of local checks and fallback retries.

#### `ExecutionTarget`

`ExecutionTarget` is the concrete unit of work the execution plane is trying to
carry out.

Examples:

- one read command
- one raft write proposal
- one peer change target
- one split target
- one merge target

For topology changes, `ExecutionTarget` must remain causally tied to the rooted
transition object created by the control plane.

#### `ExecutionOutcome`

`ExecutionOutcome` is the local state reached by an admitted target.

Minimal useful states are:

- `Rejected`
- `Queued`
- `Proposed`
- `Committed`
- `Applied`
- `Failed`

This is the minimum needed to stop conflating "accepted by API", "replicated by
raft", and "applied to local state".

#### `PublishState`

`PublishState` tracks the boundary between local apply and control-plane truth
publication.

This is a first-class boundary in NoKV's architecture:

- planned truth is published before execution
- terminal truth is published after local apply

The protocol must therefore distinguish:

- `NotRequired`
- `Pending`
- `Published`
- `PublishFailed`

This is the exact boundary where split/merge/peer-change correctness otherwise
turns into invisible best-effort behavior.

#### `RestartState`

`RestartState` describes whether one store can safely resume from local durable
state.

It should answer:

- is local peer metadata self-consistent?
- is the local raft replay pointer usable?
- does the store need Coordinator catch-up only, or local rebuild first?
- is startup safe, degraded, or fatal?

This object exists to stop restart behavior from being an implicit composition
of:

- `raftstore/localmeta`
- raft log replay
- ad hoc bootstrap logic

### 15.3 Request classes and admission

Execution-plane v1 should start by distinguishing three request classes:

- `Read`
  - local leader read admission
  - read-index / wait-applied preconditions
  - cancellation and deadline propagation
- `Write`
  - raft proposal admission
  - proposal tracking through commit/apply
  - retryable local rejection vs fatal local rejection
- `Topology`
  - peer change
  - split
  - merge
  - explicit coupling to planned and terminal rooted truth

These classes do not need separate RPC protocols, but they do need stable
admission outcomes. At minimum, those outcomes should distinguish:

- `NotLeader`
- `EpochMismatch`
- `NotHosted`
- `Canceled`
- `TimedOut`
- `QueueSaturated`
- `SchedulerDegraded`
- `Accepted`

Without this line, request behavior remains split across store-local branches
instead of becoming one coherent executor contract.

### 15.4 Publish lifecycle

Execution-plane v1 should also make the publish boundary explicit for topology
work.

The minimal lifecycle is:

1. `PlannedPublished`
2. `LocallyExecuting`
3. `Applied`
4. `TerminalPublishPending`
5. `TerminalPublished`
6. `TerminalPublishFailed`

The important rule is that `Applied` and `TerminalPublished` are different
states. Local execution success does not mean global lifecycle completion until
terminal truth is durably published.

This is the boundary that should align:

- `raftstore/store/transition_builder.go`
- `raftstore/store/transition_executor.go`
- `raftstore/store/transition_outcome.go`
- `raftstore/store/scheduler_runtime.go`

### 15.5 First landing points

Execution-plane protocol v1 landed first in the places that already carried
the boundary implicitly:

- `raftstore/store/command_ops.go`
  - request admission and context semantics
- `raftstore/store/command_pipeline.go`
  - request lifecycle states visible to callers
- `raftstore/store/scheduler_runtime.go`
  - queue overflow / degraded local behavior
- `raftstore/store/transition_builder.go`
  - execution target construction from rooted truth
- `raftstore/store/transition_executor.go`
  - local execution and apply boundary
- `raftstore/store/transition_outcome.go`
  - terminal truth publication result
- `raftstore/localmeta`
  - restart state and local recovery truth

These files still do not expose a new public API. But they now share one
explicit local protocol vocabulary instead of inventing those semantics
independently.

### 15.6 Execution invariants

The execution-plane protocol should preserve the following invariants.

#### `Admission` invariant

Every externally visible rejection should map to a stable admission reason, not
only a transport error or generic retry exhaustion.

#### `No skipped publish boundary` invariant

If local apply completed but terminal truth publication did not, the system
must surface that state explicitly. It must not be silently treated as fully
complete.

#### `Restart truth boundary` invariant

Restart must derive hosted peer truth from local durable state, not from
bootstrap config. Static config may resolve addresses, but must not overwrite
runtime truth.

#### `No hidden drop` invariant

Queue overflow, scheduler degradation, and publish retry loss must be explicit
protocol states or metrics-backed outcomes, not silent local behavior.

### 15.7 Minimal rollout target

Execution-plane protocol v1 started small.

The minimum useful delivered line is now:

1. request admission
2. topology execution outcome
3. publish boundary state
4. restart state

That is enough to formalize the most dangerous boundaries without trying to
protocolize every internal raft detail.

---

## 16. Priority and Rollout Order

The next protocol work should avoid widening either protocol until the current
v1 contracts stay small, observable, and well tested.

### 16.1 What is implemented now

The control plane has a minimal, externally visible contract:

- freshness classes
- rooted token / lag
- degraded serving state
- transition identity

The execution plane now has a minimal internal contract:

- admission class / reason
- topology outcome
- publish state
- restart state
- admin-visible `ExecutionStatus`

That is enough for v1. It gives tests and operators names for the important
boundaries without turning `raftstore` into a policy engine.

### 16.2 What should not happen next

The wrong next step would be to keep enriching lifecycle phases and diagnostic
fields before the existing v1 state proves stable under recovery and
integration tests.

That would create a vocabulary mismatch:

- control plane claims richer transition semantics than the executor can act on
- execution plane reports more states than the coordinator can use safely

### 16.3 Recommended order

1. Keep control-plane v1 and execution-plane v1 narrow.
2. Add tests around the existing publish/restart/admission states before adding
   new states.
3. Only then tighten control-plane v1 toward richer scheduler/runtime phases.

In short:

> stabilize both v1 contracts first, then deepen scheduler/runtime semantics.
