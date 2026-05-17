\* Copyright 2024-2026 The NoKV Authors.
\* SPDX-License-Identifier: Apache-2.0

--------------------------- MODULE PerasVisibleCommit ---------------------------
EXTENDS Naturals

\* Bounded positive model for the Peras visible-commit lifecycle.
\*
\* The model covers one predecessor holder and its successor handoff. It keeps
\* the state machine small, but separates the safety boundaries that matter for
\* the runtime:
\*
\*   visible log -> visible ack -> optional witness quorum -> store install ->
\*   optional root seal -> runtime install -> visible applied marker ->
\*   visible log GC
\*
\* A visible record can exist without an ack. That is the crash window where the
\* WAL append completed but the caller did not observe success. Safety is about
\* acknowledged operations: an acked operation must keep either a visible-log
\* recovery source or a completed runtime install, and authority handoff cannot
\* skip those operations.

CONSTANTS
    MaxOp,
    WitnessRequiredOps,
    PublishRequiredOps

Ops == 1..MaxOp
Holders == {"A", "B"}

VARIABLES
    \* @type: Str;
    activeHolder,
    \* @type: Set(Int);
    visibleLog,
    \* @type: Set(Int);
    acked,
    \* @type: Set(Int);
    witnessed,
    \* @type: Set(Int);
    storeInstalled,
    \* @type: Set(Int);
    rootSealed,
    \* @type: Set(Int);
    runtimeInstalled,
    \* @type: Set(Int);
    appliedMarker

Vars ==
    << activeHolder, visibleLog, acked, witnessed, storeInstalled,
       rootSealed, runtimeInstalled, appliedMarker >>

Init ==
    /\ activeHolder = "A"
    /\ visibleLog = {}
    /\ acked = {}
    /\ witnessed = {}
    /\ storeInstalled = {}
    /\ rootSealed = {}
    /\ runtimeInstalled = {}
    /\ appliedMarker = {}

Unused(op) ==
    op \notin (visibleLog \cup acked \cup witnessed \cup storeInstalled \cup
               rootSealed \cup runtimeInstalled \cup appliedMarker)

LogVisible(op) ==
    /\ activeHolder = "A"
    /\ op \in Ops
    /\ Unused(op)
    /\ visibleLog' = visibleLog \cup {op}
    /\ UNCHANGED <<activeHolder, acked, witnessed, storeInstalled,
                   rootSealed, runtimeInstalled, appliedMarker>>

AckVisible(op) ==
    /\ activeHolder = "A"
    /\ op \in visibleLog
    /\ op \notin acked
    /\ acked' = acked \cup {op}
    /\ UNCHANGED <<activeHolder, visibleLog, witnessed, storeInstalled,
                   rootSealed, runtimeInstalled, appliedMarker>>

WitnessOrRecoverSegment(op) ==
    /\ op \in visibleLog
    /\ op \notin witnessed
    /\ witnessed' = witnessed \cup {op}
    /\ UNCHANGED <<activeHolder, visibleLog, acked, storeInstalled,
                   rootSealed, runtimeInstalled, appliedMarker>>

InstallStoreCatalog(op) ==
    /\ op \in visibleLog
    /\ (op \in WitnessRequiredOps => op \in witnessed)
    /\ op \notin storeInstalled
    /\ storeInstalled' = storeInstalled \cup {op}
    /\ UNCHANGED <<activeHolder, visibleLog, acked, witnessed,
                   rootSealed, runtimeInstalled, appliedMarker>>

PublishRootSeal(op) ==
    /\ op \in PublishRequiredOps
    /\ op \in storeInstalled
    /\ op \notin rootSealed
    /\ rootSealed' = rootSealed \cup {op}
    /\ UNCHANGED <<activeHolder, visibleLog, acked, witnessed,
                   storeInstalled, runtimeInstalled, appliedMarker>>

InstallRuntimeSegment(op) ==
    /\ op \in storeInstalled
    /\ (op \in PublishRequiredOps => op \in rootSealed)
    /\ op \notin runtimeInstalled
    /\ runtimeInstalled' = runtimeInstalled \cup {op}
    /\ UNCHANGED <<activeHolder, visibleLog, acked, witnessed,
                   storeInstalled, rootSealed, appliedMarker>>

MarkVisibleApplied(op) ==
    /\ op \in runtimeInstalled
    /\ op \notin appliedMarker
    /\ appliedMarker' = appliedMarker \cup {op}
    /\ UNCHANGED <<activeHolder, visibleLog, acked, witnessed,
                   storeInstalled, rootSealed, runtimeInstalled>>

CompactVisibleLog(op) ==
    /\ op \in appliedMarker
    /\ op \in visibleLog
    /\ visibleLog' = visibleLog \ {op}
    /\ UNCHANGED <<activeHolder, acked, witnessed, storeInstalled,
                   rootSealed, runtimeInstalled, appliedMarker>>

RetireAndHandoff ==
    /\ activeHolder = "A"
    /\ acked \subseteq runtimeInstalled
    /\ activeHolder' = "B"
    /\ UNCHANGED <<visibleLog, acked, witnessed, storeInstalled,
                   rootSealed, runtimeInstalled, appliedMarker>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ \E op \in Ops : LogVisible(op)
    \/ \E op \in Ops : AckVisible(op)
    \/ \E op \in Ops : WitnessOrRecoverSegment(op)
    \/ \E op \in Ops : InstallStoreCatalog(op)
    \/ \E op \in Ops : PublishRootSeal(op)
    \/ \E op \in Ops : InstallRuntimeSegment(op)
    \/ \E op \in Ops : MarkVisibleApplied(op)
    \/ \E op \in Ops : CompactVisibleLog(op)
    \/ RetireAndHandoff
    \/ Stutter

TypeOK ==
    /\ MaxOp \in Nat
    /\ MaxOp > 0
    /\ WitnessRequiredOps \subseteq Ops
    /\ PublishRequiredOps \subseteq Ops
    /\ PublishRequiredOps \subseteq WitnessRequiredOps
    /\ activeHolder \in Holders
    /\ visibleLog \subseteq Ops
    /\ acked \subseteq Ops
    /\ witnessed \subseteq Ops
    /\ storeInstalled \subseteq Ops
    /\ rootSealed \subseteq Ops
    /\ runtimeInstalled \subseteq Ops
    /\ appliedMarker \subseteq Ops

AckRequiresRecoverySource ==
    acked \subseteq (visibleLog \cup runtimeInstalled)

StoreInstallRequiresConfiguredWitness ==
    storeInstalled \cap WitnessRequiredOps \subseteq witnessed

RootSealRequiresStoreInstall ==
    rootSealed \subseteq storeInstalled

RootSealOnlyForPublishRequiredOps ==
    rootSealed \subseteq PublishRequiredOps

RuntimeInstallRequiresRequiredRootSeal ==
    runtimeInstalled \cap PublishRequiredOps \subseteq rootSealed

AppliedMarkerRequiresRuntimeInstall ==
    appliedMarker \subseteq runtimeInstalled

CompactionRequiresAppliedMarker ==
    acked \cap (Ops \ visibleLog) \subseteq appliedMarker

HandoffRequiresDrain ==
    activeHolder = "B" => acked \subseteq runtimeInstalled

PerasVisibleCommitGuarantees ==
    /\ AckRequiresRecoverySource
    /\ StoreInstallRequiresConfiguredWitness
    /\ RootSealRequiresStoreInstall
    /\ RootSealOnlyForPublishRequiredOps
    /\ RuntimeInstallRequiresRequiredRootSeal
    /\ AppliedMarkerRequiresRuntimeInstall
    /\ CompactionRequiresAppliedMarker
    /\ HandoffRequiresDrain

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
