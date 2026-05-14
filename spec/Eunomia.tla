\* Copyright 2024-2026 The NoKV Authors.
\* SPDX-License-Identifier: Apache-2.0

------------------------------ MODULE Eunomia ------------------------------
EXTENDS Naturals, FiniteSets

\* Bounded positive model for the current Eunomia grant protocol.
\*
\* The model intentionally ignores the underlying consensus protocol. The
\* root log is represented by these committed state transitions:
\*
\*   IssueGrant        == GrantIssued
\*   SealExact         == GrantSealed(sealed_exact)
\*   RetireExpired     == GrantRetired(expired_bound)
\*   InheritRetirement == GrantInherited
\*
\* Each authority reply carries evidence from the admitted grant:
\*
\*   era, reply usage, evidence usage, and observed retired floor.
\*
\* This is a single monotone-duty abstraction. alloc_id and tso map directly to
\* the monotone bound. region_lookup uses the same shape with descriptor
\* revision as the monotone coordinate; the implementation adds root-token and
\* storage-epoch checks around that duty.

CONSTANTS
    \* @type: Int;
    MaxEra,
    \* @type: Int;
    MaxFrontier,
    \* @type: Int;
    MaxInflight

Eras          == 1..MaxEra
Frontiers     == 0..MaxFrontier
NoGrant       == MaxEra + 1
RetireModes   == {"none", "sealed_exact", "expired_bound"}
ReplySet      == { [era |-> e,
                    usage |-> u,
                    evidenceUsage |-> eu,
                    observedFloor |-> f] :
                    e \in Eras,
                    u \in Frontiers,
                    eu \in Frontiers,
                    f \in 0..MaxEra }
NoAccepted    == [valid |-> FALSE,
                  era |-> 0,
                  usage |-> 0,
                  evidenceUsage |-> 0,
                  floorAtAccept |-> 0,
                  observedFloor |-> 0]

VARIABLES
    \* @type: Set(Int);
    issued,
    \* @type: Int;
    active,
    \* @type: Int -> Int;
    bound,
    \* @type: Int -> Int;
    served,
    \* @type: Set(Int);
    retired,
    \* @type: Int -> Int;
    retirementBound,
    \* @type: Int -> Str;
    retirementMode,
    \* @type: Int -> Int;
    inheritedBy,
    \* @type: Int;
    retiredFloor,
    \* @type: Int;
    verifierFloor,
    \* @type: Set([era: Int, usage: Int, evidenceUsage: Int, observedFloor: Int]);
    inflight,
    \* @type: [valid: Bool, era: Int, usage: Int, evidenceUsage: Int, floorAtAccept: Int, observedFloor: Int];
    accepted

Vars ==
    << issued, active, bound, served, retired, retirementBound,
       retirementMode, inheritedBy, retiredFloor, verifierFloor, inflight,
       accepted >>

Init ==
    /\ issued = {}
    /\ active = NoGrant
    /\ bound = [e \in Eras |-> 0]
    /\ served = [e \in Eras |-> 0]
    /\ retired = {}
    /\ retirementBound = [e \in Eras |-> 0]
    /\ retirementMode = [e \in Eras |-> "none"]
    /\ inheritedBy = [e \in Eras |-> NoGrant]
    /\ retiredFloor = 0
    /\ verifierFloor = 0
    /\ inflight = {}
    /\ accepted = NoAccepted

NextEra == Cardinality(issued) + 1

PendingRetired ==
    { e \in retired : inheritedBy[e] = NoGrant }

Max(a, b) ==
    IF a >= b THEN a ELSE b

IssueGrant ==
    /\ active = NoGrant
    /\ NextEra \in Eras
    /\ \E b \in Frontiers:
        /\ \A p \in PendingRetired:
            b >= retirementBound[p]
        /\ issued' = issued \cup {NextEra}
        /\ active' = NextEra
        /\ bound' = [bound EXCEPT ![NextEra] = b]
        /\ served' = [served EXCEPT ![NextEra] = 0]
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<retired, retirementBound, retirementMode, inheritedBy,
                   retiredFloor, verifierFloor, inflight>>

ServeReply ==
    /\ active # NoGrant
    /\ Cardinality(inflight) < MaxInflight
    /\ \E u \in Frontiers:
        \E eu \in Frontiers:
            /\ u <= eu
            /\ eu <= bound[active]
            /\ inflight' = inflight \cup
                {[era |-> active,
                  usage |-> u,
                  evidenceUsage |-> eu,
                  observedFloor |-> retiredFloor]}
            /\ served' = [served EXCEPT ![active] = Max(served[active], u)]
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<issued, active, bound, retired, retirementBound,
                   retirementMode, inheritedBy, retiredFloor, verifierFloor>>

DeliverAcceptedReply ==
    /\ \E r \in inflight:
        /\ r.usage <= r.evidenceUsage
        /\ r.evidenceUsage <= bound[r.era]
        /\ r.era > verifierFloor
        /\ r.era > r.observedFloor
        /\ inflight' = inflight \ {r}
        /\ accepted' = [valid |-> TRUE,
                        era |-> r.era,
                        usage |-> r.usage,
                        evidenceUsage |-> r.evidenceUsage,
                        floorAtAccept |-> verifierFloor,
                        observedFloor |-> r.observedFloor]
        /\ verifierFloor' = Max(verifierFloor, r.observedFloor)
    /\ UNCHANGED <<issued, active, bound, served, retired, retirementBound,
                   retirementMode, inheritedBy, retiredFloor>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<issued, active, bound, served, retired, retirementBound,
                   retirementMode, inheritedBy, retiredFloor, verifierFloor>>

ClearAccepted ==
    /\ accepted.valid
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<issued, active, bound, served, retired, retirementBound,
                   retirementMode, inheritedBy, retiredFloor, verifierFloor,
                   inflight>>

SealExact ==
    /\ active # NoGrant
    /\ active \notin retired
    /\ retired' = retired \cup {active}
    /\ retirementBound' = [retirementBound EXCEPT ![active] = served[active]]
    /\ retirementMode' = [retirementMode EXCEPT ![active] = "sealed_exact"]
    /\ active' = NoGrant
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<issued, bound, served, inheritedBy, retiredFloor,
                   verifierFloor, inflight>>

RetireExpired ==
    /\ active # NoGrant
    /\ active \notin retired
    /\ retired' = retired \cup {active}
    /\ retirementBound' = [retirementBound EXCEPT ![active] = bound[active]]
    /\ retirementMode' = [retirementMode EXCEPT ![active] = "expired_bound"]
    /\ active' = NoGrant
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<issued, bound, served, inheritedBy, retiredFloor,
                   verifierFloor, inflight>>

InheritRetirement ==
    /\ active # NoGrant
    /\ \E p \in PendingRetired:
        /\ bound[active] >= retirementBound[p]
        /\ inheritedBy' = [inheritedBy EXCEPT ![p] = active]
        /\ retiredFloor' = Max(retiredFloor, p)
    /\ accepted' = NoAccepted
    /\ UNCHANGED <<issued, active, bound, served, retired, retirementBound,
                   retirementMode, verifierFloor, inflight>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ IssueGrant
    \/ ServeReply
    \/ DeliverAcceptedReply
    \/ DropReply
    \/ ClearAccepted
    \/ SealExact
    \/ RetireExpired
    \/ InheritRetirement
    \/ Stutter

TypeOK ==
    /\ issued \subseteq Eras
    /\ active \in Eras \cup {NoGrant}
    /\ bound \in [Eras -> Frontiers]
    /\ served \in [Eras -> Frontiers]
    /\ retired \subseteq issued
    /\ retirementBound \in [Eras -> Frontiers]
    /\ retirementMode \in [Eras -> RetireModes]
    /\ inheritedBy \in [Eras -> (Eras \cup {NoGrant})]
    /\ retiredFloor \in 0..MaxEra
    /\ verifierFloor \in 0..MaxEra
    /\ inflight \subseteq ReplySet
    /\ accepted \in [valid : BOOLEAN,
                     era : 0..MaxEra,
                     usage : Frontiers,
                     evidenceUsage : Frontiers,
                     floorAtAccept : 0..MaxEra,
                     observedFloor : 0..MaxEra]

\* Primacy: at most one root-active grant can serve.
Primacy ==
    active = NoGrant \/ (active \in issued /\ active \notin retired)

\* Bounded inheritance: every pending predecessor retirement is covered by the
\* current successor grant before that successor can serve as the active grant.
BoundedInheritance ==
    active = NoGrant \/
        \A p \in PendingRetired:
            bound[active] >= retirementBound[p]

\* Root-issued bounded evidence: every accepted reply is covered by its
\* evidence usage, and that evidence usage is inside the signed grant bound.
EvidenceBounded ==
    accepted.valid =>
        /\ accepted.usage <= accepted.evidenceUsage
        /\ accepted.evidenceUsage <= bound[accepted.era]

\* Silence is client-local and monotone: after a verifier has observed a
\* retired floor, it never accepts a reply at or below that floor. A delayed old
\* reply can only be accepted by a verifier that has not yet seen the successor
\* floor, and it must still be inside the predecessor's signed grant bound.
VerifierSilence ==
    accepted.valid =>
        /\ accepted.era > accepted.floorAtAccept
        /\ accepted.era > accepted.observedFloor

\* Finality: inherited grants are real retired predecessors, and the successor
\* grant covers the predecessor's retirement bound. Retired-but-not-inherited is
\* allowed as explicit pending audit state, never as silent completion.
Finality ==
    \A e \in retired:
        inheritedBy[e] = NoGrant \/
            /\ inheritedBy[e] \in issued
            /\ inheritedBy[e] > e
            /\ bound[inheritedBy[e]] >= retirementBound[e]

RetiredFloorCoversInherited ==
    \A e \in retired:
        inheritedBy[e] # NoGrant => e <= retiredFloor

ServedWithinGrant ==
    \A e \in issued:
        served[e] <= bound[e]

RetirementWithinGrant ==
    \A e \in retired:
        /\ retirementMode[e] # "none"
        /\ retirementBound[e] <= bound[e]

EunomiaGuarantees ==
    /\ Primacy
    /\ BoundedInheritance
    /\ EvidenceBounded
    /\ VerifierSilence
    /\ Finality
    /\ RetiredFloorCoversInherited
    /\ ServedWithinGrant
    /\ RetirementWithinGrant

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
