------------------------------ MODULE Eunomia ------------------------------
EXTENDS Naturals, FiniteSets

\* Repeated-handoff positive model for the control-plane paper.
\* This module intentionally models only service-level authority lineage,
\* not the underlying consensus protocol.
\*
\* The working fault vocabulary is kept inline for now rather than split
\* into a separate Fault.tla:
\*   delayed_reply
\*   revived_holder
\*   root_unreach
\*   lease_expiry
\*   successor_campaign
\*   budget_exhaustion
\*   descriptor_publish_race

CONSTANTS
    \* @type: Int;
    MaxEra,
    \* @type: Int;
    MaxFrontier

Eras        == 0..MaxEra
Frontiers   == 0..MaxFrontier
Phases      == {"Attached", "Active", "Sealed", "Covered", "Closed"}
NoPending   == MaxEra + 1
ReplySet    == { [era |-> e, frontier |-> f] : e \in Eras, f \in Frontiers }
NoDelivered == [valid |-> FALSE, era |-> 0, frontier |-> 0]

VARIABLES
    \* @type: Str;
    phase,
    \* @type: Set(Int);
    issued,
    \* @type: Int;
    activeEra,
    \* @type: Int;
    pendingSeal,
    \* @type: Set(Int);
    sealed,
    \* @type: Set(Int);
    covered,
    \* @type: Set(Int);
    closed,
    \* @type: Int -> Int;
    frontier,
    \* @type: Set([era: Int, frontier: Int]);
    inflight,
    \* @type: [valid: Bool, era: Int, frontier: Int];
    delivered

Vars == <<phase, issued, activeEra, pendingSeal, sealed, covered, closed, frontier, inflight, delivered>>

Init ==
    /\ phase = "Attached"
    /\ issued = {}
    /\ activeEra = 0
    /\ pendingSeal = NoPending
    /\ sealed = {}
    /\ covered = {}
    /\ closed = {}
    /\ frontier = [e \in Eras |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered

Issue ==
    \E e \in Eras:
        /\ phase \in {"Attached", "Sealed"}
        /\ e \notin issued
        /\ e > activeEra
        /\ issued' = issued \cup {e}
        /\ frontier' = [frontier EXCEPT ![e] = frontier[activeEra]]
        /\ activeEra' = e
        /\ phase' = "Active"
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<pendingSeal, sealed, covered, closed, inflight>>

ActiveReply ==
    /\ phase = "Active"
    /\ activeEra \in issued
    /\ activeEra \notin sealed
    /\ \E f \in Frontiers:
        /\ f >= frontier[activeEra]
        /\ frontier' = [frontier EXCEPT ![activeEra] = f]
        /\ inflight' = inflight \cup {[era |-> activeEra, frontier |-> f]}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, pendingSeal, sealed, covered, closed>>
    /\ phase' = phase

DeliverReply ==
    /\ \E r \in inflight:
        /\ r.era \notin sealed
        /\ inflight' = inflight \ {r}
        /\ delivered' = [valid |-> TRUE, era |-> r.era, frontier |-> r.frontier]
    /\ UNCHANGED <<phase, issued, activeEra, pendingSeal, sealed, covered, closed, frontier>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<phase, issued, activeEra, pendingSeal, sealed, covered, closed, frontier>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<phase, issued, activeEra, pendingSeal, sealed, covered, closed, frontier, inflight>>

Seal ==
    /\ phase = "Active"
    /\ pendingSeal = NoPending
    /\ activeEra \notin sealed
    /\ sealed' = sealed \cup {activeEra}
    /\ pendingSeal' = activeEra
    /\ phase' = "Sealed"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, covered, closed, frontier, inflight>>

Cover ==
    /\ phase \in {"Sealed", "Active"}
    /\ pendingSeal # NoPending
    /\ activeEra \in issued
    /\ activeEra > pendingSeal
    /\ frontier[activeEra] >= frontier[pendingSeal]
    /\ covered' = covered \cup {pendingSeal}
    /\ pendingSeal' = NoPending
    /\ phase' = "Covered"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, sealed, closed, frontier, inflight>>

Close ==
    /\ phase = "Covered"
    /\ \E e \in covered \ closed:
        /\ closed' = closed \cup {e}
        /\ phase' = "Closed"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, pendingSeal, sealed, covered, frontier, inflight>>

Reattach ==
    /\ phase = "Closed"
    /\ \A e \in sealed: e \in covered \/ e \in closed
    \* Reattach completes the predecessor handover and returns the successor to
    \* steady-state serving, so the next seal/issue cycle can proceed.
    /\ phase' = "Active"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, pendingSeal, sealed, covered, closed, frontier, inflight>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ Issue
    \/ ActiveReply
    \/ DeliverReply
    \/ DropReply
    \/ ClearDelivered
    \/ Seal
    \/ Cover
    \/ Close
    \/ Reattach
    \/ Stutter

TypeOK ==
    /\ phase \in Phases
    /\ issued \subseteq Eras
    /\ activeEra \in Eras
    /\ pendingSeal \in 0..(MaxEra + 1)
    /\ sealed \subseteq issued
    /\ covered \subseteq sealed
    /\ closed \subseteq covered
    /\ frontier \in [Eras -> Frontiers]
    /\ inflight \subseteq ReplySet
    /\ delivered \in [valid : BOOLEAN, era : Eras, frontier : Frontiers]

LiveEras == issued \ sealed

\* Stronger Primacy shape invariant: every issued era that is not the
\* current active era has already been sealed. This is induction-friendly
\* and does not depend on the concrete era bound; the bound only limits
\* how many times TLC can exercise the repeated cycle in one run.
OnlyCurrentMayRemainUnsealed ==
    \A e \in issued:
        e # activeEra => e \in sealed

ActiveEraIssued ==
    issued = {} \/ activeEra \in issued

PrimacyInductive ==
    /\ ActiveEraIssued
    /\ OnlyCurrentMayRemainUnsealed

\* Primacy: at most one era is live for serving.
Primacy ==
    Cardinality(LiveEras) <= 1

\* Inheritance: any sealed predecessor that is marked covered must be covered by a
\* strictly newer era whose frontier is no smaller.
Inheritance ==
    \A e \in covered:
        \E h \in issued:
            /\ h > e
            /\ h = activeEra \/ h \in LiveEras
            /\ frontier[h] >= frontier[e]

\* Silence: once an era is sealed, a valid reply may not still cite it.
Silence ==
    delivered.valid => delivered.era \notin sealed

\* Finality: every sealed predecessor must be pending cover,
\* already covered, or already closed before reattach is legal.
Finality ==
    \A e \in sealed:
        /\ e = pendingSeal \/ e \in covered \/ e \in closed

G1_Eunomia ==
    Inheritance

G2_Primacy ==
    Primacy

G2_PrimacyInductive ==
    PrimacyInductive

G3_Silence ==
    Silence

G4_Finality ==
    Finality

EunomiaGuarantees ==
    /\ G1_Eunomia
    /\ G2_Primacy
    /\ G3_Silence
    /\ G4_Finality

\* Stronger lemma used to support an induction-style argument for Primacy:
\* if only the current active era may remain unsealed, then there can be
\* at most one live era.
THEOREM PrimacyInductiveImpliesPrimacy ==
    PrimacyInductive => Primacy

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
