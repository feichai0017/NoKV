------------------------------ MODULE CCC ------------------------------
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
    MaxGeneration,
    \* @type: Int;
    MaxFrontier

Generations == 0..MaxGeneration
Frontiers   == 0..MaxFrontier
Phases      == {"Attached", "Active", "Sealed", "Covered", "Closed"}
NoPending   == MaxGeneration + 1
ReplySet    == { [gen |-> g, frontier |-> f] : g \in Generations, f \in Frontiers }
NoDelivered == [valid |-> FALSE, gen |-> 0, frontier |-> 0]

VARIABLES
    \* @type: Str;
    phase,
    \* @type: Set(Int);
    issued,
    \* @type: Int;
    activeGen,
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
    \* @type: Set([gen: Int, frontier: Int]);
    inflight,
    \* @type: [valid: Bool, gen: Int, frontier: Int];
    delivered

Vars == <<phase, issued, activeGen, pendingSeal, sealed, covered, closed, frontier, inflight, delivered>>

Init ==
    /\ phase = "Attached"
    /\ issued = {}
    /\ activeGen = 0
    /\ pendingSeal = NoPending
    /\ sealed = {}
    /\ covered = {}
    /\ closed = {}
    /\ frontier = [g \in Generations |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered

Issue ==
    \E g \in Generations:
        /\ phase \in {"Attached", "Sealed"}
        /\ g \notin issued
        /\ g > activeGen
        /\ issued' = issued \cup {g}
        /\ frontier' = [frontier EXCEPT ![g] = frontier[activeGen]]
        /\ activeGen' = g
        /\ phase' = "Active"
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<pendingSeal, sealed, covered, closed, inflight>>

ActiveReply ==
    /\ phase = "Active"
    /\ activeGen \in issued
    /\ activeGen \notin sealed
    /\ \E f \in Frontiers:
        /\ f >= frontier[activeGen]
        /\ frontier' = [frontier EXCEPT ![activeGen] = f]
        /\ inflight' = inflight \cup {[gen |-> activeGen, frontier |-> f]}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, pendingSeal, sealed, covered, closed>>
    /\ phase' = phase

DeliverReply ==
    /\ \E r \in inflight:
        /\ r.gen \notin sealed
        /\ inflight' = inflight \ {r}
        /\ delivered' = [valid |-> TRUE, gen |-> r.gen, frontier |-> r.frontier]
    /\ UNCHANGED <<phase, issued, activeGen, pendingSeal, sealed, covered, closed, frontier>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<phase, issued, activeGen, pendingSeal, sealed, covered, closed, frontier>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<phase, issued, activeGen, pendingSeal, sealed, covered, closed, frontier, inflight>>

Seal ==
    /\ phase = "Active"
    /\ pendingSeal = NoPending
    /\ activeGen \notin sealed
    /\ sealed' = sealed \cup {activeGen}
    /\ pendingSeal' = activeGen
    /\ phase' = "Sealed"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, covered, closed, frontier, inflight>>

Cover ==
    /\ phase \in {"Sealed", "Active"}
    /\ pendingSeal # NoPending
    /\ activeGen \in issued
    /\ activeGen > pendingSeal
    /\ frontier[activeGen] >= frontier[pendingSeal]
    /\ covered' = covered \cup {pendingSeal}
    /\ pendingSeal' = NoPending
    /\ phase' = "Covered"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, sealed, closed, frontier, inflight>>

Close ==
    /\ phase = "Covered"
    /\ \E g \in covered \ closed:
        /\ closed' = closed \cup {g}
        /\ phase' = "Closed"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, pendingSeal, sealed, covered, frontier, inflight>>

Reattach ==
    /\ phase = "Closed"
    /\ \A g \in sealed: g \in covered \/ g \in closed
    \* Reattach completes the predecessor closure and returns the successor to
    \* steady-state serving, so the next seal/issue cycle can proceed.
    /\ phase' = "Active"
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, pendingSeal, sealed, covered, closed, frontier, inflight>>

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
    /\ issued \subseteq Generations
    /\ activeGen \in Generations
    /\ pendingSeal \in 0..(MaxGeneration + 1)
    /\ sealed \subseteq issued
    /\ covered \subseteq sealed
    /\ closed \subseteq covered
    /\ frontier \in [Generations -> Frontiers]
    /\ inflight \subseteq ReplySet
    /\ delivered \in [valid : BOOLEAN, gen : Generations, frontier : Frontiers]

LiveGenerations == issued \ sealed

\* Stronger ALI-1 shape invariant: every issued generation that is not the
\* current active generation has already been sealed. This is induction-friendly
\* and does not depend on the concrete generation bound; the bound only limits
\* how many times TLC can exercise the repeated cycle in one run.
OnlyCurrentMayRemainUnsealed ==
    \A g \in issued:
        g # activeGen => g \in sealed

ActiveGenerationIssued ==
    issued = {} \/ activeGen \in issued

AuthorityUniquenessInductive ==
    /\ ActiveGenerationIssued
    /\ OnlyCurrentMayRemainUnsealed

\* ALI-1: at most one generation is live for serving.
AuthorityUniqueness ==
    Cardinality(LiveGenerations) <= 1

\* ALI-2: any sealed predecessor that is marked covered must be covered by a
\* strictly newer generation whose frontier is no smaller.
SuccessorCoverage ==
    \A g \in covered:
        \E h \in issued:
            /\ h > g
            /\ h = activeGen \/ h \in LiveGenerations
            /\ frontier[h] >= frontier[g]

\* ALI-3: once a generation is sealed, a valid reply may not still cite it.
PostSealInadmissibility ==
    delivered.valid => delivered.gen \notin sealed

\* ALI-4 / CCC closure side: every sealed predecessor must be pending cover,
\* already covered, or already closed before reattach is legal.
ClosureCompleteness ==
    \A g \in sealed:
        /\ g = pendingSeal \/ g \in covered \/ g \in closed

G1_ClosureCompleteContinuation ==
    /\ SuccessorCoverage
    /\ ClosureCompleteness

G2_AuthorityUniqueness ==
    AuthorityUniqueness

G2_AuthorityUniquenessInductive ==
    AuthorityUniquenessInductive

G3_PostSealInadmissibility ==
    PostSealInadmissibility

ALI ==
    /\ G1_ClosureCompleteContinuation
    /\ G2_AuthorityUniqueness
    /\ G3_PostSealInadmissibility

\* Stronger lemma used to support an induction-style argument for ALI-1:
\* if only the current active generation may remain unsealed, then there can be
\* at most one live generation.
THEOREM AuthorityUniquenessInductiveImpliesAuthorityUniqueness ==
    AuthorityUniquenessInductive => AuthorityUniqueness

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
