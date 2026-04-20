---------------------- MODULE ChubbyFencedLease ----------------------
EXTENDS Naturals, FiniteSets

\* Contrast model: lease-based serving with per-reply sequencer-style fencing.
\* The client remembers the highest generation it has admitted and rejects any
\* older reply after that point. This blocks stale delivery, but there is still
\* no rooted seal / cover / close object that forces successor coverage.

CONSTANTS
    \* @type: Int;
    MaxGeneration,
    \* @type: Int;
    MaxFrontier

Generations == 0..MaxGeneration
Frontiers   == 0..MaxFrontier
ReplySet    == { [gen |-> g, frontier |-> f] : g \in Generations, f \in Frontiers }
NoDelivered == [valid |-> FALSE, gen |-> 0, frontier |-> 0]

VARIABLES
    \* @type: Set(Int);
    issued,
    \* @type: Int;
    activeGen,
    \* @type: Int -> Int;
    frontier,
    \* @type: Set([gen: Int, frontier: Int]);
    inflight,
    \* @type: [valid: Bool, gen: Int, frontier: Int];
    delivered,
    \* @type: Int;
    observedMax

Vars == <<issued, activeGen, frontier, inflight, delivered, observedMax>>

Init ==
    /\ issued = {0}
    /\ activeGen = 0
    /\ frontier = [g \in Generations |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered
    /\ observedMax = 0

Issue ==
    \E g \in Generations:
        /\ g \notin issued
        /\ g > activeGen
        /\ issued' = issued \cup {g}
        /\ activeGen' = g
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<frontier, inflight, observedMax>>

CurrentReply ==
    /\ \E f \in Frontiers:
        /\ f >= frontier[activeGen]
        /\ frontier' = [frontier EXCEPT ![activeGen] = f]
        /\ inflight' = inflight \cup {[gen |-> activeGen, frontier |-> f]}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, observedMax>>

DeliverReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
        /\ \/ /\ r.gen >= observedMax
              /\ delivered' = [valid |-> TRUE, gen |-> r.gen, frontier |-> r.frontier]
              /\ observedMax' = r.gen
           \/ /\ r.gen < observedMax
              /\ delivered' = NoDelivered
              /\ observedMax' = observedMax
    /\ UNCHANGED <<issued, activeGen, frontier>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, frontier, observedMax>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, frontier, inflight, observedMax>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ Issue
    \/ CurrentReply
    \/ DeliverReply
    \/ DropReply
    \/ ClearDelivered
    \/ Stutter

TypeOK ==
    /\ issued \subseteq Generations
    /\ activeGen \in Generations
    /\ frontier \in [Generations -> Frontiers]
    /\ inflight \subseteq ReplySet
    /\ delivered \in [valid : BOOLEAN, gen : Generations, frontier : Frontiers]
    /\ observedMax \in Generations

OldReplyAfterSuccessor ==
    /\ delivered.valid
    /\ delivered.gen < activeGen

NoOldReplyAfterSuccessor ==
    ~OldReplyAfterSuccessor

CoverageGapAfterSuccessor ==
    \E g \in issued:
        /\ g < activeGen
        /\ frontier[activeGen] < frontier[g]

SuccessorCoversHistoricalFrontiers ==
    ~CoverageGapAfterSuccessor

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
