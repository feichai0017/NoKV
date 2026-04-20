--------------------------- MODULE TokenOnly ---------------------------
EXTENDS Naturals, FiniteSets

\* Contrast model: reply carries only bounded-freshness evidence. The caller
\* admits a reply if its frontier is within a lag budget of the current active
\* frontier, regardless of generation lineage. This can still admit an
\* old-generation reply after a successor is already active.

CONSTANTS
    \* @type: Int;
    MaxGeneration,
    \* @type: Int;
    MaxFrontier,
    \* @type: Int;
    LagBudget

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
    delivered

Vars == <<issued, activeGen, frontier, inflight, delivered>>

Init ==
    /\ issued = {0}
    /\ activeGen = 0
    /\ frontier = [g \in Generations |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered

Issue ==
    \E g \in Generations:
        /\ g \notin issued
        /\ g > activeGen
        /\ issued' = issued \cup {g}
        /\ activeGen' = g
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<frontier, inflight>>

CurrentReply ==
    /\ \E f \in Frontiers:
        /\ f >= frontier[activeGen]
        /\ frontier' = [frontier EXCEPT ![activeGen] = f]
        /\ inflight' = inflight \cup {[gen |-> activeGen, frontier |-> f]}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen>>

DeliverReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
        /\ LET currentF == frontier[activeGen] IN
           /\ \/ /\ currentF >= r.frontier
                 /\ currentF - r.frontier <= LagBudget
                 /\ delivered' = [valid |-> TRUE, gen |-> r.gen, frontier |-> r.frontier]
              \/ /\ ~(currentF >= r.frontier /\ currentF - r.frontier <= LagBudget)
                 /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, frontier>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, frontier>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeGen, frontier, inflight>>

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
    /\ LagBudget \in Frontiers

OldReplyAfterSuccessor ==
    /\ delivered.valid
    /\ delivered.gen < activeGen

NoOldReplyAfterSuccessor ==
    ~OldReplyAfterSuccessor

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
