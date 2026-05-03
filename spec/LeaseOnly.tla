--------------------------- MODULE LeaseOnly ---------------------------
EXTENDS Naturals, FiniteSets

\* Minimal contrast model for the paper.
\* This model intentionally omits explicit seal / cover / close state, so that
\* delayed old replies can remain admissible after a successor is already live.

CONSTANTS
    \* @type: Int;
    MaxEra,
    \* @type: Int;
    MaxFrontier

Eras        == 0..MaxEra
Frontiers   == 0..MaxFrontier
ReplySet    == { [era |-> e, frontier |-> f] : e \in Eras, f \in Frontiers }
NoDelivered == [valid |-> FALSE, era |-> 0, frontier |-> 0]

VARIABLES
    \* @type: Set(Int);
    issued,
    \* @type: Int;
    activeEra,
    \* @type: Int -> Int;
    frontier,
    \* @type: Set([era: Int, frontier: Int]);
    inflight,
    \* @type: [valid: Bool, era: Int, frontier: Int];
    delivered

Vars == <<issued, activeEra, frontier, inflight, delivered>>

Init ==
    /\ issued = {0}
    /\ activeEra = 0
    /\ frontier = [e \in Eras |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered

Issue ==
    \E e \in Eras:
        /\ e \notin issued
        /\ e > activeEra
        /\ issued' = issued \cup {e}
        /\ activeEra' = e
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<frontier, inflight>>

CurrentReply ==
    /\ \E f \in Frontiers:
        /\ f >= frontier[activeEra]
        /\ frontier' = [frontier EXCEPT ![activeEra] = f]
        /\ inflight' = inflight \cup {[era |-> activeEra, frontier |-> f]}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra>>

DeliverReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
        /\ delivered' = [valid |-> TRUE, era |-> r.era, frontier |-> r.frontier]
    /\ UNCHANGED <<issued, activeEra, frontier>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, frontier>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, frontier, inflight>>

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
    /\ issued \subseteq Eras
    /\ activeEra \in Eras
    /\ frontier \in [Eras -> Frontiers]
    /\ inflight \subseteq ReplySet
    /\ delivered \in [valid : BOOLEAN, era : Eras, frontier : Frontiers]

\* This is the bad state that TLC should be able to reach.
OldReplyAfterSuccessor ==
    /\ delivered.valid
    /\ delivered.era < activeEra

NoOldReplyAfterSuccessor ==
    ~OldReplyAfterSuccessor

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
