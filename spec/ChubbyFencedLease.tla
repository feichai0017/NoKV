---------------------- MODULE ChubbyFencedLease ----------------------
EXTENDS Naturals, FiniteSets

\* Contrast model: lease-based serving with per-reply sequencer-style fencing.
\* The client remembers the highest era it has admitted and rejects any
\* older reply after that point. This blocks stale delivery, but there is still
\* no rooted seal / cover / close object that forces successor coverage.

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
    delivered,
    \* @type: Int;
    observedMaxEra

Vars == <<issued, activeEra, frontier, inflight, delivered, observedMaxEra>>

Init ==
    /\ issued = {0}
    /\ activeEra = 0
    /\ frontier = [e \in Eras |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered
    /\ observedMaxEra = 0

Issue ==
    \E e \in Eras:
        /\ e \notin issued
        /\ e > activeEra
        /\ issued' = issued \cup {e}
        /\ activeEra' = e
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<frontier, inflight, observedMaxEra>>

CurrentReply ==
    /\ \E f \in Frontiers:
        /\ f >= frontier[activeEra]
        /\ frontier' = [frontier EXCEPT ![activeEra] = f]
        /\ inflight' = inflight \cup {[era |-> activeEra, frontier |-> f]}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, observedMaxEra>>

DeliverReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
        /\ \/ /\ r.era >= observedMaxEra
              /\ delivered' = [valid |-> TRUE, era |-> r.era, frontier |-> r.frontier]
              /\ observedMaxEra' = r.era
           \/ /\ r.era < observedMaxEra
              /\ delivered' = NoDelivered
              /\ observedMaxEra' = observedMaxEra
    /\ UNCHANGED <<issued, activeEra, frontier>>

DropReply ==
    /\ \E r \in inflight:
        /\ inflight' = inflight \ {r}
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, frontier, observedMaxEra>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<issued, activeEra, frontier, inflight, observedMaxEra>>

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
    /\ observedMaxEra \in Eras

OldReplyAfterSuccessor ==
    /\ delivered.valid
    /\ delivered.era < activeEra

NoOldReplyAfterSuccessor ==
    ~OldReplyAfterSuccessor

CoverageGapAfterSuccessor ==
    \E e \in issued:
        /\ e < activeEra
        /\ frontier[activeEra] < frontier[e]

SuccessorCoversHistoricalFrontiers ==
    ~CoverageGapAfterSuccessor

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
