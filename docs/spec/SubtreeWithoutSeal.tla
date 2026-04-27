--------------------------- MODULE SubtreeWithoutSeal ---------------------------
EXTENDS Naturals, FiniteSets

\* Negative namespace-authority model.
\* StartHandoff records a successor but leaves the predecessor active. Once the
\* successor is completed, two authorities are active for the same subtree.

CONSTANTS
    \* @type: Int;
    MaxAuthority,
    \* @type: Int;
    MaxEra

Authorities == 0..MaxAuthority
Eras        == 0..MaxEra

AuthorityRecords ==
    { [authority |-> a, era |-> e] : a \in Authorities, e \in Eras }

HandoffRecords ==
    { [pred |-> p, predEra |-> pe, succ |-> q, succEra |-> se] :
        p \in Authorities,
        pe \in Eras,
        q \in Authorities,
        se \in Eras }

\* @type: (Int, Int) => [authority: Int, era: Int];
RecordOf(authority, era) ==
    [authority |-> authority, era |-> era]

\* @type: ([pred: Int, predEra: Int, succ: Int, succEra: Int]) => [authority: Int, era: Int];
SuccOf(h) ==
    RecordOf(h.succ, h.succEra)

VARIABLES
    \* @type: Set([authority: Int, era: Int]);
    active,
    \* @type: Set([pred: Int, predEra: Int, succ: Int, succEra: Int]);
    pending

Vars == <<active, pending>>

Init ==
    /\ active = {RecordOf(0, 0)}
    /\ pending = {}

StartHandoff ==
    \E r \in active:
        /\ r.era < MaxEra
        /\ pending = {}
        /\ \E succ \in Authorities:
            /\ succ # r.authority
            /\ pending' = {[pred |-> r.authority,
                            predEra |-> r.era,
                            succ |-> succ,
                            succEra |-> r.era + 1]}
        \* Bug: predecessor remains active and is never sealed.
        /\ UNCHANGED active

CompleteHandoff ==
    \E h \in pending:
        /\ active' = active \cup {SuccOf(h)}
        /\ pending' = pending \ {h}

Stutter ==
    UNCHANGED Vars

Next ==
    \/ StartHandoff
    \/ CompleteHandoff
    \/ Stutter

TypeOK ==
    /\ active \subseteq AuthorityRecords
    /\ pending \subseteq HandoffRecords

Primacy ==
    Cardinality(active) <= 1

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
