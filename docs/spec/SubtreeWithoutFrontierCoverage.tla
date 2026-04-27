-------------------- MODULE SubtreeWithoutFrontierCoverage --------------------
EXTENDS Naturals, FiniteSets

\* Negative namespace-authority model.
\* CompleteHandoff installs a successor without checking that the successor
\* frontier covers the sealed predecessor frontier. TLC should find an
\* Inheritance violation.

CONSTANTS
    \* @type: Int;
    MaxAuthority,
    \* @type: Int;
    MaxEra,
    \* @type: Int;
    MaxFrontier

Authorities == 0..MaxAuthority
Eras        == 0..MaxEra
Frontiers   == 0..MaxFrontier

AuthorityRecords ==
    { [authority |-> a, era |-> e] : a \in Authorities, e \in Eras }

HandoffRecords ==
    { [pred |-> p, predEra |-> pe, succ |-> q, succEra |-> se, predFrontier |-> f] :
        p \in Authorities,
        pe \in Eras,
        q \in Authorities,
        se \in Eras,
        f \in Frontiers }

CoverageRecords ==
    { [pred |-> p, succ |-> q] : p \in AuthorityRecords, q \in AuthorityRecords }

\* @type: (Int, Int) => [authority: Int, era: Int];
RecordOf(authority, era) ==
    [authority |-> authority, era |-> era]

\* @type: ([pred: Int, predEra: Int, succ: Int, succEra: Int, predFrontier: Int]) => [authority: Int, era: Int];
PredOf(h) ==
    RecordOf(h.pred, h.predEra)

\* @type: ([pred: Int, predEra: Int, succ: Int, succEra: Int, predFrontier: Int]) => [authority: Int, era: Int];
SuccOf(h) ==
    RecordOf(h.succ, h.succEra)

VARIABLES
    \* @type: Set([authority: Int, era: Int]);
    active,
    \* @type: Set([authority: Int, era: Int]);
    sealed,
    \* @type: Set([authority: Int, era: Int]);
    closed,
    \* @type: Set([pred: Int, predEra: Int, succ: Int, succEra: Int, predFrontier: Int]);
    pending,
    \* @type: Set([pred: [authority: Int, era: Int], succ: [authority: Int, era: Int]]);
    coveredBy,
    \* @type: Set([authority: Int, era: Int]) -> Int;
    frontier

Vars == <<active, sealed, closed, pending, coveredBy, frontier>>

Init ==
    /\ active = {RecordOf(0, 0)}
    /\ sealed = {}
    /\ closed = {}
    /\ pending = {}
    /\ coveredBy = {}
    /\ frontier = [r \in AuthorityRecords |-> 0]

Serve ==
    \E r \in active:
        /\ \E f \in Frontiers:
            /\ f >= frontier[r]
            /\ frontier' = [frontier EXCEPT ![r] = f]
        /\ UNCHANGED <<active, sealed, closed, pending, coveredBy>>

StartHandoff ==
    \E r \in active:
        /\ r.era < MaxEra
        /\ pending = {}
        /\ \E succ \in Authorities:
            /\ succ # r.authority
            /\ LET h == [pred |-> r.authority,
                         predEra |-> r.era,
                         succ |-> succ,
                         succEra |-> r.era + 1,
                         predFrontier |-> frontier[r]] IN
               /\ pending' = {h}
        /\ active' = active \ {r}
        /\ sealed' = sealed \cup {r}
        /\ UNCHANGED <<closed, coveredBy, frontier>>

CompleteHandoff ==
    \E h \in pending:
        /\ \E f \in Frontiers:
            \* Bug: no f >= h.predFrontier guard.
            /\ LET pred == PredOf(h) IN
               LET succ == SuccOf(h) IN
               LET cov == [pred |-> pred, succ |-> succ] IN
               /\ active' = active \cup {succ}
               /\ pending' = pending \ {h}
               /\ closed' = closed \cup {pred}
               /\ coveredBy' = coveredBy \cup {cov}
               /\ frontier' = [frontier EXCEPT ![succ] = f]
        /\ UNCHANGED sealed

Stutter ==
    UNCHANGED Vars

Next ==
    \/ Serve
    \/ StartHandoff
    \/ CompleteHandoff
    \/ Stutter

TypeOK ==
    /\ active \subseteq AuthorityRecords
    /\ sealed \subseteq AuthorityRecords
    /\ closed \subseteq sealed
    /\ pending \subseteq HandoffRecords
    /\ coveredBy \subseteq CoverageRecords
    /\ frontier \in [AuthorityRecords -> Frontiers]

Inheritance ==
    \A pred \in closed:
        \E cov \in coveredBy:
            /\ cov.pred = pred
            /\ cov.succ.era > pred.era
            /\ frontier[cov.succ] >= frontier[pred]

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
