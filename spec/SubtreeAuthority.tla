-------------------------- MODULE SubtreeAuthority --------------------------
EXTENDS Naturals, FiniteSets

\* Namespace-authority instance of Eunomia.
\* One subtree has at most one active authority. Handoff seals the predecessor,
\* installs one successor with a covering frontier, and closes the predecessor.
\* The model intentionally omits the KV dentry mutation; this is the authority
\* protocol that RenameSubtree v1 will consume.

CONSTANTS
    \* @type: Int;
    MaxSubtree,
    \* @type: Int;
    MaxAuthority,
    \* @type: Int;
    MaxEra,
    \* @type: Int;
    MaxFrontier

Subtrees    == 0..MaxSubtree
Authorities == 0..MaxAuthority
Eras        == 0..MaxEra
Frontiers   == 0..MaxFrontier

AuthorityRecords ==
    { [subtree |-> s, authority |-> a, era |-> e] :
        s \in Subtrees, a \in Authorities, e \in Eras }

HandoffRecords ==
    { [subtree |-> s,
       pred |-> p,
       predEra |-> pe,
       succ |-> q,
       succEra |-> se,
       predFrontier |-> f] :
        s \in Subtrees,
        p \in Authorities,
        pe \in Eras,
        q \in Authorities,
        se \in Eras,
        f \in Frontiers }

CoverageRecords ==
    { [pred |-> p, succ |-> q] : p \in AuthorityRecords, q \in AuthorityRecords }

ReplySet ==
    { [subtree |-> s, authority |-> a, era |-> e, frontier |-> f] :
        s \in Subtrees, a \in Authorities, e \in Eras, f \in Frontiers }

NoDelivered == [valid |-> FALSE, subtree |-> 0, authority |-> 0, era |-> 0, frontier |-> 0]

\* @type: (Int, Int, Int) => [subtree: Int, authority: Int, era: Int];
RecordOf(subtree, authority, era) ==
    [subtree |-> subtree, authority |-> authority, era |-> era]

\* @type: ([subtree: Int, pred: Int, predEra: Int, succ: Int, succEra: Int, predFrontier: Int]) => [subtree: Int, authority: Int, era: Int];
PredOf(h) ==
    RecordOf(h.subtree, h.pred, h.predEra)

\* @type: ([subtree: Int, pred: Int, predEra: Int, succ: Int, succEra: Int, predFrontier: Int]) => [subtree: Int, authority: Int, era: Int];
SuccOf(h) ==
    RecordOf(h.subtree, h.succ, h.succEra)

VARIABLES
    \* @type: Set(Int);
    declared,
    \* @type: Set([subtree: Int, authority: Int, era: Int]);
    active,
    \* @type: Set([subtree: Int, authority: Int, era: Int]);
    sealed,
    \* @type: Set([subtree: Int, authority: Int, era: Int]);
    closed,
    \* @type: Set([subtree: Int, pred: Int, predEra: Int, succ: Int, succEra: Int, predFrontier: Int]);
    pending,
    \* @type: Set([pred: [subtree: Int, authority: Int, era: Int], succ: [subtree: Int, authority: Int, era: Int]]);
    coveredBy,
    \* @type: Set([subtree: Int, authority: Int, era: Int]) -> Int;
    frontier,
    \* @type: Set([subtree: Int, authority: Int, era: Int, frontier: Int]);
    inflight,
    \* @type: [valid: Bool, subtree: Int, authority: Int, era: Int, frontier: Int];
    delivered

Vars == <<declared, active, sealed, closed, pending, coveredBy, frontier, inflight, delivered>>

Init ==
    /\ declared = {}
    /\ active = {}
    /\ sealed = {}
    /\ closed = {}
    /\ pending = {}
    /\ coveredBy = {}
    /\ frontier = [r \in AuthorityRecords |-> 0]
    /\ inflight = {}
    /\ delivered = NoDelivered

NoActiveFor(subtree) ==
    \A r \in active: r.subtree # subtree

NoPendingFor(subtree) ==
    \A h \in pending: h.subtree # subtree

DeclareAuthority ==
    \E s \in Subtrees:
        /\ s \notin declared
        /\ NoActiveFor(s)
        /\ NoPendingFor(s)
        /\ \E a \in Authorities:
            /\ LET r == RecordOf(s, a, 0) IN
               /\ r \notin sealed
               /\ declared' = declared \cup {s}
               /\ active' = active \cup {r}
               /\ frontier' = [frontier EXCEPT ![r] = 0]
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<sealed, closed, pending, coveredBy, inflight>>

Serve ==
    \E r \in active:
        /\ \E f \in Frontiers:
            /\ f >= frontier[r]
            /\ frontier' = [frontier EXCEPT ![r] = f]
            /\ inflight' = inflight \cup {[subtree |-> r.subtree, authority |-> r.authority, era |-> r.era, frontier |-> f]}
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<declared, active, sealed, closed, pending, coveredBy>>

StartHandoff ==
    \E r \in active:
        /\ r.era < MaxEra
        /\ NoPendingFor(r.subtree)
        /\ \E succ \in Authorities:
            /\ succ # r.authority
            /\ LET h == [subtree |-> r.subtree,
                         pred |-> r.authority,
                         predEra |-> r.era,
                         succ |-> succ,
                         succEra |-> r.era + 1,
                         predFrontier |-> frontier[r]] IN
               /\ pending' = pending \cup {h}
        /\ active' = active \ {r}
        /\ sealed' = sealed \cup {r}
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<declared, closed, coveredBy, frontier, inflight>>

CompleteHandoff ==
    \E h \in pending:
        /\ \E f \in Frontiers:
            /\ f >= h.predFrontier
            /\ LET pred == PredOf(h) IN
               LET succ == SuccOf(h) IN
               LET cov == [pred |-> pred, succ |-> succ] IN
               /\ active' = active \cup {succ}
               /\ pending' = pending \ {h}
               /\ closed' = closed \cup {pred}
               /\ coveredBy' = coveredBy \cup {cov}
               /\ frontier' = [frontier EXCEPT ![succ] = f]
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<declared, sealed, inflight>>

DeliverReply ==
    \E reply \in inflight:
        /\ RecordOf(reply.subtree, reply.authority, reply.era) \notin sealed
        /\ inflight' = inflight \ {reply}
        /\ delivered' = [valid |-> TRUE,
                         subtree |-> reply.subtree,
                         authority |-> reply.authority,
                         era |-> reply.era,
                         frontier |-> reply.frontier]
        /\ UNCHANGED <<declared, active, sealed, closed, pending, coveredBy, frontier>>

DropReply ==
    \E reply \in inflight:
        /\ inflight' = inflight \ {reply}
        /\ delivered' = NoDelivered
        /\ UNCHANGED <<declared, active, sealed, closed, pending, coveredBy, frontier>>

ClearDelivered ==
    /\ delivered.valid
    /\ delivered' = NoDelivered
    /\ UNCHANGED <<declared, active, sealed, closed, pending, coveredBy, frontier, inflight>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ DeclareAuthority
    \/ Serve
    \/ StartHandoff
    \/ CompleteHandoff
    \/ DeliverReply
    \/ DropReply
    \/ ClearDelivered
    \/ Stutter

TypeOK ==
    /\ declared \subseteq Subtrees
    /\ active \subseteq AuthorityRecords
    /\ sealed \subseteq AuthorityRecords
    /\ closed \subseteq sealed
    /\ pending \subseteq HandoffRecords
    /\ coveredBy \subseteq CoverageRecords
    /\ frontier \in [AuthorityRecords -> Frontiers]
    /\ inflight \subseteq ReplySet
    /\ delivered \in [valid : BOOLEAN, subtree : Subtrees, authority : Authorities, era : Eras, frontier : Frontiers]

ActiveFor(subtree) ==
    { r \in active : r.subtree = subtree }

ActiveRecordsDeclared ==
    \A r \in active: r.subtree \in declared

OnlyOneActiveRecordPerSubtree ==
    \A r \in active:
        \A q \in active:
            r.subtree = q.subtree => r = q

PrimacyInductive ==
    /\ ActiveRecordsDeclared
    /\ OnlyOneActiveRecordPerSubtree

PendingPreds ==
    { PredOf(h) : h \in pending }

Primacy ==
    \A s \in Subtrees:
        Cardinality(ActiveFor(s)) <= 1

Inheritance ==
    \A pred \in closed:
        \E cov \in coveredBy:
            /\ cov.pred = pred
            /\ cov.succ.subtree = pred.subtree
            /\ cov.succ.era > pred.era
            /\ frontier[cov.succ] >= frontier[pred]

Silence ==
    delivered.valid =>
        RecordOf(delivered.subtree, delivered.authority, delivered.era) \notin sealed

Finality ==
    \A pred \in sealed:
        pred \in PendingPreds \/ pred \in closed

SubtreeAuthorityGuarantees ==
    /\ Primacy
    /\ PrimacyInductive
    /\ Inheritance
    /\ Silence
    /\ Finality

THEOREM PrimacyInductiveImpliesPrimacy ==
    PrimacyInductive => Primacy

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
