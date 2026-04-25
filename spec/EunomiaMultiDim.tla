------------------------- MODULE EunomiaMultiDim -------------------------
EXTENDS Naturals, FiniteSets

\* Minimal positive model for the CRDB #66562 analog.
\* The predecessor exposes a served-read summary keyed by object + timestamp.
\* The successor may only campaign if its lease_start strictly covers every
\* served read timestamp in that summary.

CONSTANTS
    \* @type: Int;
    MaxKey,
    \* @type: Int;
    MaxLeaseStart,
    \* @type: Int;
    MaxTimestamp

Keys       == 0..MaxKey
LeaseStart == 0..MaxLeaseStart
Timestamps == 0..MaxTimestamp
ReadSet    == { [key |-> k, ts |-> t] : k \in Keys, t \in Timestamps }
NoLease    == MaxLeaseStart + 1
NoWrite    == [valid |-> FALSE, key |-> 0, ts |-> 0]

VARIABLES
    \* @type: Bool;
    predecessorActive,
    \* @type: Int;
    predecessorLeaseStart,
    \* @type: Set([key: Int, ts: Int]);
    servedReads,
    \* @type: Bool;
    sealed,
    \* @type: Bool;
    successorIssued,
    \* @type: Int;
    successorLeaseStart,
    \* @type: [valid: Bool, key: Int, ts: Int];
    deliveredWrite

Vars ==
    << predecessorActive, predecessorLeaseStart, servedReads, sealed,
       successorIssued, successorLeaseStart, deliveredWrite >>

Init ==
    /\ predecessorActive = TRUE
    /\ predecessorLeaseStart = 0
    /\ servedReads = {}
    /\ sealed = FALSE
    /\ successorIssued = FALSE
    /\ successorLeaseStart = NoLease
    /\ deliveredWrite = NoWrite

ServeRead ==
    /\ predecessorActive
    /\ \E r \in ReadSet:
        /\ r.ts > predecessorLeaseStart
        /\ servedReads' = servedReads \cup {r}
    /\ UNCHANGED <<predecessorActive, predecessorLeaseStart, sealed,
                   successorIssued, successorLeaseStart, deliveredWrite>>

Seal ==
    /\ predecessorActive
    /\ predecessorActive' = FALSE
    /\ sealed' = TRUE
    /\ UNCHANGED <<predecessorLeaseStart, servedReads, successorIssued,
                   successorLeaseStart, deliveredWrite>>

IssueSuccessor ==
    /\ sealed
    /\ ~successorIssued
    /\ \E ls \in LeaseStart:
        /\ \A r \in servedReads: ls > r.ts
        /\ successorIssued' = TRUE
        /\ successorLeaseStart' = ls
    /\ UNCHANGED <<predecessorActive, predecessorLeaseStart, servedReads,
                   sealed, deliveredWrite>>

AcceptWrite ==
    /\ successorIssued
    /\ ~deliveredWrite.valid
    /\ \E k \in Keys:
        /\ \E ts \in Timestamps:
            /\ deliveredWrite' = [valid |-> TRUE, key |-> k, ts |-> ts]
    /\ UNCHANGED <<predecessorActive, predecessorLeaseStart, servedReads,
                   sealed, successorIssued, successorLeaseStart>>

ClearWrite ==
    /\ deliveredWrite.valid
    /\ deliveredWrite' = NoWrite
    /\ UNCHANGED <<predecessorActive, predecessorLeaseStart, servedReads,
                   sealed, successorIssued, successorLeaseStart>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ ServeRead
    \/ Seal
    \/ IssueSuccessor
    \/ AcceptWrite
    \/ ClearWrite
    \/ Stutter

TypeOK ==
    /\ predecessorActive \in BOOLEAN
    /\ predecessorLeaseStart \in LeaseStart
    /\ servedReads \subseteq ReadSet
    /\ sealed \in BOOLEAN
    /\ successorIssued \in BOOLEAN
    /\ successorLeaseStart \in 0..(MaxLeaseStart + 1)
    /\ deliveredWrite \in [valid : BOOLEAN, key : Keys, ts : Timestamps]

WriteBehindServedRead ==
    /\ deliveredWrite.valid
    /\ \E r \in servedReads:
        /\ r.key = deliveredWrite.key
        /\ successorLeaseStart <= r.ts
        /\ deliveredWrite.ts <= r.ts

NoWriteBehindServedRead ==
    ~WriteBehindServedRead

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
