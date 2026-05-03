--------------------------- MODULE Percolator2PC ---------------------------
EXTENDS Naturals

\* Bounded positive model for NoKV's Percolator-style 2PC protocol.
\* It focuses on primary authority, min-commit-ts safety, rollback markers,
\* lock resolution, heartbeat/TTL extension, and GC safepoint admission.

CONSTANTS
    \* @type: Int;
    MaxTs,
    \* @type: Int;
    MinCommitTs,
    \* @type: Int;
    InitialTTL,
    \* @type: Int;
    HeartbeatExtend

Times == 0..MaxTs
StartTs == 1

VARIABLES
    \* @type: Str;
    primary,
    \* @type: Str;
    secondary,
    \* @type: Int;
    primaryCommitTs,
    \* @type: Int;
    secondaryCommitTs,
    \* @type: Bool;
    primaryRollbackMarker,
    \* @type: Bool;
    secondaryRollbackMarker,
    \* @type: Int;
    now,
    \* @type: Int;
    expireAt,
    \* @type: Int;
    primaryCommitClock,
    \* @type: Int;
    primaryCommitExpireAt,
    \* @type: Int;
    gcSafepoint

Vars ==
    << primary, secondary, primaryCommitTs, secondaryCommitTs,
       primaryRollbackMarker, secondaryRollbackMarker, now, expireAt,
       primaryCommitClock, primaryCommitExpireAt, gcSafepoint >>

States == {"None", "Prewritten", "Committed", "RolledBack"}

Init ==
    /\ primary = "None"
    /\ secondary = "None"
    /\ primaryCommitTs = 0
    /\ secondaryCommitTs = 0
    /\ primaryRollbackMarker = FALSE
    /\ secondaryRollbackMarker = FALSE
    /\ now = 0
    /\ expireAt = InitialTTL
    /\ primaryCommitClock = 0
    /\ primaryCommitExpireAt = InitialTTL
    /\ gcSafepoint = 0

LiveLock ==
    primary = "Prewritten" \/ secondary = "Prewritten"

Tick ==
    /\ now < MaxTs
    /\ now' = now + 1
    /\ UNCHANGED <<primary, secondary, primaryCommitTs, secondaryCommitTs,
                   primaryRollbackMarker, secondaryRollbackMarker, expireAt,
                   primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

HeartbeatPrimary ==
    /\ primary = "Prewritten"
    /\ now <= expireAt
    /\ expireAt + HeartbeatExtend <= MaxTs
    /\ expireAt' = expireAt + HeartbeatExtend
    /\ UNCHANGED <<primary, secondary, primaryCommitTs, secondaryCommitTs,
                   primaryRollbackMarker, secondaryRollbackMarker, now,
                   primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

PrewritePrimary ==
    /\ primary = "None"
    /\ ~primaryRollbackMarker
    /\ gcSafepoint < StartTs
    /\ primary' = "Prewritten"
    /\ expireAt' = now + InitialTTL
    /\ expireAt' <= MaxTs
    /\ UNCHANGED <<secondary, primaryCommitTs, secondaryCommitTs,
                   primaryRollbackMarker, secondaryRollbackMarker, now,
                   primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

PrewriteSecondary ==
    /\ primary = "Prewritten"
    /\ secondary = "None"
    /\ ~secondaryRollbackMarker
    /\ secondary' = "Prewritten"
    /\ UNCHANGED <<primary, primaryCommitTs, secondaryCommitTs,
                   primaryRollbackMarker, secondaryRollbackMarker, now,
                   expireAt, primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

CommitPrimary ==
    /\ primary = "Prewritten"
    /\ now <= expireAt
    /\ \E c \in Times:
        /\ c >= MinCommitTs
        /\ c > StartTs
        /\ primary' = "Committed"
        /\ primaryCommitTs' = c
    /\ primaryCommitClock' = now
    /\ primaryCommitExpireAt' = expireAt
    /\ UNCHANGED <<secondary, secondaryCommitTs, primaryRollbackMarker,
                   secondaryRollbackMarker, expireAt, now, gcSafepoint>>

CommitSecondary ==
    /\ secondary = "Prewritten"
    /\ primary = "Committed"
    /\ secondary' = "Committed"
    /\ secondaryCommitTs' = primaryCommitTs
    /\ UNCHANGED <<primary, primaryCommitTs, primaryRollbackMarker,
                   secondaryRollbackMarker, now, expireAt,
                   primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

RollbackPrimaryExpired ==
    /\ primary = "Prewritten"
    /\ now > expireAt
    /\ primary' = "RolledBack"
    /\ primaryRollbackMarker' = TRUE
    /\ UNCHANGED <<secondary, primaryCommitTs, secondaryCommitTs,
                   secondaryRollbackMarker, now, expireAt,
                   primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

ResolveSecondaryRollback ==
    /\ secondary = "Prewritten"
    /\ primary = "RolledBack"
    /\ secondary' = "RolledBack"
    /\ secondaryRollbackMarker' = TRUE
    /\ UNCHANGED <<primary, primaryCommitTs, secondaryCommitTs,
                   primaryRollbackMarker, now, expireAt,
                   primaryCommitClock, primaryCommitExpireAt, gcSafepoint>>

AdvanceSafepoint ==
    /\ ~LiveLock
    /\ \E s \in Times:
        /\ s >= gcSafepoint
        /\ gcSafepoint' = s
    /\ UNCHANGED <<primary, secondary, primaryCommitTs, secondaryCommitTs,
                   primaryRollbackMarker, secondaryRollbackMarker, now,
                   expireAt, primaryCommitClock, primaryCommitExpireAt>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ Tick
    \/ HeartbeatPrimary
    \/ PrewritePrimary
    \/ PrewriteSecondary
    \/ CommitPrimary
    \/ CommitSecondary
    \/ RollbackPrimaryExpired
    \/ ResolveSecondaryRollback
    \/ AdvanceSafepoint
    \/ Stutter

TypeOK ==
    /\ primary \in States
    /\ secondary \in States
    /\ primaryCommitTs \in Times
    /\ secondaryCommitTs \in Times
    /\ primaryRollbackMarker \in BOOLEAN
    /\ secondaryRollbackMarker \in BOOLEAN
    /\ now \in Times
    /\ expireAt \in Times
    /\ primaryCommitClock \in Times
    /\ primaryCommitExpireAt \in Times
    /\ gcSafepoint \in Times

SecondaryCommitFollowsPrimary ==
    secondary = "Committed" =>
        /\ primary = "Committed"
        /\ secondaryCommitTs = primaryCommitTs

CommitTsRespectsMinCommit ==
    primary = "Committed" => primaryCommitTs >= MinCommitTs

CommittedPrimaryWasLive ==
    primary = "Committed" => primaryCommitClock <= primaryCommitExpireAt

RollbackMarkerExcludesCommit ==
    /\ primaryRollbackMarker => primary # "Committed"
    /\ secondaryRollbackMarker => secondary # "Committed"

GCSafepointDoesNotCrossLiveLock ==
    LiveLock => gcSafepoint < StartTs

Percolator2PCGuarantees ==
    /\ SecondaryCommitFollowsPrimary
    /\ CommitTsRespectsMinCommit
    /\ CommittedPrimaryWasLive
    /\ RollbackMarkerExcludesCommit
    /\ GCSafepointDoesNotCrossLiveLock

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
