------------------------------- MODULE MVCCGC -------------------------------
EXTENDS Naturals, FiniteSets

\* Bounded positive model for MVCC GC admission.
\* GC may remove obsolete records only when the safepoint is below active
\* snapshot and lock floors, and each destructive removal records the
\* safepoint that made it legal.

CONSTANTS
    \* @type: Int;
    MaxTs

Times == 0..MaxTs

VARIABLES
    \* @type: Set(Int);
    activeSnapshots,
    \* @type: Set(Int);
    activeLocks,
    \* @type: Set(Int);
    versions,
    \* @type: Set(Int);
    rollbackMarkers,
    \* @type: Set(Int);
    defaultTombstones,
    \* @type: Set(Int);
    removedRollbackMarkers,
    \* @type: Set(Int);
    removedDefaultTombstones,
    \* @type: Set(Int);
    removedVersions,
    \* @type: Int;
    safepoint

Vars ==
    << activeSnapshots, activeLocks, versions, rollbackMarkers,
       defaultTombstones, removedRollbackMarkers, removedDefaultTombstones,
       removedVersions, safepoint >>

Init ==
    /\ activeSnapshots = {}
    /\ activeLocks = {}
    /\ versions = {1}
    /\ rollbackMarkers = {}
    /\ defaultTombstones = {}
    /\ removedRollbackMarkers = {}
    /\ removedDefaultTombstones = {}
    /\ removedVersions = {}
    /\ safepoint = 0

AddSnapshot ==
    \E ts \in Times:
        /\ ts > 0
        /\ ts > safepoint
        /\ \E v \in versions: v <= ts
        /\ activeSnapshots' = activeSnapshots \cup {ts}
        /\ UNCHANGED <<activeLocks, versions, rollbackMarkers,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

RetireSnapshot ==
    \E ts \in activeSnapshots:
        /\ activeSnapshots' = activeSnapshots \ {ts}
        /\ UNCHANGED <<activeLocks, versions, rollbackMarkers,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

AddLock ==
    \E ts \in Times:
        /\ ts > 0
        /\ ts > safepoint
        /\ activeLocks' = activeLocks \cup {ts}
        /\ UNCHANGED <<activeSnapshots, versions, rollbackMarkers,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

ResolveLock ==
    \E ts \in activeLocks:
        /\ activeLocks' = activeLocks \ {ts}
        /\ UNCHANGED <<activeSnapshots, versions, rollbackMarkers,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

AddVersion ==
    \E ts \in Times:
        /\ ts > 0
        /\ versions' = versions \cup {ts}
        /\ UNCHANGED <<activeSnapshots, activeLocks, rollbackMarkers,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

AddRollbackMarker ==
    \E ts \in Times:
        /\ ts > 0
        /\ rollbackMarkers' = rollbackMarkers \cup {ts}
        /\ UNCHANGED <<activeSnapshots, activeLocks, versions,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

AddDefaultTombstone ==
    \E ts \in Times:
        /\ ts > 0
        /\ defaultTombstones' = defaultTombstones \cup {ts}
        /\ UNCHANGED <<activeSnapshots, activeLocks, versions,
                       rollbackMarkers, removedRollbackMarkers,
                       removedDefaultTombstones, removedVersions, safepoint>>

CanAdvanceTo(ts) ==
    /\ ts >= safepoint
    /\ \A s \in activeSnapshots: ts < s
    /\ \A l \in activeLocks: ts < l

AdvanceSafepoint ==
    \E ts \in Times:
        /\ CanAdvanceTo(ts)
        /\ safepoint' = ts
        /\ UNCHANGED <<activeSnapshots, activeLocks, versions,
                       rollbackMarkers, defaultTombstones,
                       removedRollbackMarkers, removedDefaultTombstones,
                       removedVersions>>

CanRemoveVersion(v) ==
    \A s \in activeSnapshots:
        \E kept \in versions \ {v}: kept <= s

GCVersion ==
    \E v \in versions:
        /\ v <= safepoint
        /\ CanRemoveVersion(v)
        /\ versions' = versions \ {v}
        /\ removedVersions' = removedVersions \cup {v}
        /\ UNCHANGED <<activeSnapshots, activeLocks, rollbackMarkers,
                       defaultTombstones, removedRollbackMarkers,
                       removedDefaultTombstones, safepoint>>

GCRollbackMarker ==
    \E ts \in rollbackMarkers:
        /\ ts <= safepoint
        /\ rollbackMarkers' = rollbackMarkers \ {ts}
        /\ removedRollbackMarkers' = removedRollbackMarkers \cup {ts}
        /\ UNCHANGED <<activeSnapshots, activeLocks, versions,
                       defaultTombstones, removedDefaultTombstones,
                       removedVersions, safepoint>>

GCDefaultTombstone ==
    \E ts \in defaultTombstones:
        /\ ts <= safepoint
        /\ defaultTombstones' = defaultTombstones \ {ts}
        /\ removedDefaultTombstones' = removedDefaultTombstones \cup {ts}
        /\ UNCHANGED <<activeSnapshots, activeLocks, versions,
                       rollbackMarkers, removedRollbackMarkers,
                       removedVersions, safepoint>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ AddSnapshot
    \/ RetireSnapshot
    \/ AddLock
    \/ ResolveLock
    \/ AddVersion
    \/ AddRollbackMarker
    \/ AddDefaultTombstone
    \/ AdvanceSafepoint
    \/ GCVersion
    \/ GCRollbackMarker
    \/ GCDefaultTombstone
    \/ Stutter

TypeOK ==
    /\ activeSnapshots \subseteq Times
    /\ activeLocks \subseteq Times
    /\ versions \subseteq Times
    /\ rollbackMarkers \subseteq Times
    /\ defaultTombstones \subseteq Times
    /\ removedRollbackMarkers \subseteq Times
    /\ removedDefaultTombstones \subseteq Times
    /\ removedVersions \subseteq Times
    /\ safepoint \in Times

SafepointBelowActiveSnapshots ==
    \A s \in activeSnapshots: safepoint < s

SafepointBelowActiveLocks ==
    \A l \in activeLocks: safepoint < l

ActiveSnapshotsHaveVisibleVersion ==
    \A s \in activeSnapshots:
        \E v \in versions: v <= s

RemovedRollbackMarkersWereSafe ==
    \A ts \in removedRollbackMarkers: ts <= safepoint

RemovedDefaultTombstonesWereSafe ==
    \A ts \in removedDefaultTombstones: ts <= safepoint

RemovedVersionsWereSafe ==
    \A ts \in removedVersions: ts <= safepoint

MVCCGCGuarantees ==
    /\ SafepointBelowActiveSnapshots
    /\ SafepointBelowActiveLocks
    /\ ActiveSnapshotsHaveVisibleVersion
    /\ RemovedRollbackMarkersWereSafe
    /\ RemovedDefaultTombstonesWereSafe
    /\ RemovedVersionsWereSafe

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
