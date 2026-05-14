\* Copyright 2024-2026 The NoKV Authors.
\* SPDX-License-Identifier: Apache-2.0

--------------------------- MODULE FSMetaNamespace ---------------------------
EXTENDS Naturals, FiniteSets

\* Small fsmeta namespace model. It intentionally models only a bounded root
\* directory with create/link/unlink/rename/snapshot/session operations; complex
\* mixed histories remain the job of the Go history checker.

CONSTANTS
    \* @type: Int;
    MaxName,
    \* @type: Int;
    MaxInode,
    \* @type: Int;
    MaxSnapshot,
    \* @type: Int;
    MaxTime,
    \* @type: Int;
    SessionTTL

Names == 0..MaxName
Inodes == 1..MaxInode
Snapshots == 0..MaxSnapshot
Times == 0..MaxTime
NoInode == 0

EmptyDentry == [n \in Names |-> NoInode]
EmptyLinkCount == [i \in Inodes |-> 0]
EmptySessionExpiry == [i \in Inodes |-> 0]
EmptySnapshots == [s \in Snapshots |-> EmptyDentry]

VARIABLES
    \* @type: Int -> Int;
    dentry,
    \* @type: Set(Int);
    liveInodes,
    \* @type: Set(Int);
    everInodes,
    \* @type: Int -> Int;
    linkCount,
    \* @type: Set(Int);
    snapshots,
    \* @type: Int -> (Int -> Int);
    snapshotDentry,
    \* @type: Set(Int);
    sessions,
    \* @type: Int -> Int;
    sessionExpiry,
    \* @type: Int;
    now

Vars ==
    << dentry, liveInodes, everInodes, linkCount, snapshots,
       snapshotDentry, sessions, sessionExpiry, now >>

Init ==
    /\ dentry = EmptyDentry
    /\ liveInodes = {}
    /\ everInodes = {}
    /\ linkCount = EmptyLinkCount
    /\ snapshots = {}
    /\ snapshotDentry = EmptySnapshots
    /\ sessions = {}
    /\ sessionExpiry = EmptySessionExpiry
    /\ now = 0

NamesFor(inode) ==
    { n \in Names : dentry[n] = inode }

LiveInodesFromDentry ==
    { i \in Inodes : Cardinality(NamesFor(i)) > 0 }

LiveSessionsAt(t) ==
    { i \in sessions : sessionExpiry[i] > t }

Create ==
    \E n \in Names:
        /\ dentry[n] = NoInode
        /\ \E i \in Inodes:
            /\ i \notin everInodes
            /\ dentry' = [dentry EXCEPT ![n] = i]
            /\ liveInodes' = liveInodes \cup {i}
            /\ everInodes' = everInodes \cup {i}
            /\ linkCount' = [linkCount EXCEPT ![i] = 1]
        /\ UNCHANGED <<snapshots, snapshotDentry, sessions, sessionExpiry, now>>

Link ==
    \E src \in Names:
        /\ dentry[src] # NoInode
        /\ \E dst \in Names:
            /\ src # dst
            /\ dentry[dst] = NoInode
            /\ LET i == dentry[src] IN
               /\ linkCount[i] < MaxName + 1
               /\ dentry' = [dentry EXCEPT ![dst] = i]
               /\ linkCount' = [linkCount EXCEPT ![i] = linkCount[i] + 1]
        /\ UNCHANGED <<liveInodes, everInodes, snapshots, snapshotDentry,
                       sessions, sessionExpiry, now>>

Unlink ==
    \E n \in Names:
        /\ dentry[n] # NoInode
        /\ LET i == dentry[n] IN
           LET newCount == linkCount[i] - 1 IN
           /\ dentry' = [dentry EXCEPT ![n] = NoInode]
           /\ linkCount' = [linkCount EXCEPT ![i] = newCount]
           /\ liveInodes' =
                IF newCount = 0 THEN liveInodes \ {i} ELSE liveInodes
           /\ sessions' =
                IF newCount = 0 THEN sessions \ {i} ELSE sessions
        /\ UNCHANGED <<everInodes, snapshots, snapshotDentry, sessionExpiry, now>>

Rename ==
    \E src \in Names:
        /\ dentry[src] # NoInode
        /\ \E dst \in Names:
            /\ src # dst
            /\ dentry[dst] = NoInode
            /\ dentry' = [dentry EXCEPT ![dst] = dentry[src], ![src] = NoInode]
        /\ UNCHANGED <<liveInodes, everInodes, linkCount, snapshots,
                       snapshotDentry, sessions, sessionExpiry, now>>

SnapshotSubtree ==
    \E sid \in Snapshots:
        /\ sid \notin snapshots
        /\ snapshots' = snapshots \cup {sid}
        /\ snapshotDentry' = [snapshotDentry EXCEPT ![sid] = dentry]
        /\ UNCHANGED <<dentry, liveInodes, everInodes, linkCount,
                       sessions, sessionExpiry, now>>

RetireSnapshot ==
    \E sid \in snapshots:
        /\ snapshots' = snapshots \ {sid}
        /\ UNCHANGED <<dentry, liveInodes, everInodes, linkCount,
                       snapshotDentry, sessions, sessionExpiry, now>>

OpenSession ==
    \E i \in liveInodes:
        /\ now + SessionTTL <= MaxTime
        /\ sessions' = sessions \cup {i}
        /\ sessionExpiry' = [sessionExpiry EXCEPT ![i] = now + SessionTTL]
        /\ UNCHANGED <<dentry, liveInodes, everInodes, linkCount,
                       snapshots, snapshotDentry, now>>

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ sessions' = LiveSessionsAt(now')
    /\ UNCHANGED <<dentry, liveInodes, everInodes, linkCount,
                   snapshots, snapshotDentry, sessionExpiry>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ Create
    \/ Link
    \/ Unlink
    \/ Rename
    \/ SnapshotSubtree
    \/ RetireSnapshot
    \/ OpenSession
    \/ AdvanceTime
    \/ Stutter

TypeOK ==
    /\ dentry \in [Names -> 0..MaxInode]
    /\ liveInodes \subseteq Inodes
    /\ everInodes \subseteq Inodes
    /\ linkCount \in [Inodes -> 0..(MaxName + 1)]
    /\ snapshots \subseteq Snapshots
    /\ snapshotDentry \in [Snapshots -> [Names -> 0..MaxInode]]
    /\ sessions \subseteq Inodes
    /\ sessionExpiry \in [Inodes -> Times]
    /\ now \in Times

ActiveDentriesPointToLiveInodes ==
    \A n \in Names:
        dentry[n] # NoInode => dentry[n] \in liveInodes

LiveInodesMatchDentries ==
    liveInodes = LiveInodesFromDentry

LinkCountMatchesDentries ==
    \A i \in Inodes:
        linkCount[i] = Cardinality(NamesFor(i))

SessionsTargetLiveInodes ==
    sessions \subseteq liveInodes

SessionsNotExpired ==
    \A i \in sessions:
        sessionExpiry[i] > now

SnapshotsPointToEverInodes ==
    \A sid \in snapshots:
        \A n \in Names:
            snapshotDentry[sid][n] = NoInode \/ snapshotDentry[sid][n] \in everInodes

FSMetaNamespaceGuarantees ==
    /\ ActiveDentriesPointToLiveInodes
    /\ LiveInodesMatchDentries
    /\ LinkCountMatchesDentries
    /\ SessionsTargetLiveInodes
    /\ SessionsNotExpired
    /\ SnapshotsPointToEverInodes

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
