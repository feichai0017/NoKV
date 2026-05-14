\* Copyright 2024-2026 The NoKV Authors.
\* SPDX-License-Identifier: Apache-2.0

---------------------------- MODULE RootReplayWatch ----------------------------
EXTENDS Naturals

\* Bounded positive model for rooted metadata event replay and watch delivery.
\* It checks root epoch monotonicity, snapshot/replay ordering, follower catch-up,
\* and gap-aware watch cursors.

CONSTANTS
    \* @type: Int;
    MaxEpoch,
    \* @type: Int;
    WatchWindow

Epochs == 0..MaxEpoch

VARIABLES
    \* @type: Int;
    rootEpoch,
    \* @type: Int;
    snapshotEpoch,
    \* @type: Int;
    followerEpoch,
    \* @type: Int;
    watchCursor,
    \* @type: Bool;
    watchNeedsReconcile,
    \* @type: Bool;
    snapshotReplayPending

Vars ==
    << rootEpoch, snapshotEpoch, followerEpoch, watchCursor,
       watchNeedsReconcile, snapshotReplayPending >>

Init ==
    /\ rootEpoch = 0
    /\ snapshotEpoch = 0
    /\ followerEpoch = 0
    /\ watchCursor = 0
    /\ watchNeedsReconcile = FALSE
    /\ snapshotReplayPending = FALSE

GapAt(cursor, epoch) ==
    cursor + WatchWindow < epoch

AppendRootEvent ==
    /\ rootEpoch < MaxEpoch
    /\ rootEpoch' = rootEpoch + 1
    /\ watchNeedsReconcile' = (watchNeedsReconcile \/ GapAt(watchCursor, rootEpoch'))
    /\ UNCHANGED <<snapshotEpoch, followerEpoch, watchCursor, snapshotReplayPending>>

PublishSnapshot ==
    /\ snapshotEpoch < rootEpoch
    /\ snapshotEpoch' = rootEpoch
    /\ snapshotReplayPending' = TRUE
    /\ UNCHANGED <<rootEpoch, followerEpoch, watchCursor, watchNeedsReconcile>>

ReplaySnapshot ==
    /\ snapshotReplayPending
    /\ followerEpoch <= snapshotEpoch
    /\ followerEpoch' = snapshotEpoch
    /\ snapshotReplayPending' = FALSE
    /\ UNCHANGED <<rootEpoch, snapshotEpoch, watchCursor, watchNeedsReconcile>>

WatchNext ==
    /\ ~watchNeedsReconcile
    /\ watchCursor < rootEpoch
    /\ watchCursor' = watchCursor + 1
    /\ followerEpoch' =
        IF followerEpoch < watchCursor'
        THEN watchCursor'
        ELSE followerEpoch
    /\ UNCHANGED <<rootEpoch, snapshotEpoch, watchNeedsReconcile, snapshotReplayPending>>

ReconcileWatch ==
    /\ watchNeedsReconcile
    /\ watchCursor' = rootEpoch
    /\ followerEpoch' = rootEpoch
    /\ watchNeedsReconcile' = FALSE
    /\ snapshotReplayPending' = FALSE
    /\ UNCHANGED <<rootEpoch, snapshotEpoch>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ AppendRootEvent
    \/ PublishSnapshot
    \/ ReplaySnapshot
    \/ WatchNext
    \/ ReconcileWatch
    \/ Stutter

TypeOK ==
    /\ rootEpoch \in Epochs
    /\ snapshotEpoch \in Epochs
    /\ followerEpoch \in Epochs
    /\ watchCursor \in Epochs
    /\ watchNeedsReconcile \in BOOLEAN
    /\ snapshotReplayPending \in BOOLEAN
    /\ WatchWindow \in Epochs

SnapshotNeverAheadOfRoot ==
    snapshotEpoch <= rootEpoch

FollowerNeverAheadOfRoot ==
    followerEpoch <= rootEpoch

WatchCursorNeverAheadOfRoot ==
    watchCursor <= rootEpoch

NoSilentWatchGap ==
    GapAt(watchCursor, rootEpoch) => watchNeedsReconcile

ReplayLagIsExplicit ==
    snapshotEpoch > followerEpoch => snapshotReplayPending \/ watchNeedsReconcile

RootReplayWatchGuarantees ==
    /\ SnapshotNeverAheadOfRoot
    /\ FollowerNeverAheadOfRoot
    /\ WatchCursorNeverAheadOfRoot
    /\ NoSilentWatchGap
    /\ ReplayLagIsExplicit

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
