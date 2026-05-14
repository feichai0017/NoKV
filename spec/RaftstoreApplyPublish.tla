\* Copyright 2024-2026 The NoKV Authors.
\* SPDX-License-Identifier: Apache-2.0

------------------------- MODULE RaftstoreApplyPublish -------------------------
EXTENDS Naturals

\* Bounded positive model for raft Ready handling and snapshot publication.
\* It separates apply, publish, Ready advance, outbound send, and interrupted
\* snapshot install so TLC can check the ordering contract directly.

VARIABLES
    \* @type: Bool;
    ready,
    \* @type: Bool;
    applied,
    \* @type: Bool;
    published,
    \* @type: Bool;
    advanced,
    \* @type: Bool;
    sent,
    \* @type: Bool;
    failedAfterAdvance,
    \* @type: Bool;
    snapshotInstalling,
    \* @type: Bool;
    snapshotInstalled,
    \* @type: Bool;
    snapshotPublished,
    \* @type: Bool;
    snapshotAborted

Vars ==
    << ready, applied, published, advanced, sent, failedAfterAdvance,
       snapshotInstalling, snapshotInstalled, snapshotPublished, snapshotAborted >>

Init ==
    /\ ready = FALSE
    /\ applied = FALSE
    /\ published = FALSE
    /\ advanced = FALSE
    /\ sent = FALSE
    /\ failedAfterAdvance = FALSE
    /\ snapshotInstalling = FALSE
    /\ snapshotInstalled = FALSE
    /\ snapshotPublished = FALSE
    /\ snapshotAborted = FALSE

BeginReady ==
    /\ ~ready
    /\ ~advanced
    /\ ready' = TRUE
    /\ UNCHANGED <<applied, published, advanced, sent, failedAfterAdvance,
                   snapshotInstalling, snapshotInstalled, snapshotPublished,
                   snapshotAborted>>

ApplyReady ==
    /\ ready
    /\ ~applied
    /\ applied' = TRUE
    /\ UNCHANGED <<ready, published, advanced, sent, failedAfterAdvance,
                   snapshotInstalling, snapshotInstalled, snapshotPublished,
                   snapshotAborted>>

PublishApply ==
    /\ applied
    /\ ~published
    /\ published' = TRUE
    /\ UNCHANGED <<ready, applied, advanced, sent, failedAfterAdvance,
                   snapshotInstalling, snapshotInstalled, snapshotPublished,
                   snapshotAborted>>

AdvanceReady ==
    /\ ready
    /\ applied
    /\ ~advanced
    /\ advanced' = TRUE
    /\ UNCHANGED <<ready, applied, published, sent, failedAfterAdvance,
                   snapshotInstalling, snapshotInstalled, snapshotPublished,
                   snapshotAborted>>

FailAfterAdvanceBeforeSend ==
    /\ advanced
    /\ ~sent
    /\ ~failedAfterAdvance
    /\ failedAfterAdvance' = TRUE
    /\ UNCHANGED <<ready, applied, published, advanced, sent,
                   snapshotInstalling, snapshotInstalled, snapshotPublished,
                   snapshotAborted>>

SendMessages ==
    /\ advanced
    /\ ~sent
    /\ sent' = TRUE
    /\ failedAfterAdvance' = FALSE
    /\ UNCHANGED <<ready, applied, published, advanced, snapshotInstalling,
                   snapshotInstalled, snapshotPublished, snapshotAborted>>

BeginSnapshotInstall ==
    /\ ~snapshotInstalling
    /\ ~snapshotInstalled
    /\ ~snapshotPublished
    /\ snapshotInstalling' = TRUE
    /\ snapshotAborted' = FALSE
    /\ UNCHANGED <<ready, applied, published, advanced, sent,
                   failedAfterAdvance, snapshotInstalled, snapshotPublished>>

CompleteSnapshotInstall ==
    /\ snapshotInstalling
    /\ ~snapshotAborted
    /\ snapshotInstalled' = TRUE
    /\ snapshotInstalling' = FALSE
    /\ UNCHANGED <<ready, applied, published, advanced, sent,
                   failedAfterAdvance, snapshotPublished, snapshotAborted>>

AbortSnapshotInstall ==
    /\ snapshotInstalling
    /\ snapshotInstalling' = FALSE
    /\ snapshotInstalled' = FALSE
    /\ snapshotPublished' = FALSE
    /\ snapshotAborted' = TRUE
    /\ UNCHANGED <<ready, applied, published, advanced, sent, failedAfterAdvance>>

PublishSnapshot ==
    /\ snapshotInstalled
    /\ ~snapshotAborted
    /\ snapshotPublished' = TRUE
    /\ UNCHANGED <<ready, applied, published, advanced, sent,
                   failedAfterAdvance, snapshotInstalling, snapshotInstalled,
                   snapshotAborted>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ BeginReady
    \/ ApplyReady
    \/ PublishApply
    \/ AdvanceReady
    \/ FailAfterAdvanceBeforeSend
    \/ SendMessages
    \/ BeginSnapshotInstall
    \/ CompleteSnapshotInstall
    \/ AbortSnapshotInstall
    \/ PublishSnapshot
    \/ Stutter

TypeOK ==
    /\ ready \in BOOLEAN
    /\ applied \in BOOLEAN
    /\ published \in BOOLEAN
    /\ advanced \in BOOLEAN
    /\ sent \in BOOLEAN
    /\ failedAfterAdvance \in BOOLEAN
    /\ snapshotInstalling \in BOOLEAN
    /\ snapshotInstalled \in BOOLEAN
    /\ snapshotPublished \in BOOLEAN
    /\ snapshotAborted \in BOOLEAN

PublishRequiresApply ==
    published => applied

AdvanceRequiresApply ==
    advanced => applied

SendRequiresAdvance ==
    sent => advanced

SnapshotPublishRequiresInstall ==
    snapshotPublished => snapshotInstalled

AbortedSnapshotNeverPublished ==
    snapshotAborted => ~snapshotPublished

RaftstoreApplyPublishGuarantees ==
    /\ PublishRequiresApply
    /\ AdvanceRequiresApply
    /\ SendRequiresAdvance
    /\ SnapshotPublishRequiresInstall
    /\ AbortedSnapshotNeverPublished

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
