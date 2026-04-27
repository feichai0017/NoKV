--------------------------- MODULE MountLifecycle ---------------------------
EXTENDS Naturals, FiniteSets

\* Rooted mount lifecycle model for fsmeta Stage 3.
\* A mount can be registered once, retired once, and never made active again.
\* Address/liveness are deliberately absent; they are runtime view, not rooted
\* membership truth.

CONSTANTS
    \* @type: Int;
    MaxMount,
    \* @type: Int;
    MaxRootInode,
    \* @type: Int;
    MaxSchemaVersion

Mounts   == 0..MaxMount
RootIDs  == 1..MaxRootInode
Schemas  == 1..MaxSchemaVersion
NoRoot   == 0
NoSchema == 0

VARIABLES
    \* @type: Set(Int);
    everRegistered,
    \* @type: Set(Int);
    active,
    \* @type: Set(Int);
    retired,
    \* @type: Int -> Int;
    rootInode,
    \* @type: Int -> Int;
    schemaVersion

Vars == <<everRegistered, active, retired, rootInode, schemaVersion>>

Init ==
    /\ everRegistered = {}
    /\ active = {}
    /\ retired = {}
    /\ rootInode = [m \in Mounts |-> NoRoot]
    /\ schemaVersion = [m \in Mounts |-> NoSchema]

RegisterMount ==
    \E m \in Mounts:
        /\ m \notin everRegistered
        /\ m \notin retired
        /\ \E root \in RootIDs:
            /\ \E schema \in Schemas:
                /\ everRegistered' = everRegistered \cup {m}
                /\ active' = active \cup {m}
                /\ rootInode' = [rootInode EXCEPT ![m] = root]
                /\ schemaVersion' = [schemaVersion EXCEPT ![m] = schema]
        /\ UNCHANGED retired

RetireMount ==
    \E m \in active:
        /\ active' = active \ {m}
        /\ retired' = retired \cup {m}
        /\ UNCHANGED <<everRegistered, rootInode, schemaVersion>>

Stutter ==
    UNCHANGED Vars

Next ==
    \/ RegisterMount
    \/ RetireMount
    \/ Stutter

TypeOK ==
    /\ everRegistered \subseteq Mounts
    /\ active \subseteq Mounts
    /\ retired \subseteq Mounts
    /\ rootInode \in [Mounts -> 0..MaxRootInode]
    /\ schemaVersion \in [Mounts -> 0..MaxSchemaVersion]

\* A retired mount cannot be active again.
MountRetirementTerminal ==
    active \cap retired = {}

\* Every active or retired mount was explicitly registered first.
NoImplicitMount ==
    active \cup retired \subseteq everRegistered

\* Root metadata is immutable after registration.
MountIdentityStable ==
    \A m \in everRegistered:
        /\ rootInode[m] # NoRoot
        /\ schemaVersion[m] # NoSchema

MountLifecycleGuarantees ==
    /\ MountRetirementTerminal
    /\ NoImplicitMount
    /\ MountIdentityStable

Spec ==
    Init /\ [][Next]_Vars

=============================================================================
