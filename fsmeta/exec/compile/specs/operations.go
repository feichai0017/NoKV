package specs

import "github.com/feichai0017/NoKV/fsmeta/exec/compile/specdsl"

var Create = specdsl.OpSpec{
	Name:           "Create",
	FileName:       "create.peras.go",
	ProgramType:    "CreateProgram",
	ValuesType:     "CreateValues",
	RequestType:    "fsmeta.CreateRequest",
	CompileName:    "CompileCreateProgram",
	LoweringName:   "Create",
	Materialize:    "MaterializeCreate",
	OperationKind:  "fsmeta.OperationCreate",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    2,
	Emitter:        "create",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}, Inodes: []string{"inode"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_absent", Kind: "PredicateNotExists"},
		{Name: "inode_absent", Kind: "PredicateNotExists"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "dentry", Kind: "EffectPut", ValueName: "DentryValue"},
		{Name: "inode", Kind: "EffectPut", ValueName: "InodeValue"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var UpdateInode = specdsl.OpSpec{
	Name:           "UpdateInode",
	FileName:       "inode.peras.go",
	ProgramType:    "UpdateInodeProgram",
	RequestType:    "fsmeta.UpdateInodeRequest",
	CompileName:    "CompileUpdateInodeProgram",
	LoweringName:   "lowerUpdateInode",
	OperationKind:  "fsmeta.OperationUpdateInode",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    1,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}, Inodes: []string{"inode"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_observed", Kind: "PredicateObservedValue"},
		{Name: "inode_observed", Kind: "PredicateObservedValue"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "inode", Kind: "EffectDerivedPut"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "single_link_inode", Guard: "GuardSingleLinkInode"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var Lookup = specdsl.OpSpec{
	Name:           "Lookup",
	FileName:       "lookup.peras.go",
	ProgramType:    "LookupProgram",
	RequestType:    "fsmeta.LookupRequest",
	CompileName:    "CompileLookupProgram",
	LoweringName:   "lowerLookup",
	OperationKind:  "fsmeta.OperationLookup",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilitySlowPath",
	SlowReason:     "SlowReasonReadOnly",
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_exists", Kind: "PredicateExists"},
	},
}

var ReadDir = specdsl.OpSpec{
	Name:           "ReadDir",
	FileName:       "readdir.peras.go",
	ProgramType:    "ReadDirProgram",
	RequestType:    "fsmeta.ReadDirRequest",
	CompileName:    "CompileReadDirProgram",
	LoweringName:   "lowerReadDir",
	OperationKind:  "fsmeta.OperationReadDir",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilitySlowPath",
	SlowReason:     "SlowReasonRangeRead",
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_prefix", Kind: "PredicatePrefixScan"},
	},
}

var SnapshotSubtree = specdsl.OpSpec{
	Name:              "SnapshotSubtree",
	FileName:          "snapshot.peras.go",
	ProgramType:       "SnapshotSubtreeProgram",
	RequestType:       "fsmeta.SnapshotSubtreeRequest",
	CompileName:       "CompileSnapshotSubtreeProgram",
	LoweringName:      "lowerSnapshotSubtree",
	OperationKind:     "fsmeta.OperationSnapshotSubtree",
	Durability:        "DurabilityNeedsPublishCheckpoint",
	Eligibility:       "EligibilitySlowPath",
	SlowReason:        "SlowReasonDurabilityBarrier",
	DurabilityBarrier: true,
	PredicateCount:    1,
	EffectCount:       0,
	Emitter:           "operation",
	Authority:         specdsl.AuthoritySpec{Parents: []string{"root"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "subtree_prefix", Kind: "PredicatePrefixScan"},
	},
}

var Rename = specdsl.OpSpec{
	Name:           "Rename",
	FileName:       "rename.peras.go",
	ProgramType:    "RenameProgram",
	RequestType:    "fsmeta.RenameRequest",
	CompileName:    "CompileRenameProgram",
	LoweringName:   "lowerRename",
	OperationKind:  "fsmeta.OperationRename",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    2,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"from_parent", "to_parent"}},
	SlowFallbacks:  []string{"SlowReasonCrossBucket"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "from_dentry_exists", Kind: "PredicateExists"},
		{Name: "to_dentry_absent", Kind: "PredicateNotExists"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "from_dentry", Kind: "EffectDelete"},
		{Name: "to_dentry", Kind: "EffectDerivedPut"},
	},
}

var RenameSubtree = specdsl.OpSpec{
	Name:              "RenameSubtree",
	FileName:          "rename.peras.go",
	ProgramType:       "RenameSubtreeProgram",
	RequestType:       "fsmeta.RenameSubtreeRequest",
	CompileName:       "CompileRenameSubtreeProgram",
	LoweringName:      "lowerRenameSubtree",
	OperationKind:     "fsmeta.OperationRenameSubtree",
	Durability:        "DurabilityNeedsPublishCheckpoint",
	Eligibility:       "EligibilitySlowPath",
	SlowReason:        "SlowReasonDurabilityBarrier",
	DurabilityBarrier: true,
	WatchAtSeal:       true,
	PredicateCount:    2,
	EffectCount:       2,
	Emitter:           "operation",
	Authority:         specdsl.AuthoritySpec{Parents: []string{"from_parent", "to_parent"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "from_dentry_exists", Kind: "PredicateExists"},
		{Name: "to_dentry_absent", Kind: "PredicateNotExists"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "from_dentry", Kind: "EffectDelete"},
		{Name: "to_dentry", Kind: "EffectDerivedPut"},
	},
}

var Link = specdsl.OpSpec{
	Name:           "Link",
	FileName:       "link.peras.go",
	ProgramType:    "LinkProgram",
	RequestType:    "fsmeta.LinkRequest",
	CompileName:    "CompileLinkProgram",
	LoweringName:   "lowerLink",
	OperationKind:  "fsmeta.OperationLink",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    2,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"from_parent", "to_parent"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "from_dentry_observed", Kind: "PredicateObservedValue"},
		{Name: "to_dentry_absent", Kind: "PredicateNotExists"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "from_inode", Kind: "EffectDerivedPut"},
		{Name: "to_dentry", Kind: "EffectDerivedPut"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "non_directory_inode", Guard: "GuardNonDirectoryInode"},
		{Name: "same_authority", Guard: "GuardSameAuthority"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var Unlink = specdsl.OpSpec{
	Name:           "Unlink",
	FileName:       "unlink.peras.go",
	ProgramType:    "UnlinkProgram",
	RequestType:    "fsmeta.UnlinkRequest",
	CompileName:    "CompileUnlinkProgram",
	LoweringName:   "lowerUnlink",
	OperationKind:  "fsmeta.OperationUnlink",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: -1,
	EffectCount:    2,
	HasOptions:     true,
	Emitter:        "operation",
	Authority:      specdsl.AuthoritySpec{Parents: []string{"parent"}},
	SlowFallbacks:  []string{"SlowReasonSharedQuota"},
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_observed", Kind: "PredicateObservedValue"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "dentry", Kind: "EffectDelete"},
		{Name: "inode", Kind: "EffectDerivedPut"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "non_directory_inode", Guard: "GuardNonDirectoryInode"},
	},
	OptionalGuards: []specdsl.GuardSpec{
		{Name: "quota_credit", Guard: "GuardQuotaCredit", Condition: "quota_escrow"},
	},
}

var OpenWriteSession = specdsl.OpSpec{
	Name:           "OpenWriteSession",
	FileName:       "session.peras.go",
	ProgramType:    "OpenWriteSessionProgram",
	ValuesType:     "OpenWriteSessionValues",
	RequestType:    "fsmeta.OpenWriteSessionRequest",
	CompileName:    "CompileOpenWriteSessionProgram",
	LoweringName:   "lowerOpenWriteSession",
	Materialize:    "MaterializeOpenWriteSession",
	OperationKind:  "fsmeta.OperationOpenWriteSession",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 3,
	EffectCount:    2,
	Emitter:        "operation",
	Materializer:   "session_put",
	Authority:      specdsl.AuthoritySpec{Inodes: []string{"inode"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "inode_observed", Kind: "PredicateObservedValue"},
		{Name: "session_observed", Kind: "PredicateObservedValue"},
		{Name: "owner_observed", Kind: "PredicateObservedValue"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "session", Kind: "EffectDerivedPut"},
		{Name: "owner", Kind: "EffectDerivedPut"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "non_directory_inode", Guard: "GuardNonDirectoryInode"},
		{Name: "expired_session_owner", Guard: "GuardExpiredSessionOwner"},
	},
}

var HeartbeatWriteSession = specdsl.OpSpec{
	Name:           "HeartbeatWriteSession",
	FileName:       "session.peras.go",
	ProgramType:    "HeartbeatWriteSessionProgram",
	ValuesType:     "HeartbeatWriteSessionValues",
	RequestType:    "fsmeta.HeartbeatWriteSessionRequest",
	CompileName:    "CompileHeartbeatWriteSessionProgram",
	LoweringName:   "lowerHeartbeatWriteSession",
	Materialize:    "MaterializeHeartbeatWriteSession",
	OperationKind:  "fsmeta.OperationHeartbeatSession",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    2,
	Emitter:        "operation",
	Materializer:   "session_put",
	Authority:      specdsl.AuthoritySpec{Inodes: []string{"inode"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "session_observed", Kind: "PredicateObservedValue"},
		{Name: "owner_observed", Kind: "PredicateObservedValue"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "session", Kind: "EffectDerivedPut"},
		{Name: "owner", Kind: "EffectDerivedPut"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "live_session", Guard: "GuardLiveSession"},
	},
}

var CloseWriteSession = specdsl.OpSpec{
	Name:           "CloseWriteSession",
	FileName:       "session.peras.go",
	ProgramType:    "CloseWriteSessionProgram",
	ValuesType:     "CloseWriteSessionValues",
	RequestType:    "fsmeta.CloseWriteSessionRequest",
	CompileName:    "CompileCloseWriteSessionProgram",
	LoweringName:   "lowerCloseWriteSession",
	Materialize:    "MaterializeCloseWriteSession",
	OperationKind:  "fsmeta.OperationCloseSession",
	Durability:     "DurabilityNeedsCloseSession",
	Eligibility:    "EligibilityVisibleCommit",
	PredicateCount: 2,
	EffectCount:    -1,
	Emitter:        "operation",
	Materializer:   "session_close",
	Authority:      specdsl.AuthoritySpec{Inodes: []string{"inode"}},
	Predicates: []specdsl.PredicateSpec{
		{Name: "session_observed", Kind: "PredicateObservedValue"},
		{Name: "owner_observed", Kind: "PredicateObservedValue"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "session", Kind: "EffectDelete"},
		{Name: "owner", Kind: "EffectDerivedDelete"},
	},
	Guards: []specdsl.GuardSpec{
		{Name: "live_session", Guard: "GuardLiveSession"},
	},
}

var ExpireWriteSessions = specdsl.OpSpec{
	Name:           "ExpireWriteSessions",
	FileName:       "session.peras.go",
	ProgramType:    "ExpireWriteSessionsProgram",
	RequestType:    "fsmeta.ExpireWriteSessionsRequest",
	CompileName:    "CompileExpireWriteSessionsProgram",
	LoweringName:   "lowerExpireWriteSessions",
	OperationKind:  "fsmeta.OperationExpireSessions",
	Durability:     "DurabilityVisibleOnly",
	Eligibility:    "EligibilitySlowPath",
	SlowReason:     "SlowReasonMaintenanceScan",
	PredicateCount: -1,
	EffectCount:    0,
	Emitter:        "operation",
	Predicates: []specdsl.PredicateSpec{
		{Name: "session_prefix", Kind: "PredicatePrefixScan", Repeatable: true},
	},
}

func All() []specdsl.OpSpec {
	return []specdsl.OpSpec{
		Create,
		UpdateInode,
		Lookup,
		ReadDir,
		SnapshotSubtree,
		Rename,
		RenameSubtree,
		Link,
		Unlink,
		OpenWriteSession,
		HeartbeatWriteSession,
		CloseWriteSession,
		ExpireWriteSessions,
	}
}
