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
	PredicateCount: 2,
	EffectCount:    2,
	Emitter:        "create",
	Predicates: []specdsl.PredicateSpec{
		{Name: "dentry_absent", Kind: "PredicateNotExists"},
		{Name: "inode_absent", Kind: "PredicateNotExists"},
	},
	Effects: []specdsl.EffectSpec{
		{Name: "dentry", Kind: "EffectPut", ValueName: "DentryValue"},
		{Name: "inode", Kind: "EffectPut", ValueName: "InodeValue"},
	},
	ConflictNames: []string{"dentry", "inode"},
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
	PredicateCount: 2,
	EffectCount:    1,
	HasOptions:     true,
	Emitter:        "operation",
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
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
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
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
}

var SnapshotSubtree = specdsl.OpSpec{
	Name:           "SnapshotSubtree",
	FileName:       "snapshot.peras.go",
	ProgramType:    "SnapshotSubtreeProgram",
	RequestType:    "fsmeta.SnapshotSubtreeRequest",
	CompileName:    "CompileSnapshotSubtreeProgram",
	LoweringName:   "lowerSnapshotSubtree",
	OperationKind:  "fsmeta.OperationSnapshotSubtree",
	Durability:     "DurabilityNeedsPublishCheckpoint",
	PredicateCount: 1,
	EffectCount:    0,
	Emitter:        "operation",
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
	PredicateCount: -1,
	EffectCount:    2,
	Emitter:        "operation",
}

var RenameSubtree = specdsl.OpSpec{
	Name:           "RenameSubtree",
	FileName:       "rename.peras.go",
	ProgramType:    "RenameSubtreeProgram",
	RequestType:    "fsmeta.RenameSubtreeRequest",
	CompileName:    "CompileRenameSubtreeProgram",
	LoweringName:   "lowerRenameSubtree",
	OperationKind:  "fsmeta.OperationRenameSubtree",
	Durability:     "DurabilityNeedsPublishCheckpoint",
	PredicateCount: 2,
	EffectCount:    2,
	Emitter:        "operation",
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
	PredicateCount: -1,
	EffectCount:    2,
	HasOptions:     true,
	Emitter:        "operation",
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
	PredicateCount: -1,
	EffectCount:    2,
	HasOptions:     true,
	Emitter:        "operation",
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
	PredicateCount: 3,
	EffectCount:    2,
	Emitter:        "operation",
	Materializer:   "session_put",
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
	PredicateCount: 2,
	EffectCount:    2,
	Emitter:        "operation",
	Materializer:   "session_put",
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
	PredicateCount: 2,
	EffectCount:    -1,
	Emitter:        "operation",
	Materializer:   "session_close",
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
	PredicateCount: -1,
	EffectCount:    0,
	Emitter:        "operation",
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
