package rootraft

import (
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// Checkpoint is the compact materialized snapshot of the metadata-root state
// machine.
type Checkpoint struct {
	State       rootpkg.State
	Descriptors map[uint64]descriptor.Descriptor
}

func (cp Checkpoint) Clone() Checkpoint {
	out := Checkpoint{State: cp.State}
	if len(cp.Descriptors) > 0 {
		out.Descriptors = make(map[uint64]descriptor.Descriptor, len(cp.Descriptors))
		for id, desc := range cp.Descriptors {
			out.Descriptors[id] = desc.Clone()
		}
	}
	return out
}

// CheckpointStore persists compact root checkpoints. The first implementation
// can remain local-file backed; the raft package only depends on the interface.
type CheckpointStore interface {
	Load() (Checkpoint, error)
	Save(Checkpoint) error
}
