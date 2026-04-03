package root

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// CloneDescriptors returns a detached descriptor catalog copy.
func CloneDescriptors(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	return rootstate.CloneDescriptors(in)
}

// CloneEvent returns a detached rooted metadata event copy.
func CloneEvent(in Event) Event { return rootevent.CloneEvent(in) }
