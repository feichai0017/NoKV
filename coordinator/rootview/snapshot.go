package rootview

import (
	"math"
	"slices"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

// AllocatorState captures persisted counters for ID and TSO allocators.
type AllocatorState struct {
	IDCurrent uint64
	TSCurrent uint64
}

type CatchUpState uint8

const (
	CatchUpStateUnspecified CatchUpState = iota
	CatchUpStateFresh
	CatchUpStateLagging
	CatchUpStateBootstrapRequired
	CatchUpStateUnavailable
)

func (s CatchUpState) String() string {
	switch s {
	case CatchUpStateFresh:
		return "fresh"
	case CatchUpStateLagging:
		return "lagging"
	case CatchUpStateBootstrapRequired:
		return "bootstrap_required"
	case CatchUpStateUnavailable:
		return "unavailable"
	default:
		return "unspecified"
	}
}

// Snapshot is the reconstructed Coordinator bootstrap catalog derived from durable
// metadata-root truth.
type Snapshot struct {
	ClusterEpoch        uint64
	RootToken           rootstorage.TailToken
	CatchUpState        CatchUpState
	Stores              map[uint64]rootstate.StoreMembership
	Mounts              map[string]rootstate.MountRecord
	Descriptors         map[uint64]descriptor.Descriptor
	PendingPeerChanges  map[uint64]rootstate.PendingPeerChange
	PendingRangeChanges map[uint64]rootstate.PendingRangeChange
	Allocator           AllocatorState
	Tenure              rootstate.Tenure
	Legacy              rootstate.Legacy
	Handover            rootstate.Handover
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{
		ClusterEpoch:        snapshot.ClusterEpoch,
		RootToken:           snapshot.RootToken,
		CatchUpState:        snapshot.CatchUpState,
		Stores:              rootstate.CloneStoreMemberships(snapshot.Stores),
		Mounts:              rootstate.CloneMounts(snapshot.Mounts),
		Descriptors:         rootstate.CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges),
		Allocator:           snapshot.Allocator,
		Tenure:              snapshot.Tenure,
		Legacy:              snapshot.Legacy,
		Handover:            snapshot.Handover,
	}
}

func SnapshotFromRoot(snapshot rootstate.Snapshot) Snapshot {
	return Snapshot{
		ClusterEpoch: snapshot.State.ClusterEpoch,
		RootToken: rootstorage.TailToken{
			Cursor:   snapshot.State.LastCommitted,
			Revision: 0,
		},
		CatchUpState:        CatchUpStateFresh,
		Stores:              rootstate.CloneStoreMemberships(snapshot.Stores),
		Mounts:              rootstate.CloneMounts(snapshot.Mounts),
		Descriptors:         rootstate.CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(snapshot.PendingRangeChanges),
		Allocator: AllocatorState{
			IDCurrent: snapshot.State.IDFence,
			TSCurrent: snapshot.State.TSOFence,
		},
		Tenure:   snapshot.State.Tenure,
		Legacy:   snapshot.State.Legacy,
		Handover: snapshot.State.Handover,
	}
}

func (s Snapshot) RootSnapshot() rootstate.Snapshot {
	return rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:  s.ClusterEpoch,
			LastCommitted: s.RootToken.Cursor,
			IDFence:       s.Allocator.IDCurrent,
			TSOFence:      s.Allocator.TSCurrent,
			Tenure:        s.Tenure,
			Legacy:        s.Legacy,
			Handover:      s.Handover,
		},
		Stores:              rootstate.CloneStoreMemberships(s.Stores),
		Mounts:              rootstate.CloneMounts(s.Mounts),
		Descriptors:         rootstate.CloneDescriptors(s.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.PendingRangeChanges),
	}
}

// BootstrapInfo captures rooted Coordinator bootstrap results.
type BootstrapInfo struct {
	LoadedRegions int
	IDStart       uint64
	TSStart       uint64
	Snapshot      Snapshot
}

// ResolveAllocatorStarts raises starts to checkpoint+1 when needed.
func ResolveAllocatorStarts(idStart, tsStart uint64, state AllocatorState) (uint64, uint64) {
	nextID := state.IDCurrent
	if nextID < math.MaxUint64 {
		nextID++
	}
	if nextID > idStart {
		idStart = nextID
	}

	nextTS := state.TSCurrent
	if nextTS < math.MaxUint64 {
		nextTS++
	}
	if nextTS > tsStart {
		tsStart = nextTS
	}
	return idStart, tsStart
}

// RestoreDescriptors replays a rooted descriptor catalog into one runtime cluster view.
func RestoreDescriptors(apply func(descriptor.Descriptor) error, descriptors map[uint64]descriptor.Descriptor) (int, error) {
	if apply == nil || len(descriptors) == 0 {
		return 0, nil
	}
	ids := make([]uint64, 0, len(descriptors))
	for id := range descriptors {
		if id == 0 {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)

	loaded := 0
	for _, id := range ids {
		desc := descriptors[id]
		if desc.RegionID == 0 {
			continue
		}
		if err := apply(desc); err != nil {
			return loaded, err
		}
		loaded++
	}
	return loaded, nil
}

// Bootstrap reconstructs one Coordinator runtime view from rooted durable metadata and
// resolves allocator starts against persisted fences.
func Bootstrap(store RootStorage, apply func(descriptor.Descriptor) error, idStart, tsStart uint64) (BootstrapInfo, error) {
	if store == nil {
		return BootstrapInfo{IDStart: idStart, TSStart: tsStart}, nil
	}
	snapshot, err := store.Load()
	if err != nil {
		return BootstrapInfo{}, err
	}
	loadedRegions, err := RestoreDescriptors(apply, snapshot.Descriptors)
	if err != nil {
		return BootstrapInfo{}, err
	}
	idStart, tsStart = ResolveAllocatorStarts(idStart, tsStart, snapshot.Allocator)
	return BootstrapInfo{
		LoadedRegions: loadedRegions,
		IDStart:       idStart,
		TSStart:       tsStart,
		Snapshot:      snapshot,
	}, nil
}
