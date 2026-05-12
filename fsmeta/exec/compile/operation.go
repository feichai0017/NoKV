package compile

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/feichai0017/NoKV/fsmeta"
)

const segmentFormatVersion uint16 = 1

// CompiledOp is the segment-installable semantic descriptor for one metadata
// operation. SemanticDelta is the executor-facing mutation contract; this
// descriptor is the compiler boundary Peras needs for admission, segment
// packing, recovery, completion, and watch projection.
type CompiledOp struct {
	Delta      SemanticDelta
	Authority  AuthorityPlan
	Placement  PlacementPlan
	Predicates []PredicateObligation
	Effects    []EffectPlan
	Atomicity  AtomicityGroup
	Durability DurabilityClass
	Watch      []WatchProjection
}

type FenceMode uint8

const (
	FenceNone FenceMode = iota
	FenceActiveAuthority
)

type AuthorityPlan struct {
	Scope    AuthorityScope
	Required bool
	Fence    FenceMode
}

type SegmentInstallMode uint8

const (
	SegmentInstallNone SegmentInstallMode = iota
	SegmentInstallSingleBucket
	SegmentInstallCatalog
)

type SegmentMergeKey struct {
	MountKeyID    fsmeta.MountKeyID
	PrimaryBucket fsmeta.AffinityBucket
	Install       SegmentInstallMode
	Durability    DurabilityClass
	FormatVersion uint16
}

type PlacementPlan struct {
	MountKeyID          fsmeta.MountKeyID
	Buckets             []fsmeta.AffinityBucket
	SingleBucket        bool
	Install             SegmentInstallMode
	CanSegment          bool
	RequiresMaterialize bool
	SlowReason          SlowReason
	MergeKey            SegmentMergeKey
}

type PredicateObligation struct {
	Kind       PredicateKind
	Key        []byte
	NeedValue  bool
	NeedAbsent bool
	Guard      RuntimeGuard
}

type ReadSource uint8

const (
	ReadSourceUnknown ReadSource = iota
	ReadSourceOverlay
	ReadSourceSegment
	ReadSourceBase
)

type PredicateProof struct {
	Key     []byte
	Present bool
	Value   []byte
	Version uint64
	Source  ReadSource
	Digest  [32]byte
}

type DerivationKind uint8

const (
	DerivationNone DerivationKind = iota
	DerivationRuntimeValue
)

type EffectPlan struct {
	Kind       EffectKind
	Key        []byte
	Value      []byte
	Derivation DerivationKind
}

type MutationID uint16

type RecoveryRule uint8

const (
	RecoveryReplayAllOrNothing RecoveryRule = iota
)

type AtomicityGroup struct {
	Members    []MutationID
	Splittable bool
	Recovery   RecoveryRule
}

type DurabilityClass uint8

const (
	DurabilityVisibleOnly DurabilityClass = iota
	DurabilityNeedsFsyncDir
	DurabilityNeedsCloseSession
	DurabilityNeedsPublishCheckpoint
)

type WatchEmitPoint uint8

const (
	WatchEmitVisible WatchEmitPoint = iota
	WatchEmitSeal
)

type WatchEventKind uint8

const (
	WatchEventUnknown WatchEventKind = iota
	WatchEventCreate
	WatchEventDelete
	WatchEventRename
	WatchEventUpdate
)

type WatchProjection struct {
	EventKind WatchEventKind
	Parent    fsmeta.InodeID
	Name      string
	Inode     fsmeta.InodeID
	EmitAt    WatchEmitPoint
}

func CompileDelta(delta SemanticDelta) CompiledOp {
	delta = cloneDelta(delta)
	durability := durabilityClass(delta)
	placement := placementPlan(delta, durability)
	return CompiledOp{
		Delta: delta,
		Authority: AuthorityPlan{
			Scope:    cloneScope(delta.Authority),
			Required: delta.Eligibility == EligibilityVisibleCommit,
			Fence:    fenceMode(delta),
		},
		Placement:  placement,
		Predicates: predicateObligations(delta),
		Effects:    effectPlans(delta),
		Atomicity:  atomicityGroup(delta),
		Durability: durability,
		Watch:      watchProjections(delta),
	}
}

func fenceMode(delta SemanticDelta) FenceMode {
	if delta.Eligibility != EligibilityVisibleCommit {
		return FenceNone
	}
	return FenceActiveAuthority
}

func durabilityClass(delta SemanticDelta) DurabilityClass {
	if !delta.DurabilityBarrier {
		return DurabilityVisibleOnly
	}
	switch delta.Kind {
	case fsmeta.OperationSnapshotSubtree, fsmeta.OperationRenameSubtree:
		return DurabilityNeedsPublishCheckpoint
	default:
		return DurabilityNeedsFsyncDir
	}
}

func placementPlan(delta SemanticDelta, durability DurabilityClass) PlacementPlan {
	out := PlacementPlan{
		MountKeyID: delta.Authority.MountKeyID,
		Buckets:    append([]fsmeta.AffinityBucket(nil), delta.Authority.Buckets...),
		SlowReason: delta.SlowReason,
	}
	out.SingleBucket = len(out.Buckets) == 1
	if delta.Eligibility != EligibilityVisibleCommit || delta.DurabilityBarrier || len(delta.WriteEffects) == 0 {
		return out
	}
	var mount fsmeta.MountKeyID
	var fsmetaKeys bool
	var opaqueKeys bool
	buckets := make([]fsmeta.AffinityBucket, 0, len(delta.WriteEffects))
	for _, effect := range delta.WriteEffects {
		switch effect.Kind {
		case EffectPut:
			if len(effect.Key) == 0 || effect.Value == nil {
				out.RequiresMaterialize = true
				return out
			}
		case EffectDelete:
			if len(effect.Key) == 0 {
				out.RequiresMaterialize = true
				return out
			}
		case EffectDerivedPut, EffectDerivedDelete:
			out.RequiresMaterialize = true
			return out
		default:
			out.SlowReason = SlowReasonDynamicWriteSet
			return out
		}
		parts, ok := fsmeta.InspectKey(effect.Key)
		if !ok {
			if fsmetaKeys {
				out.SlowReason = SlowReasonDynamicWriteSet
				return out
			}
			opaqueKeys = true
			continue
		}
		if opaqueKeys {
			out.SlowReason = SlowReasonDynamicWriteSet
			return out
		}
		if !fsmetaKeys {
			mount = parts.MountKeyID
			fsmetaKeys = true
		} else if mount != parts.MountKeyID {
			out.SlowReason = SlowReasonCrossBucket
			return out
		}
		buckets = append(buckets, parts.Bucket)
	}
	if !fsmetaKeys {
		if opaqueKeys {
			out.CanSegment = true
			out.Install = SegmentInstallSingleBucket
			out.MergeKey.MountKeyID = out.MountKeyID
			out.MergeKey.Install = out.Install
			out.MergeKey.Durability = durability
			out.MergeKey.FormatVersion = segmentFormatVersion
		}
		return out
	}
	out.MountKeyID = mount
	out.Buckets = uniqueBuckets(buckets)
	out.SingleBucket = len(out.Buckets) == 1
	out.CanSegment = true
	if out.SingleBucket {
		out.Install = SegmentInstallSingleBucket
		out.MergeKey.PrimaryBucket = out.Buckets[0]
	} else {
		out.Install = SegmentInstallCatalog
	}
	out.MergeKey.MountKeyID = mount
	out.MergeKey.Install = out.Install
	out.MergeKey.Durability = durability
	out.MergeKey.FormatVersion = segmentFormatVersion
	return out
}

func predicateObligations(delta SemanticDelta) []PredicateObligation {
	out := make([]PredicateObligation, 0, len(delta.ReadPredicates))
	for _, predicate := range delta.ReadPredicates {
		obligation := PredicateObligation{
			Kind: predicate.Kind,
			Key:  cloneBytes(predicate.Key),
		}
		switch predicate.Kind {
		case PredicateNotExists:
			obligation.NeedAbsent = true
		case PredicateObservedValue:
			obligation.NeedValue = true
		}
		out = append(out, obligation)
	}
	return out
}

func effectPlans(delta SemanticDelta) []EffectPlan {
	out := make([]EffectPlan, 0, len(delta.WriteEffects))
	for _, effect := range delta.WriteEffects {
		plan := EffectPlan{
			Kind:  effect.Kind,
			Key:   cloneBytes(effect.Key),
			Value: cloneBytes(effect.Value),
		}
		switch effect.Kind {
		case EffectDerivedPut, EffectDerivedDelete:
			plan.Derivation = DerivationRuntimeValue
		}
		out = append(out, plan)
	}
	return out
}

func atomicityGroup(delta SemanticDelta) AtomicityGroup {
	group := AtomicityGroup{
		Members:  make([]MutationID, 0, len(delta.WriteEffects)),
		Recovery: RecoveryReplayAllOrNothing,
	}
	for i := range delta.WriteEffects {
		group.Members = append(group.Members, MutationID(i))
	}
	group.Splittable = len(group.Members) <= 1
	return group
}

func watchProjections(delta SemanticDelta) []WatchProjection {
	if len(delta.WriteEffects) == 0 {
		return nil
	}
	emitAt := WatchEmitVisible
	if delta.WatchAtSeal || delta.DurabilityBarrier {
		emitAt = WatchEmitSeal
	}
	out := make([]WatchProjection, 0, len(delta.WriteEffects))
	for _, effect := range delta.WriteEffects {
		if len(effect.Key) == 0 {
			continue
		}
		parts, ok := fsmeta.InspectKey(effect.Key)
		if !ok || parts.Kind != fsmeta.KeyKindDentry {
			continue
		}
		projection := WatchProjection{
			EventKind: watchEventKind(delta, effect),
			Parent:    parts.Parent,
			Name:      dentryName(effect.Key),
			EmitAt:    emitAt,
		}
		if len(effect.Value) > 0 {
			if dentry, err := fsmeta.DecodeDentryValue(effect.Value); err == nil {
				projection.Inode = dentry.Inode
			}
		}
		out = append(out, projection)
	}
	return out
}

func watchEventKind(delta SemanticDelta, effect WriteEffect) WatchEventKind {
	switch delta.Kind {
	case fsmeta.OperationCreate:
		return WatchEventCreate
	case fsmeta.OperationRename, fsmeta.OperationRenameSubtree:
		return WatchEventRename
	case fsmeta.OperationUnlink:
		return WatchEventDelete
	}
	switch effect.Kind {
	case EffectDelete, EffectDerivedDelete:
		return WatchEventDelete
	case EffectPut, EffectDerivedPut:
		return WatchEventUpdate
	default:
		return WatchEventUnknown
	}
}

func dentryName(key []byte) string {
	name, ok := fsmeta.DentryNameOfKey(key)
	if !ok {
		return ""
	}
	return name
}

func PredicateProofDigest(key, value []byte, present bool, version uint64, source ReadSource) [32]byte {
	h := sha256.New()
	h.Write(key)
	if present {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write(value)
	var buf [9]byte
	buf[0] = byte(source)
	binary.BigEndian.PutUint64(buf[1:], version)
	h.Write(buf[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func cloneDelta(delta SemanticDelta) SemanticDelta {
	return SemanticDelta{
		Kind:              delta.Kind,
		Plan:              clonePlan(delta.Plan),
		Authority:         cloneScope(delta.Authority),
		ReadPredicates:    clonePredicates(delta.ReadPredicates),
		WriteEffects:      cloneEffects(delta.WriteEffects),
		RuntimeGuards:     append([]RuntimeGuard(nil), delta.RuntimeGuards...),
		Eligibility:       delta.Eligibility,
		SlowReason:        delta.SlowReason,
		DurabilityBarrier: delta.DurabilityBarrier,
		WatchAtSeal:       delta.WatchAtSeal,
	}
}
