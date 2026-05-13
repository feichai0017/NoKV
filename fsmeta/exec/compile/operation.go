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
	Delta            SemanticDelta
	DescriptorDigest [32]byte
	IntentDigest     [32]byte
	ReplayDigest     [32]byte
	Authority        AuthorityPlan
	Placement        PlacementPlan
	Footprint        KeyFootprint
	Predicates       []PredicateObligation
	Guards           []GuardObligation
	Effects          []EffectPlan
	Atomicity        AtomicityGroup
	Durability       DurabilityClass
	Watch            []WatchProjection
	Completion       CompletionPlan
	Segment          SegmentPlan
}

// MaterializedOp is the closed Peras IR admitted by the holder. It has the
// static descriptor from CompileDelta plus any runtime predicate proofs and
// concrete effects needed to install the operation inside a segment.
type MaterializedOp struct {
	CompiledOp
	PredicateProofs []PredicateProof
	GuardProofs     []GuardProof
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

type KeyAccessMode uint8

const (
	KeyAccessRead KeyAccessMode = iota
	KeyAccessReadPrefix
	KeyAccessWrite
)

type KeyRef struct {
	Mode       KeyAccessMode
	Key        []byte
	Opaque     bool
	MountKeyID fsmeta.MountKeyID
	Bucket     fsmeta.AffinityBucket
	Kind       fsmeta.KeyKind
	Parent     fsmeta.InodeID
	Inode      fsmeta.InodeID
}

type KeyFootprint struct {
	Reads          []KeyRef
	Writes         []KeyRef
	ConflictKeys   []KeyRef
	HasPrefixRead  bool
	HasOpaqueKeys  bool
	EstimatedBytes uint64
}

type PredicateObligation struct {
	Kind             PredicateKind
	Key              []byte
	NeedValue        bool
	NeedAbsent       bool
	Guard            RuntimeGuard
	HasExpectedValue bool
	ExpectHash       [32]byte
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

type GuardObligation struct {
	Guard  RuntimeGuard
	Digest [32]byte
}

type GuardProof struct {
	Guard  RuntimeGuard
	Passed bool
	Digest [32]byte
}

type DerivationKind uint8

const (
	DerivationNone DerivationKind = iota
	DerivationRuntimeValue
)

type EffectPlan struct {
	ID         MutationID
	Kind       EffectKind
	Key        []byte
	Value      []byte
	Concrete   bool
	Opaque     bool
	MountKeyID fsmeta.MountKeyID
	Bucket     fsmeta.AffinityBucket
	RecordKind fsmeta.KeyKind
	ValueHash  [32]byte
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
	Digest     [32]byte
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
	Key       []byte
	Parent    fsmeta.InodeID
	Name      string
	Inode     fsmeta.InodeID
	EmitAt    WatchEmitPoint
}

type CompletionKind uint8

const (
	CompletionNone CompletionKind = iota
	CompletionVisible
	CompletionDurable
)

type CompletionPlan struct {
	RetainCompletion bool
	Kind             CompletionKind
	MutationCount    uint32
	DescriptorDigest [32]byte
}

type SegmentPlan struct {
	MergeKey              SegmentMergeKey
	Install               SegmentInstallMode
	MaterializeMergeKey   SegmentMergeKey
	MaterializeInstall    SegmentInstallMode
	CanAppend             bool
	CanMaterialize        bool
	RequiresMaterialize   bool
	EstimatedPayloadBytes uint64
	OperationCount        uint32
	MutationCount         uint32
}

type SegmentBudget struct {
	MaxOperations   uint32
	MaxMutations    uint32
	MaxPayloadBytes uint64
}

type SegmentDecisionKind uint8

const (
	SegmentDecisionAppend SegmentDecisionKind = iota
	SegmentDecisionCut
	SegmentDecisionReject
	SegmentDecisionFlushBeforeAndAfter
)

type SegmentDecision struct {
	Kind   SegmentDecisionKind
	Reason SlowReason
}

func CompileDelta(delta SemanticDelta) CompiledOp {
	delta = cloneDelta(delta)
	durability := durabilityClass(delta)
	placement := placementPlan(delta, durability)
	footprint := keyFootprint(delta)
	predicates := predicateObligations(delta)
	guards := guardObligations(delta)
	effects := effectPlans(delta)
	digest := descriptorDigest(delta)
	atomicity := atomicityGroup(delta, digest)
	segment := segmentPlan(placement, footprint, uint32(len(effects)))
	return CompiledOp{
		Delta:            delta,
		DescriptorDigest: digest,
		IntentDigest:     digest,
		ReplayDigest:     digest,
		Authority: AuthorityPlan{
			Scope:    cloneScope(delta.Authority),
			Required: delta.Eligibility == EligibilityVisibleCommit,
			Fence:    fenceMode(delta),
		},
		Placement:  placement,
		Footprint:  footprint,
		Predicates: predicates,
		Guards:     guards,
		Effects:    effects,
		Atomicity:  atomicity,
		Durability: durability,
		Watch:      watchProjections(delta),
		Completion: completionPlan(delta, uint32(len(effects)), digest),
		Segment:    segment,
	}
}

func MaterializeDelta(delta SemanticDelta, proofs []PredicateProof) MaterializedOp {
	return MaterializeCompiledOp(CompileDelta(delta), nil, proofs)
}

func MaterializeCompiledOp(op CompiledOp, effects []WriteEffect, proofs []PredicateProof) MaterializedOp {
	return MaterializeCompiledOpWithGuardProofs(op, effects, proofs, nil)
}

func MaterializeCompiledOpWithGuardProofs(op CompiledOp, effects []WriteEffect, proofs []PredicateProof, guardProofs []GuardProof) MaterializedOp {
	delta := op.Delta
	if effects != nil {
		delta.WriteEffects = cloneEffects(effects)
	}
	delta.Authority = authorityScopeWithDeltaKeys(delta.Authority, delta)
	compiled := CompileDelta(delta)
	compiled.IntentDigest = op.IntentDigest
	compiled.ReplayDigest = compiled.DescriptorDigest
	return MaterializedOp{
		CompiledOp:      compiled,
		PredicateProofs: clonePredicateProofs(proofs),
		GuardProofs:     cloneGuardProofs(guardProofs),
	}
}

func authorityScopeWithDeltaKeys(scope AuthorityScope, delta SemanticDelta) AuthorityScope {
	out := cloneScope(scope)
	for _, predicate := range delta.ReadPredicates {
		out = authorityScopeWithKey(out, predicate.Key)
	}
	for _, effect := range delta.WriteEffects {
		out = authorityScopeWithKey(out, effect.Key)
	}
	return out
}

func authorityScopeWithKey(scope AuthorityScope, key []byte) AuthorityScope {
	parts, ok := fsmeta.InspectKey(key)
	if !ok {
		return scope
	}
	if scope.MountKeyID == 0 {
		scope.MountKeyID = parts.MountKeyID
	}
	scope.Buckets = uniqueBuckets(append(scope.Buckets, parts.Bucket))
	switch parts.Kind {
	case fsmeta.KeyKindDentry:
		scope.Parents = uniqueInodes(append(scope.Parents, parts.Parent))
	case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
		scope.Inodes = uniqueInodes(append(scope.Inodes, parts.Inode))
	case fsmeta.KeyKindUsage:
		scope.Parents = uniqueInodes(append(scope.Parents, fsmeta.InodeID(parts.UsageScope)))
	}
	return scope
}

func WithGuardProofs(op MaterializedOp, proofs []GuardProof) MaterializedOp {
	op.GuardProofs = cloneGuardProofs(proofs)
	return op
}

func CanAppendSegment(current, next CompiledOp, budget SegmentBudget) SegmentDecision {
	return CanAppendSegmentPlans(current.Segment, next.Segment, next.Durability, budget)
}

func CanAppendSegmentPlans(current, next SegmentPlan, nextDurability DurabilityClass, budget SegmentBudget) SegmentDecision {
	if nextDurability != DurabilityVisibleOnly {
		return SegmentDecision{Kind: SegmentDecisionFlushBeforeAndAfter, Reason: SlowReasonDurabilityBarrier}
	}
	if !current.CanAppend || !next.CanAppend {
		return SegmentDecision{Kind: SegmentDecisionReject, Reason: SlowReasonDynamicWriteSet}
	}
	if current.MergeKey != next.MergeKey {
		return SegmentDecision{Kind: SegmentDecisionCut, Reason: SlowReasonCrossBucket}
	}
	if budget.MaxOperations > 0 && current.OperationCount+next.OperationCount > budget.MaxOperations {
		return SegmentDecision{Kind: SegmentDecisionCut}
	}
	if budget.MaxMutations > 0 && current.MutationCount+next.MutationCount > budget.MaxMutations {
		return SegmentDecision{Kind: SegmentDecisionCut}
	}
	if budget.MaxPayloadBytes > 0 && current.EstimatedPayloadBytes+next.EstimatedPayloadBytes > budget.MaxPayloadBytes {
		return SegmentDecision{Kind: SegmentDecisionCut}
	}
	return SegmentDecision{Kind: SegmentDecisionAppend}
}

func MergeSegmentPlans(current, next SegmentPlan) SegmentPlan {
	out := current
	out.OperationCount += next.OperationCount
	out.MutationCount += next.MutationCount
	out.EstimatedPayloadBytes += next.EstimatedPayloadBytes
	return out
}

func SegmentPlanForInstall(plan SegmentPlan, materialize bool) (SegmentPlan, bool) {
	if !materialize {
		return plan, plan.CanAppend && plan.Install != SegmentInstallNone
	}
	if !plan.CanMaterialize || plan.MaterializeInstall == SegmentInstallNone {
		return SegmentPlan{}, false
	}
	out := plan
	out.Install = plan.MaterializeInstall
	out.MergeKey = plan.MaterializeMergeKey
	out.CanAppend = true
	return out, true
}

func fenceMode(delta SemanticDelta) FenceMode {
	if delta.Eligibility != EligibilityVisibleCommit {
		return FenceNone
	}
	return FenceActiveAuthority
}

func durabilityClass(delta SemanticDelta) DurabilityClass {
	if delta.Kind == fsmeta.OperationCloseSession {
		return DurabilityNeedsCloseSession
	}
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
	out.Install = SegmentInstallCatalog
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
			Kind:             predicate.Kind,
			Key:              cloneBytes(predicate.Key),
			HasExpectedValue: predicate.HasExpectedValue,
		}
		if predicate.HasExpectedValue {
			obligation.ExpectHash = sha256.Sum256(predicate.ExpectedValue)
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

func guardObligations(delta SemanticDelta) []GuardObligation {
	out := make([]GuardObligation, 0, len(delta.RuntimeGuards))
	for _, guard := range delta.RuntimeGuards {
		out = append(out, GuardObligation{
			Guard:  guard,
			Digest: GuardProofDigest(guard, true),
		})
	}
	return out
}

func effectPlans(delta SemanticDelta) []EffectPlan {
	out := make([]EffectPlan, 0, len(delta.WriteEffects))
	for i, effect := range delta.WriteEffects {
		plan := EffectPlan{
			ID:       MutationID(i),
			Kind:     effect.Kind,
			Key:      cloneBytes(effect.Key),
			Value:    cloneBytes(effect.Value),
			Concrete: effect.Kind == EffectPut || effect.Kind == EffectDelete,
		}
		if len(effect.Value) > 0 {
			plan.ValueHash = sha256.Sum256(effect.Value)
		}
		if parts, ok := fsmeta.InspectKey(effect.Key); ok {
			plan.MountKeyID = parts.MountKeyID
			plan.Bucket = parts.Bucket
			plan.RecordKind = parts.Kind
		} else if len(effect.Key) > 0 {
			plan.Opaque = true
		}
		switch effect.Kind {
		case EffectDerivedPut, EffectDerivedDelete:
			plan.Derivation = DerivationRuntimeValue
		}
		out = append(out, plan)
	}
	return out
}

func atomicityGroup(delta SemanticDelta, digest [32]byte) AtomicityGroup {
	group := AtomicityGroup{
		Members:  make([]MutationID, 0, len(delta.WriteEffects)),
		Recovery: RecoveryReplayAllOrNothing,
		Digest:   digest,
	}
	for i := range delta.WriteEffects {
		group.Members = append(group.Members, MutationID(i))
	}
	group.Splittable = len(group.Members) <= 1
	return group
}

func keyFootprint(delta SemanticDelta) KeyFootprint {
	out := KeyFootprint{
		Reads:        make([]KeyRef, 0, len(delta.ReadPredicates)),
		Writes:       make([]KeyRef, 0, len(delta.WriteEffects)),
		ConflictKeys: make([]KeyRef, 0, len(delta.ReadPredicates)+len(delta.WriteEffects)),
	}
	for _, predicate := range delta.ReadPredicates {
		mode := KeyAccessRead
		if predicate.Kind == PredicatePrefixScan {
			mode = KeyAccessReadPrefix
			out.HasPrefixRead = true
		}
		ref := keyRef(mode, predicate.Key)
		out.Reads = append(out.Reads, ref)
		out.ConflictKeys = append(out.ConflictKeys, ref)
		out.EstimatedBytes += uint64(len(predicate.Key) + len(predicate.ExpectedValue))
		if ref.Opaque {
			out.HasOpaqueKeys = true
		}
	}
	for _, effect := range delta.WriteEffects {
		ref := keyRef(KeyAccessWrite, effect.Key)
		out.Writes = append(out.Writes, ref)
		out.ConflictKeys = append(out.ConflictKeys, ref)
		out.EstimatedBytes += uint64(len(effect.Key) + len(effect.Value))
		if ref.Opaque {
			out.HasOpaqueKeys = true
		}
	}
	return out
}

func keyRef(mode KeyAccessMode, key []byte) KeyRef {
	out := KeyRef{
		Mode: mode,
		Key:  cloneBytes(key),
	}
	parts, ok := fsmeta.InspectKey(key)
	if !ok {
		out.Opaque = len(key) > 0
		return out
	}
	out.MountKeyID = parts.MountKeyID
	out.Bucket = parts.Bucket
	out.Kind = parts.Kind
	out.Parent = parts.Parent
	out.Inode = parts.Inode
	return out
}

func segmentPlan(placement PlacementPlan, footprint KeyFootprint, mutations uint32) SegmentPlan {
	plan := SegmentPlan{
		MergeKey:              placement.MergeKey,
		Install:               placement.Install,
		CanAppend:             placement.CanSegment,
		RequiresMaterialize:   placement.RequiresMaterialize,
		EstimatedPayloadBytes: footprint.EstimatedBytes,
		OperationCount:        1,
		MutationCount:         mutations,
	}
	switch {
	case placement.Install == SegmentInstallSingleBucket:
		plan.CanMaterialize = placement.CanSegment
		plan.MaterializeInstall = SegmentInstallSingleBucket
		plan.MaterializeMergeKey = placement.MergeKey
	case placement.Install == SegmentInstallCatalog && placement.SingleBucket && len(placement.Buckets) == 1:
		plan.CanMaterialize = placement.CanSegment
		plan.MaterializeInstall = SegmentInstallSingleBucket
		plan.MaterializeMergeKey = SegmentMergeKey{
			MountKeyID:    placement.MountKeyID,
			PrimaryBucket: placement.Buckets[0],
			Install:       SegmentInstallSingleBucket,
			Durability:    placement.MergeKey.Durability,
			FormatVersion: placement.MergeKey.FormatVersion,
		}
	}
	return plan
}

func completionPlan(delta SemanticDelta, mutations uint32, digest [32]byte) CompletionPlan {
	if delta.Eligibility != EligibilityVisibleCommit || mutations == 0 {
		return CompletionPlan{}
	}
	kind := CompletionVisible
	if durabilityClass(delta) != DurabilityVisibleOnly {
		kind = CompletionDurable
	}
	return CompletionPlan{
		RetainCompletion: true,
		Kind:             kind,
		MutationCount:    mutations,
		DescriptorDigest: digest,
	}
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
			Key:       cloneBytes(effect.Key),
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

func PredicateProofSetDigest(proofs []PredicateProof) [32]byte {
	if len(proofs) == 0 {
		return [32]byte{}
	}
	h := sha256.New()
	writeDigestUint64(h, uint64(len(proofs)))
	for _, proof := range proofs {
		writeDigestBytes(h, proof.Key)
		if proof.Present {
			writeDigestUint64(h, 1)
		} else {
			writeDigestUint64(h, 0)
		}
		writeDigestBytes(h, proof.Value)
		writeDigestUint64(h, proof.Version)
		writeDigestUint64(h, uint64(proof.Source))
		writeDigestBytes(h, proof.Digest[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func GuardProofDigest(guard RuntimeGuard, passed bool) [32]byte {
	h := sha256.New()
	writeDigestString(h, string(guard))
	if passed {
		writeDigestUint64(h, 1)
	} else {
		writeDigestUint64(h, 0)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func GuardProofsFor(guards []RuntimeGuard) []GuardProof {
	if len(guards) == 0 {
		return nil
	}
	out := make([]GuardProof, 0, len(guards))
	for _, guard := range guards {
		out = append(out, GuardProof{
			Guard:  guard,
			Passed: true,
			Digest: GuardProofDigest(guard, true),
		})
	}
	return out
}

func GuardProofSetDigest(proofs []GuardProof) [32]byte {
	if len(proofs) == 0 {
		return [32]byte{}
	}
	h := sha256.New()
	writeDigestUint64(h, uint64(len(proofs)))
	for _, proof := range proofs {
		writeDigestString(h, string(proof.Guard))
		if proof.Passed {
			writeDigestUint64(h, 1)
		} else {
			writeDigestUint64(h, 0)
		}
		writeDigestBytes(h, proof.Digest[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func AdmissionProofSetDigest(predicates []PredicateProof, guards []GuardProof) [32]byte {
	h := sha256.New()
	predicateDigest := PredicateProofSetDigest(predicates)
	guardDigest := GuardProofSetDigest(guards)
	writeDigestBytes(h, predicateDigest[:])
	writeDigestBytes(h, guardDigest[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func descriptorDigest(delta SemanticDelta) [32]byte {
	h := sha256.New()
	writeDigestString(h, string(delta.Kind))
	writeDigestString(h, delta.Eligibility.String())
	writeDigestString(h, string(delta.SlowReason))
	if delta.DurabilityBarrier {
		writeDigestUint64(h, 1)
	}
	if delta.WatchAtSeal {
		writeDigestUint64(h, 1)
	}
	writeDigestUint64(h, uint64(delta.Authority.MountKeyID))
	for _, bucket := range delta.Authority.Buckets {
		writeDigestUint64(h, uint64(bucket))
	}
	for _, predicate := range delta.ReadPredicates {
		writeDigestUint64(h, uint64(predicate.Kind))
		writeDigestBytes(h, predicate.Key)
		if predicate.HasExpectedValue {
			writeDigestBytes(h, predicate.ExpectedValue)
		}
	}
	for _, effect := range delta.WriteEffects {
		writeDigestUint64(h, uint64(effect.Kind))
		writeDigestBytes(h, effect.Key)
		writeDigestBytes(h, effect.Value)
	}
	for _, guard := range delta.RuntimeGuards {
		writeDigestString(h, string(guard))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func writeDigestString(h interface{ Write([]byte) (int, error) }, value string) {
	writeDigestBytes(h, []byte(value))
}

func writeDigestBytes(h interface{ Write([]byte) (int, error) }, value []byte) {
	writeDigestUint64(h, uint64(len(value)))
	h.Write(value)
}

func writeDigestUint64(h interface{ Write([]byte) (int, error) }, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	h.Write(buf[:])
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

func clonePredicateProofs(proofs []PredicateProof) []PredicateProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]PredicateProof, len(proofs))
	for i, proof := range proofs {
		out[i] = PredicateProof{
			Key:     cloneBytes(proof.Key),
			Present: proof.Present,
			Value:   cloneBytes(proof.Value),
			Version: proof.Version,
			Source:  proof.Source,
			Digest:  proof.Digest,
		}
	}
	return out
}

func cloneGuardProofs(proofs []GuardProof) []GuardProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]GuardProof, len(proofs))
	copy(out, proofs)
	return out
}
