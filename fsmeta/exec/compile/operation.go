package compile

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"unsafe"

	"github.com/feichai0017/NoKV/fsmeta"
)

const segmentFormatVersion uint16 = 1

// CompiledOp is the segment-installable semantic descriptor for one metadata
// operation. SemanticDelta is the executor-facing mutation contract; this
// descriptor is the compiler boundary Peras needs for admission, segment
// packing, recovery, completion, and watch projection.
type CompiledOp struct {
	Delta SemanticDelta
	// DescriptorDigest is the canonical digest of this exact generated
	// descriptor, including any materialized observed values and effects.
	DescriptorDigest [32]byte
	// IntentDigest stays pinned to the original request intent across
	// materialization so completion/watch code can identify the user's op.
	IntentDigest [32]byte
	// ReplayDigest is the digest that segment replay must install. After
	// materialization it must equal DescriptorDigest.
	ReplayDigest [32]byte
	Authority    AuthorityPlan
	Placement    PlacementPlan
	Footprint    KeyFootprint
	Predicates   []PredicateObligation
	Guards       []GuardObligation
	Effects      []EffectPlan
	Atomicity    AtomicityGroup
	Durability   DurabilityClass
	Watch        []WatchProjection
	Completion   CompletionPlan
	Segment      SegmentPlan
}

// MaterializedOp is the closed Peras IR admitted by the holder. It has the
// static generated descriptor plus any runtime predicate proofs and concrete
// effects needed to install the operation inside a segment.
type MaterializedOp struct {
	CompiledOp
	PredicateProofs []PredicateProof
	GuardProofs     []GuardProof
}

// PredicateEvidence carries runtime reads that turn a generated program with
// symbolic predicates into the proof-carrying descriptor admitted by Peras.
type PredicateEvidence struct {
	Proofs []PredicateProof
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

type GuardEvidence struct {
	DescriptorDigest     [32]byte
	PredicateProofDigest [32]byte
	FootprintDigest      [32]byte
}

type GuardProof struct {
	Guard    RuntimeGuard
	Passed   bool
	Evidence GuardEvidence
	Digest   [32]byte
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

// InstallPlan is the segment-install command header produced after a compiled
// replay plan has been sealed into a concrete Peras segment. Routing and apply
// scheduling consume this metadata without decoding the segment payload.
type InstallPlan struct {
	Mode               SegmentInstallMode
	Materialize        bool
	RoutingKeys        [][]byte
	DependencyKeys     [][]byte
	CatalogKeys        [][]byte
	MaterializedKeys   [][]byte
	CanonicalObjectKey []byte
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

// MaterializeCompiledOpWithEvidence recompiles a generated descriptor with
// concrete runtime effects and predicate evidence, without runtime semantic
// lowering.
func MaterializeCompiledOpWithEvidence(op CompiledOp, effects []WriteEffect, evidence PredicateEvidence, guardProofs []GuardProof) (MaterializedOp, error) {
	delta := op.Delta
	if effects != nil {
		delta.WriteEffects = cloneEffects(effects)
	}
	var err error
	delta, err = applyPredicateEvidence(delta, evidence)
	if err != nil {
		return MaterializedOp{}, err
	}
	delta.Authority = authorityScopeWithDeltaKeys(delta.Authority, delta)
	compiled, err := compileAOTDelta(delta)
	if err != nil {
		return MaterializedOp{}, err
	}
	compiled.IntentDigest = op.IntentDigest
	compiled.ReplayDigest = compiled.DescriptorDigest
	return MaterializedOp{
		CompiledOp:      compiled,
		PredicateProofs: clonePredicateProofs(evidence.Proofs),
		GuardProofs:     cloneGuardProofs(guardProofs),
	}, nil
}

func applyPredicateEvidence(delta SemanticDelta, evidence PredicateEvidence) (SemanticDelta, error) {
	delta = cloneDelta(delta)
	proofs, err := predicateProofMap(evidence.Proofs)
	if err != nil {
		return SemanticDelta{}, err
	}
	seen := make(map[string]struct{}, len(delta.ReadPredicates)+len(proofs))
	for i := range delta.ReadPredicates {
		predicate := &delta.ReadPredicates[i]
		if predicate.Kind == PredicatePrefixScan {
			continue
		}
		if len(predicate.Key) == 0 {
			return SemanticDelta{}, fsmeta.ErrInvalidRequest
		}
		key := string(predicate.Key)
		seen[key] = struct{}{}
		if proof, ok := proofs[key]; ok {
			applyPredicateProof(predicate, proof)
			continue
		}
	}
	extraKeys := make([]string, 0, len(proofs))
	for key := range proofs {
		if _, ok := seen[key]; !ok {
			extraKeys = append(extraKeys, key)
		}
	}
	slices.Sort(extraKeys)
	for _, key := range extraKeys {
		proof := proofs[key]
		predicate := Predicate{Key: cloneBytes(proof.Key)}
		applyPredicateProof(&predicate, proof)
		delta.ReadPredicates = append(delta.ReadPredicates, predicate)
	}
	return delta, nil
}

func applyPredicateProof(predicate *Predicate, proof PredicateProof) {
	if !proof.Present {
		predicate.Kind = PredicateNotExists
		predicate.ExpectedValue = nil
		predicate.HasExpectedValue = false
		predicate.RuntimeChecked = true
		return
	}
	predicate.Kind = PredicateObservedValue
	predicate.ExpectedValue = cloneBytes(proof.Value)
	predicate.HasExpectedValue = true
	predicate.RuntimeChecked = true
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

func WithPredicateProofs(op MaterializedOp, proofs []PredicateProof) MaterializedOp {
	op.PredicateProofs = clonePredicateProofs(proofs)
	return op
}

func WithAdmissionProofs(op MaterializedOp, predicateProofs []PredicateProof, guardProofs []GuardProof) MaterializedOp {
	op.PredicateProofs = clonePredicateProofs(predicateProofs)
	op.GuardProofs = cloneGuardProofs(guardProofs)
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

func keyRef(mode KeyAccessMode, key []byte) KeyRef {
	out := KeyRef{
		Mode: mode,
		Key:  key,
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
	name, ok := fsmeta.DentryNameBytesOfKey(key)
	if !ok || len(name) == 0 {
		return ""
	}
	return unsafe.String(&name[0], len(name))
}

func PredicateProofDigest(key, value []byte, present bool, version uint64, source ReadSource) [32]byte {
	h := newDigestBuilder()
	h.writeRaw(key)
	h.writeBoolByte(present)
	h.writeRaw(value)
	h.writeByte(byte(source))
	h.writeUint64(version)
	return h.sum()
}

func PredicateProofFor(key, value []byte, present bool, version uint64, source ReadSource) PredicateProof {
	proof := PredicateProof{
		Key:     cloneBytes(key),
		Present: present,
		Value:   cloneBytes(value),
		Version: version,
		Source:  source,
	}
	proof.Digest = PredicateProofDigest(proof.Key, proof.Value, proof.Present, proof.Version, proof.Source)
	return proof
}

func PredicateProofSetDigest(proofs []PredicateProof) [32]byte {
	if len(proofs) == 0 {
		return [32]byte{}
	}
	h := newDigestBuilder()
	h.writeUint64(uint64(len(proofs)))
	for _, proof := range proofs {
		h.writeBytes(proof.Key)
		h.writeBool(proof.Present)
		h.writeBytes(proof.Value)
		h.writeUint64(proof.Version)
		h.writeUint64(uint64(proof.Source))
		h.writeBytes(proof.Digest[:])
	}
	return h.sum()
}

func GuardObligationDigest(guard RuntimeGuard) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(guard))
	return h.sum()
}

func GuardEvidenceFor(op CompiledOp, predicateProofs []PredicateProof) GuardEvidence {
	return GuardEvidence{
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: PredicateProofSetDigest(predicateProofs),
		FootprintDigest:      KeyFootprintDigest(op.Footprint),
	}
}

func KeyFootprintDigest(footprint KeyFootprint) [32]byte {
	h := newDigestBuilder()
	h.writeUint64(uint64(len(footprint.Reads)))
	for _, ref := range footprint.Reads {
		h.writeKeyRef(ref)
	}
	h.writeUint64(uint64(len(footprint.Writes)))
	for _, ref := range footprint.Writes {
		h.writeKeyRef(ref)
	}
	h.writeUint64(uint64(len(footprint.ConflictKeys)))
	for _, ref := range footprint.ConflictKeys {
		h.writeKeyRef(ref)
	}
	h.writeBool(footprint.HasPrefixRead)
	h.writeBool(footprint.HasOpaqueKeys)
	h.writeUint64(footprint.EstimatedBytes)
	return h.sum()
}

// GuardProofDigest commits to the generated guard identity, boolean result,
// and the predicate/effect descriptor evidence the holder used for admission.
func GuardProofDigest(guard RuntimeGuard, passed bool, evidence GuardEvidence) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(guard))
	h.writeBool(passed)
	h.writeBytes(evidence.DescriptorDigest[:])
	h.writeBytes(evidence.PredicateProofDigest[:])
	h.writeBytes(evidence.FootprintDigest[:])
	return h.sum()
}

func GuardProofFor(guard RuntimeGuard, passed bool, evidence GuardEvidence) GuardProof {
	return GuardProof{
		Guard:    guard,
		Passed:   passed,
		Evidence: evidence,
		Digest:   GuardProofDigest(guard, passed, evidence),
	}
}

func GuardProofsFor(guards []RuntimeGuard, evidence GuardEvidence) []GuardProof {
	if len(guards) == 0 {
		return nil
	}
	out := make([]GuardProof, 0, len(guards))
	for _, guard := range guards {
		out = append(out, GuardProofFor(guard, true, evidence))
	}
	return out
}

func ExecutionPlanDigest(segment SegmentPlan, atomicity AtomicityGroup, durability DurabilityClass) [32]byte {
	h := newDigestBuilder()
	h.writeSegmentMergeKey(segment.MergeKey)
	h.writeUint64(uint64(segment.Install))
	h.writeSegmentMergeKey(segment.MaterializeMergeKey)
	h.writeUint64(uint64(segment.MaterializeInstall))
	h.writeBool(segment.CanAppend)
	h.writeBool(segment.CanMaterialize)
	h.writeBool(segment.RequiresMaterialize)
	h.writeUint64(segment.EstimatedPayloadBytes)
	h.writeUint64(uint64(segment.OperationCount))
	h.writeUint64(uint64(segment.MutationCount))
	h.writeUint64(uint64(atomicity.Recovery))
	h.writeBool(atomicity.Splittable)
	h.writeBytes(atomicity.Digest[:])
	h.writeUint64(uint64(len(atomicity.Members)))
	for _, member := range atomicity.Members {
		h.writeUint64(uint64(member))
	}
	h.writeUint64(uint64(durability))
	return h.sum()
}

func GuardProofSetDigest(proofs []GuardProof) [32]byte {
	if len(proofs) == 0 {
		return [32]byte{}
	}
	h := newDigestBuilder()
	h.writeUint64(uint64(len(proofs)))
	for _, proof := range proofs {
		h.writeString(string(proof.Guard))
		h.writeBool(proof.Passed)
		h.writeBytes(proof.Evidence.DescriptorDigest[:])
		h.writeBytes(proof.Evidence.PredicateProofDigest[:])
		h.writeBytes(proof.Evidence.FootprintDigest[:])
		h.writeBytes(proof.Digest[:])
	}
	return h.sum()
}

func AdmissionProofSetDigest(predicates []PredicateProof, guards []GuardProof) [32]byte {
	h := newDigestBuilder()
	predicateDigest := PredicateProofSetDigest(predicates)
	guardDigest := GuardProofSetDigest(guards)
	h.writeBytes(predicateDigest[:])
	h.writeBytes(guardDigest[:])
	return h.sum()
}

func descriptorDigest(delta SemanticDelta) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(delta.Kind))
	h.writeString(delta.Eligibility.String())
	h.writeString(string(delta.SlowReason))
	if delta.DurabilityBarrier {
		h.writeUint64(1)
	}
	if delta.WatchAtSeal {
		h.writeUint64(1)
	}
	h.writeUint64(uint64(delta.Authority.MountKeyID))
	for _, bucket := range delta.Authority.Buckets {
		h.writeUint64(uint64(bucket))
	}
	for _, predicate := range delta.ReadPredicates {
		h.writeUint64(uint64(predicate.Kind))
		h.writeBytes(predicate.Key)
		if predicate.HasExpectedValue {
			h.writeBytes(predicate.ExpectedValue)
		}
	}
	for _, effect := range delta.WriteEffects {
		h.writeUint64(uint64(effect.Kind))
		h.writeBytes(effect.Key)
		h.writeBytes(effect.Value)
	}
	for _, guard := range delta.RuntimeGuards {
		h.writeString(string(guard))
	}
	return h.sum()
}

type digestBuilder struct {
	stack [512]byte
	off   int
	heap  []byte
}

func newDigestBuilder() digestBuilder {
	return digestBuilder{}
}

func (b *digestBuilder) writeSegmentMergeKey(key SegmentMergeKey) {
	b.writeUint64(uint64(key.MountKeyID))
	b.writeUint64(uint64(key.PrimaryBucket))
	b.writeUint64(uint64(key.Install))
	b.writeUint64(uint64(key.Durability))
	b.writeUint64(uint64(key.FormatVersion))
}

func (b *digestBuilder) writeKeyRef(ref KeyRef) {
	b.writeUint64(uint64(ref.Mode))
	b.writeBytes(ref.Key)
	b.writeBool(ref.Opaque)
	b.writeUint64(uint64(ref.MountKeyID))
	b.writeUint64(uint64(ref.Bucket))
	b.writeUint64(uint64(ref.Kind))
	b.writeUint64(uint64(ref.Parent))
	b.writeUint64(uint64(ref.Inode))
}

func (b *digestBuilder) writeString(value string) {
	b.writeUint64(uint64(len(value)))
	b.writeRawString(value)
}

func (b *digestBuilder) writeBytes(value []byte) {
	b.writeUint64(uint64(len(value)))
	b.writeRaw(value)
}

func (b *digestBuilder) writeRaw(value []byte) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		copy(b.stack[b.off:], value)
		b.off += len(value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) writeUint64(value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	b.writeRaw(buf[:])
}

func (b *digestBuilder) writeBool(value bool) {
	if value {
		b.writeUint64(1)
		return
	}
	b.writeUint64(0)
}

func (b *digestBuilder) writeBoolByte(value bool) {
	if value {
		b.writeByte(1)
		return
	}
	b.writeByte(0)
}

func (b *digestBuilder) writeByte(value byte) {
	if b.heap != nil {
		b.heap = append(b.heap, value)
		return
	}
	if b.off < len(b.stack) {
		b.stack[b.off] = value
		b.off++
		return
	}
	b.spill(1)
	b.heap = append(b.heap, value)
}

func (b *digestBuilder) sum() [32]byte {
	if b.heap != nil {
		return sha256.Sum256(b.heap)
	}
	return sha256.Sum256(b.stack[:b.off])
}

func (b *digestBuilder) writeRawString(value string) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		b.off += copy(b.stack[b.off:], value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) spill(extra int) {
	needed := b.off + extra
	capacity := len(b.stack) * 2
	if needed > capacity {
		capacity = needed
	}
	b.heap = make([]byte, b.off, capacity)
	copy(b.heap, b.stack[:b.off])
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
