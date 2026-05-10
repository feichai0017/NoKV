// Package compile turns fsmeta request plans into Capsule semantic deltas.
//
// The compiler is deliberately conservative. It describes the static key
// footprint and the guards a future Capsule holder must prove before it may
// bypass the ordinary Percolator/Raft path. It does not execute reads, does not
// allocate timestamps, and does not weaken the current fsmeta executor.
package compile

import (
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
)

// Eligibility describes whether a request can enter the Capsule write path
// after the listed runtime guards have been checked by the holder.
type Eligibility uint8

const (
	EligibilityFastPath Eligibility = iota
	EligibilitySlowPath
)

func (e Eligibility) String() string {
	switch e {
	case EligibilityFastPath:
		return "fast_path"
	case EligibilitySlowPath:
		return "slow_path"
	default:
		return "unknown"
	}
}

// SlowReason records why a request must remain on the current Percolator/Raft
// path.
type SlowReason string

const (
	SlowReasonNone              SlowReason = ""
	SlowReasonReadOnly          SlowReason = "read_only"
	SlowReasonRangeRead         SlowReason = "range_read"
	SlowReasonDurabilityBarrier SlowReason = "durability_barrier"
	SlowReasonCrossParent       SlowReason = "cross_parent"
	SlowReasonSharedQuota       SlowReason = "shared_quota"
	SlowReasonDynamicWriteSet   SlowReason = "dynamic_write_set"
	SlowReasonMaintenanceScan   SlowReason = "maintenance_scan"
)

// RuntimeGuard is a condition the future Capsule holder must verify against
// its merged holder view before it can issue a certificate.
type RuntimeGuard string

const (
	GuardSingleLinkInode     RuntimeGuard = "single_link_inode"
	GuardSameAuthority       RuntimeGuard = "same_authority"
	GuardNonDirectoryInode   RuntimeGuard = "non_directory_inode"
	GuardNotLastReference    RuntimeGuard = "not_last_reference"
	GuardLiveSession         RuntimeGuard = "live_session"
	GuardExpiredSessionOwner RuntimeGuard = "expired_session_owner"
	GuardQuotaCredit         RuntimeGuard = "quota_credit"
)

// PredicateKind is the static predicate class carried by a semantic delta.
type PredicateKind uint8

const (
	PredicateExists PredicateKind = iota
	PredicateNotExists
	PredicateObservedValue
	PredicatePrefixScan
)

// Predicate describes one key or prefix condition. ObservedValue means the
// holder must read the current value and later prove it did not change before
// certificate issue; the concrete value is intentionally not embedded here.
type Predicate struct {
	Kind PredicateKind
	Key  []byte
}

// EffectKind is the mutation class a Capsule certificate would eventually
// replay.
type EffectKind uint8

const (
	EffectPut EffectKind = iota
	EffectDelete
	EffectDerivedPut
	EffectDerivedDelete
)

// WriteEffect describes a static write target. Value is present only when the
// request itself fully determines the bytes, such as Create.
type WriteEffect struct {
	Kind  EffectKind
	Key   []byte
	Value []byte
}

// AuthorityScope is the mount-local scope a holder grant must cover. It is a
// runtime contract, not persisted root truth.
type AuthorityScope struct {
	Mount      fsmeta.MountID
	MountKeyID fsmeta.MountKeyID
	Buckets    []fsmeta.AffinityBucket
	Parents    []fsmeta.InodeID
	Inodes     []fsmeta.InodeID
}

// SemanticDelta is the Capsule-facing contract produced from one fsmeta
// request. The existing executor still uses fsmeta.OperationPlan directly.
type SemanticDelta struct {
	Kind              fsmeta.OperationKind
	Plan              fsmeta.OperationPlan
	Authority         AuthorityScope
	ReadPredicates    []Predicate
	WriteEffects      []WriteEffect
	RuntimeGuards     []RuntimeGuard
	Eligibility       Eligibility
	SlowReason        SlowReason
	DurabilityBarrier bool
	WatchAtSeal       bool
}

type QuotaMode uint8

const (
	QuotaModeNone QuotaMode = iota
	QuotaModeEscrow
	QuotaModeShared
)

type Options struct {
	QuotaMode QuotaMode
}

type Option func(*Options)

func WithQuotaMode(mode QuotaMode) Option {
	return func(opts *Options) {
		opts.QuotaMode = mode
	}
}

func Create(req fsmeta.CreateRequest, mount fsmeta.MountIdentity, inodeID fsmeta.InodeID, opts ...Option) (SemanticDelta, error) {
	plan, err := fsmeta.PlanCreate(req, mount, inodeID)
	if err != nil {
		return SemanticDelta{}, err
	}
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := fsmeta.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inodeID, Type: inode.Type}
	dentryValue, err := fsmeta.EncodeDentryValue(dentry)
	if err != nil {
		return SemanticDelta{}, err
	}
	inodeValue, err := fsmeta.EncodeInodeValue(inode)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := mutationDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.Parent}, []fsmeta.InodeID{inodeID}),
		[]Predicate{
			{Kind: PredicateNotExists, Key: plan.MutateKeys[0]},
			{Kind: PredicateNotExists, Key: plan.MutateKeys[1]},
		},
		[]WriteEffect{
			{Kind: EffectPut, Key: plan.MutateKeys[0], Value: dentryValue},
			{Kind: EffectPut, Key: plan.MutateKeys[1], Value: inodeValue},
		},
	)
	return applyQuotaPolicy(delta, collectOptions(opts...), GuardQuotaCredit), nil
}

func UpdateInode(req fsmeta.UpdateInodeRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	plan, err := fsmeta.PlanUpdateInode(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	if !req.SetSize && !req.SetMode && !req.SetUpdatedUnixNs && !req.SetOpaqueAttrs {
		return SemanticDelta{}, fsmeta.ErrInvalidRequest
	}
	delta := mutationDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.Parent}, []fsmeta.InodeID{req.Inode}),
		[]Predicate{
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[0]},
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[1]},
		},
		[]WriteEffect{{Kind: EffectDerivedPut, Key: plan.MutateKeys[0]}},
	)
	delta.RuntimeGuards = append(delta.RuntimeGuards, GuardSingleLinkInode)
	if req.SetSize {
		return applyQuotaPolicy(delta, collectOptions(opts...), GuardQuotaCredit), nil
	}
	return delta, nil
}

func Lookup(req fsmeta.LookupRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanLookup(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return readDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.Parent}, nil), []Predicate{{Kind: PredicateExists, Key: plan.PrimaryKey}}, SlowReasonReadOnly), nil
}

func ReadDir(req fsmeta.ReadDirRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanReadDir(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return readDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.Parent}, nil), []Predicate{{Kind: PredicatePrefixScan, Key: plan.ReadPrefixes[0]}}, SlowReasonRangeRead), nil
}

func SnapshotSubtree(req fsmeta.SnapshotSubtreeRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanSnapshotSubtree(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := readDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.RootInode}, nil), []Predicate{{Kind: PredicatePrefixScan, Key: plan.ReadPrefixes[0]}}, SlowReasonDurabilityBarrier)
	delta.DurabilityBarrier = true
	return delta, nil
}

func Rename(req fsmeta.RenameRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanRename(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := mutationDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.FromParent, req.ToParent}, nil),
		[]Predicate{
			{Kind: PredicateExists, Key: plan.ReadKeys[0]},
			{Kind: PredicateNotExists, Key: plan.ReadKeys[1]},
		},
		[]WriteEffect{
			{Kind: EffectDelete, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[1]},
		},
	)
	if req.FromParent != req.ToParent {
		delta.Eligibility = EligibilitySlowPath
		delta.SlowReason = SlowReasonCrossParent
	}
	return delta, nil
}

func RenameSubtree(req fsmeta.RenameSubtreeRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanRenameSubtree(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := mutationDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.FromParent, req.ToParent}, nil),
		[]Predicate{
			{Kind: PredicateExists, Key: plan.ReadKeys[0]},
			{Kind: PredicateNotExists, Key: plan.ReadKeys[1]},
		},
		[]WriteEffect{
			{Kind: EffectDelete, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[1]},
		},
	)
	delta.Eligibility = EligibilitySlowPath
	delta.SlowReason = SlowReasonDurabilityBarrier
	delta.DurabilityBarrier = true
	delta.WatchAtSeal = true
	return delta, nil
}

func Link(req fsmeta.LinkRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	plan, err := fsmeta.PlanLink(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := mutationDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.FromParent, req.ToParent}, nil),
		[]Predicate{
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[0]},
			{Kind: PredicateNotExists, Key: plan.ReadKeys[1]},
		},
		[]WriteEffect{
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedPut},
		},
	)
	delta.RuntimeGuards = append(delta.RuntimeGuards, GuardNonDirectoryInode, GuardSameAuthority)
	delta = applyQuotaPolicy(delta, collectOptions(opts...), GuardQuotaCredit)
	if delta.Eligibility == EligibilityFastPath {
		delta.Eligibility = EligibilitySlowPath
		delta.SlowReason = SlowReasonDynamicWriteSet
	}
	return delta, nil
}

func Unlink(req fsmeta.UnlinkRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	plan, err := fsmeta.PlanUnlink(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := mutationDelta(plan, scopeFor(mount, []fsmeta.InodeID{req.Parent}, nil),
		[]Predicate{{Kind: PredicateObservedValue, Key: plan.ReadKeys[0]}},
		[]WriteEffect{
			{Kind: EffectDelete, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedPut},
		},
	)
	delta.RuntimeGuards = append(delta.RuntimeGuards, GuardNotLastReference)
	delta = applyQuotaPolicy(delta, collectOptions(opts...), GuardQuotaCredit)
	if delta.Eligibility == EligibilityFastPath {
		delta.Eligibility = EligibilitySlowPath
		delta.SlowReason = SlowReasonDynamicWriteSet
	}
	return delta, nil
}

func OpenWriteSession(req fsmeta.OpenWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanOpenWriteSession(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	if req.TTL <= 0 {
		return SemanticDelta{}, fsmeta.ErrInvalidRequest
	}
	delta := mutationDelta(plan, scopeFor(mount, nil, []fsmeta.InodeID{req.Inode}),
		[]Predicate{
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[0]},
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[1]},
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[2]},
		},
		[]WriteEffect{
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[1]},
		},
	)
	delta.RuntimeGuards = append(delta.RuntimeGuards, GuardNonDirectoryInode, GuardExpiredSessionOwner)
	return delta, nil
}

func HeartbeatWriteSession(req fsmeta.HeartbeatWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanHeartbeatWriteSession(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	if req.TTL <= 0 {
		return SemanticDelta{}, fsmeta.ErrInvalidRequest
	}
	delta := mutationDelta(plan, scopeFor(mount, nil, []fsmeta.InodeID{req.Inode}),
		[]Predicate{
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[0]},
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[1]},
		},
		[]WriteEffect{
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedPut, Key: plan.MutateKeys[1]},
		},
	)
	delta.RuntimeGuards = append(delta.RuntimeGuards, GuardLiveSession)
	return delta, nil
}

func CloseWriteSession(req fsmeta.CloseWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanCloseWriteSession(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, req.Inode)
	if err != nil {
		return SemanticDelta{}, err
	}
	delta := mutationDelta(plan, scopeFor(mount, nil, []fsmeta.InodeID{req.Inode}),
		[]Predicate{
			{Kind: PredicateObservedValue, Key: plan.ReadKeys[0]},
			{Kind: PredicateObservedValue, Key: ownerKey},
		},
		[]WriteEffect{
			{Kind: EffectDelete, Key: plan.MutateKeys[0]},
			{Kind: EffectDerivedDelete, Key: ownerKey},
		},
	)
	delta.RuntimeGuards = append(delta.RuntimeGuards, GuardLiveSession)
	return delta, nil
}

func ExpireWriteSessions(req fsmeta.ExpireWriteSessionsRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	plan, err := fsmeta.PlanExpireWriteSessions(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	predicates := make([]Predicate, 0, len(plan.ReadPrefixes))
	for _, prefix := range plan.ReadPrefixes {
		predicates = append(predicates, Predicate{Kind: PredicatePrefixScan, Key: prefix})
	}
	return readDelta(plan, scopeFor(mount, nil, nil), predicates, SlowReasonMaintenanceScan), nil
}

func mutationDelta(plan fsmeta.OperationPlan, scope AuthorityScope, predicates []Predicate, effects []WriteEffect) SemanticDelta {
	return SemanticDelta{
		Kind:           plan.Kind,
		Plan:           clonePlan(plan),
		Authority:      cloneScope(scope),
		ReadPredicates: clonePredicates(predicates),
		WriteEffects:   cloneEffects(effects),
		Eligibility:    EligibilityFastPath,
	}
}

func readDelta(plan fsmeta.OperationPlan, scope AuthorityScope, predicates []Predicate, reason SlowReason) SemanticDelta {
	delta := mutationDelta(plan, scope, predicates, nil)
	delta.Eligibility = EligibilitySlowPath
	delta.SlowReason = reason
	return delta
}

func applyQuotaPolicy(delta SemanticDelta, opts Options, guard RuntimeGuard) SemanticDelta {
	switch opts.QuotaMode {
	case QuotaModeShared:
		delta.Eligibility = EligibilitySlowPath
		delta.SlowReason = SlowReasonSharedQuota
	case QuotaModeEscrow:
		delta.RuntimeGuards = append(delta.RuntimeGuards, guard)
	}
	return delta
}

func collectOptions(opts ...Option) Options {
	var out Options
	for _, opt := range opts {
		if opt != nil {
			opt(&out)
		}
	}
	return out
}

func scopeFor(mount fsmeta.MountIdentity, parents, inodes []fsmeta.InodeID) AuthorityScope {
	scope := AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Parents:    uniqueInodes(parents),
		Inodes:     uniqueInodes(inodes),
	}
	buckets := make([]fsmeta.AffinityBucket, 0, len(scope.Parents)+len(scope.Inodes))
	for _, parent := range scope.Parents {
		buckets = append(buckets, fsmeta.BucketForInodeID(parent))
	}
	for _, inode := range scope.Inodes {
		buckets = append(buckets, fsmeta.BucketForInodeID(inode))
	}
	scope.Buckets = uniqueBuckets(buckets)
	return scope
}

func uniqueInodes(in []fsmeta.InodeID) []fsmeta.InodeID {
	out := make([]fsmeta.InodeID, 0, len(in))
	seen := make(map[fsmeta.InodeID]struct{}, len(in))
	for _, id := range in {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func uniqueBuckets(in []fsmeta.AffinityBucket) []fsmeta.AffinityBucket {
	out := make([]fsmeta.AffinityBucket, 0, len(in))
	seen := make(map[fsmeta.AffinityBucket]struct{}, len(in))
	for _, bucket := range in {
		if _, ok := seen[bucket]; ok {
			continue
		}
		seen[bucket] = struct{}{}
		out = append(out, bucket)
	}
	slices.Sort(out)
	return out
}

func clonePlan(plan fsmeta.OperationPlan) fsmeta.OperationPlan {
	return fsmeta.OperationPlan{
		Kind:         plan.Kind,
		Mount:        plan.Mount,
		PrimaryKey:   cloneBytes(plan.PrimaryKey),
		StartKey:     cloneBytes(plan.StartKey),
		Limit:        plan.Limit,
		ReadKeys:     cloneKeySet(plan.ReadKeys),
		ReadPrefixes: cloneKeySet(plan.ReadPrefixes),
		MutateKeys:   cloneKeySet(plan.MutateKeys),
	}
}

func cloneScope(scope AuthorityScope) AuthorityScope {
	return AuthorityScope{
		Mount:      scope.Mount,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]fsmeta.AffinityBucket(nil), scope.Buckets...),
		Parents:    append([]fsmeta.InodeID(nil), scope.Parents...),
		Inodes:     append([]fsmeta.InodeID(nil), scope.Inodes...),
	}
}

func clonePredicates(in []Predicate) []Predicate {
	out := make([]Predicate, 0, len(in))
	for _, predicate := range in {
		out = append(out, Predicate{
			Kind: predicate.Kind,
			Key:  cloneBytes(predicate.Key),
		})
	}
	return out
}

func cloneEffects(in []WriteEffect) []WriteEffect {
	out := make([]WriteEffect, 0, len(in))
	for _, effect := range in {
		out = append(out, WriteEffect{
			Kind:  effect.Kind,
			Key:   cloneBytes(effect.Key),
			Value: cloneBytes(effect.Value),
		})
	}
	return out
}

func cloneKeySet(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, key := range in {
		out = append(out, cloneBytes(key))
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
