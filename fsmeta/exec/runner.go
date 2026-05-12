package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const (
	// Percolator TTLs are encoded in milliseconds. fsmeta mutations cross the
	// coordinator TSO path plus raft apply queues, so the default must cover
	// short commit stalls instead of letting read-side lock resolution roll
	// back a live metadata transaction.
	defaultLockTTL uint64 = uint64(30 * time.Second / time.Millisecond)

	// Non-lock conflicts are retried by count because fresh timestamps normally
	// make progress immediately. Live locks are bounded separately by the lock
	// TTL so fsmeta does not leak ordinary Percolator lock waits to callers.
	maxTxnContentionRetries  = 32
	maxReadContentionRetries = 32

	txnContentionRetryBaseBackoff = time.Millisecond
	txnContentionRetryMaxBackoff  = 100 * time.Millisecond
	maxTxnLockRetryBudget         = time.Hour
)

// KV is the minimal key/value tuple the fsmeta executor consumes from scans.
type KV struct {
	Key   []byte
	Value []byte
}

// TxnRunner is the NoKV transaction surface required by fsmeta execution.
//
// ReserveTimestamp returns the first timestamp in a consecutive range of count
// timestamps. Mutate must provide Percolator-style atomicity for all mutations
// and return the commit timestamp that made the mutation visible. MutateAtCommit
// is reserved for operations whose commit timestamp is already part of an
// external authority protocol, so the runner must not allocate a later commit_ts.
type TxnRunner interface {
	ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
	Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
	Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error)
	MutateAtCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error)
}

// AtomicMutateOnePhase is an optional TxnRunner extension. handled=false
// means the runner could not keep the mutation in one proven-atomic local
// apply group and the caller must fall back to Mutate.
type AtomicMutateOnePhase interface {
	TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (handled bool, err error)
}

// InodeAllocator assigns Create inode IDs. The executor allocates once before
// transaction retry so a retry cannot publish a different inode for the same
// logical Create after a conflict or ambiguous transport error.
type InodeAllocator interface {
	AllocateCreateInode(ctx context.Context, mount fsmeta.MountIdentity, parent fsmeta.InodeID, name string) (fsmeta.InodeID, error)
}

// statsProvider is implemented by lower fsmeta runtime layers that can expose
// their own counters without becoming part of the transaction execution API.
type statsProvider interface {
	Stats() map[string]any
}

// MountAdmission is the executor's mount-admission view.
type MountAdmission struct {
	MountID       fsmeta.MountID
	MountKeyID    fsmeta.MountKeyID
	RootInode     fsmeta.InodeID
	SchemaVersion uint32
	Retired       bool
}

func (m MountAdmission) Identity() fsmeta.MountIdentity {
	return fsmeta.MountIdentity{MountID: m.MountID, MountKeyID: m.MountKeyID}
}

// MountResolver checks rooted mount lifecycle before mutating fsmeta data.
type MountResolver interface {
	ResolveMount(context.Context, fsmeta.MountID) (MountAdmission, error)
}

// SubtreeHandoffPublisher publishes rooted subtree authority handoff events for
// successful authority-aware namespace mutations.
type SubtreeHandoffPublisher interface {
	StartSubtreeHandoff(context.Context, fsmeta.MountID, fsmeta.InodeID, uint64) error
	CompleteSubtreeHandoff(context.Context, fsmeta.MountID, fsmeta.InodeID, uint64) error
}

// SubtreeAuthorityResolver decides whether an ordinary data-plane rename stays
// inside one rooted authority. Cross-authority moves must use RenameSubtree so
// root can advance authority eras explicitly.
type SubtreeAuthorityResolver interface {
	SameAuthority(context.Context, fsmeta.MountID, fsmeta.InodeID, fsmeta.InodeID) (bool, error)
}

// PerasAuthorityAdmitter is the fsmeta holder-side boundary for Peras.
// It is intentionally narrower than the root protocol: the executor only asks
// whether a compiled authority scope is locally owned before it can enter a
// future Peras visible commit.
type PerasAuthorityAdmitter interface {
	AcquirePerasAuthority(context.Context, compile.AuthorityScope) (owned bool, err error)
}

type PerasAuthorityRetirer = fsperas.AuthorityRetirer

// PerasCommitter is the experimental, opt-in Peras visible commit boundary.
// Success replaces the ordinary Percolator/Raft commit for this fsmeta
// operation, so errors are returned and never silently fall back after the
// holder overlay may already include the operation.
type PerasCommitter interface {
	CommitPeras(context.Context, fsperas.OperationID, compile.SemanticDelta, fsperas.AdmissionFunc) (fsperas.VisibleAck, error)
}

type PerasOverlayReader interface {
	GetPerasOverlay(key []byte) (value []byte, deleted bool, ok bool)
	ScanPerasOverlay(start []byte, limit uint32) []fsperas.OverlayKV
}

type PerasFlusher interface {
	Flush(context.Context) error
}

type PerasAuthorityFlusher interface {
	FlushAuthority(context.Context, compile.AuthorityScope) error
}

type PerasAuthorityDrainer interface {
	DrainAuthority(context.Context, fsperas.AuthorityRetirer, ...compile.AuthorityScope) error
}

type PerasQuotaAdmitter interface {
	AllowPerasVisibleQuota(context.Context, []QuotaChange) (bool, error)
}

// NegativeCache is the dentry-miss memo surface used by Lookup.
type NegativeCache interface {
	Has([]byte) bool
	Remember([]byte)
	Invalidate([]byte)
}

// DirPageCache is the ReadDirPlus page memo surface.
type DirPageCache interface {
	CurrentEpoch(dirpage.DirectoryKey) uint64
	Lookup(dirpage.PageKey, uint64) ([]dirpage.Entry, bool)
	MaterializeAsync(dirpage.PageKey, uint64, []dirpage.Entry) error
	Invalidate(dirpage.DirectoryKey) uint64
	Stats() dirpage.Stats
}

// Executor interprets fsmeta operation plans against a TxnRunner.
type Executor struct {
	runner                  TxnRunner
	inodes                  InodeAllocator
	mounts                  MountResolver
	quotas                  QuotaResolver
	subtrees                SubtreeHandoffPublisher
	authorities             SubtreeAuthorityResolver
	perasAuthority          PerasAuthorityAdmitter
	perasCommitter          PerasCommitter
	negCache                NegativeCache
	dirPages                DirPageCache
	lockTTL                 uint64
	now                     func() time.Time
	readRetriesTotal        atomic.Uint64
	readRetryExhaustedTotal atomic.Uint64
	txnRetriesTotal         atomic.Uint64
	txnRetryExhaustedTotal  atomic.Uint64
	createTotal             atomic.Uint64
	perasAdmission          perasAdmissionCounters
	perasVisible            perasVisibleCounters
	perasSeq                atomic.Uint64
	atomicOnePhase          map[fsmeta.OperationKind]*atomicOnePhaseCounters
}

type perasAdmissionCounters struct {
	eligibleTotal         atomic.Uint64
	slowTotal             atomic.Uint64
	slowReadOnlyTotal     atomic.Uint64
	slowRangeReadTotal    atomic.Uint64
	slowDurabilityTotal   atomic.Uint64
	slowCrossBucketTotal  atomic.Uint64
	slowSharedQuotaTotal  atomic.Uint64
	slowDynamicWriteTotal atomic.Uint64
	slowMaintenanceTotal  atomic.Uint64
	slowUnknownTotal      atomic.Uint64
	acquireTotal          atomic.Uint64
	ownedTotal            atomic.Uint64
	heldTotal             atomic.Uint64
	errorTotal            atomic.Uint64
}

type perasVisibleCounters struct {
	attemptTotal           atomic.Uint64
	successTotal           atomic.Uint64
	errorTotal             atomic.Uint64
	skipIneligibleTotal    atomic.Uint64
	skipNoAuthorityTotal   atomic.Uint64
	skipNonConcreteTotal   atomic.Uint64
	skipPlacementTotal     atomic.Uint64
	skipPredicateTotal     atomic.Uint64
	latencyTotalNanosecond atomic.Uint64
}

type atomicOnePhaseCounters struct {
	attemptTotal           atomic.Uint64
	skipTotal              atomic.Uint64
	backoffSkipTotal       atomic.Uint64
	runnerUnsupportedTotal atomic.Uint64
	fallbackTotal          atomic.Uint64
	successTotal           atomic.Uint64
	consecutiveFallbacks   atomic.Uint64
	mu                     sync.Mutex
	fallbacksByAffinity    map[string]uint64
}

// Option configures an Executor.
type Option func(*Executor)

// WithLockTTL overrides the Percolator lock TTL used by mutating operations.
func WithLockTTL(ttl uint64) Option {
	return func(e *Executor) {
		if ttl > 0 {
			e.lockTTL = ttl
		}
	}
}

// WithClock overrides the wall clock used for write-session expiry.
func WithClock(now func() time.Time) Option {
	return func(e *Executor) {
		if now != nil {
			e.now = now
		}
	}
}

// WithMountResolver enables rooted mount lifecycle admission for mutating
// fsmeta operations.
func WithMountResolver(resolver MountResolver) Option {
	return func(e *Executor) {
		e.mounts = resolver
	}
}

// WithQuotaResolver enables rooted quota-fence admission for resource-creating
// fsmeta operations.
func WithQuotaResolver(resolver QuotaResolver) Option {
	return func(e *Executor) {
		e.quotas = resolver
	}
}

// WithInodeAllocator enables server-side inode assignment for Create.
func WithInodeAllocator(allocator InodeAllocator) Option {
	return func(e *Executor) {
		e.inodes = allocator
	}
}

// WithNegativeCache wires the visible-commit "this dentry does not exist" memo.
// Lookup checks Has on the dentry primary key before consulting the runner;
// misses are recorded via Remember; mutating ops call Invalidate on the
// touched dentry keys after a successful commit.
//
// A nil cache disables the cache.
func WithNegativeCache(cache NegativeCache) Option {
	return func(e *Executor) {
		e.negCache = cache
	}
}

// WithDirPageCache wires the ReadDirPlus derived page cache. ReadDirPlus first asks the
// cache for a fresh page set keyed by (mountHash, parentInode); on hit the
// runner-side dentry scan + N inode BatchGet are skipped entirely. On miss, the
// runner path runs as today and the assembled DentryAttrPair slice is
// asynchronously materialized into the cache for the next call.
//
// Mutating ops (Create/Link/Unlink/Rename/RenameSubtree) call Invalidate
// on the affected parent directory's PageKey after a successful commit
// so subsequent Lookup observes the change.
//
// A nil cache disables the cache. The mount hash uses xxhash.Sum64
// over the MountID string, so collision probability is negligible.
func WithDirPageCache(cache DirPageCache) Option {
	return func(e *Executor) {
		e.dirPages = cache
	}
}

// WithSubtreeHandoffPublisher enables rooted subtree authority era advancement
// for RenameSubtree.
func WithSubtreeHandoffPublisher(publisher SubtreeHandoffPublisher) Option {
	return func(e *Executor) {
		e.subtrees = publisher
	}
}

// WithSubtreeAuthorityResolver enables admission for ordinary Rename. Without
// a resolver, the executor uses the current single-authority mount model.
func WithSubtreeAuthorityResolver(resolver SubtreeAuthorityResolver) Option {
	return func(e *Executor) {
		e.authorities = resolver
	}
}

// WithPerasAuthorityAdmitter enables holder-authority admission for
// Peras-eligible mutations.
func WithPerasAuthorityAdmitter(admitter PerasAuthorityAdmitter) Option {
	return func(e *Executor) {
		e.perasAuthority = admitter
	}
}

// WithPerasCommitter enables Peras visible commits. This option is intentionally
// explicit so production callers choose the visible-commit contract.
func WithPerasCommitter(committer PerasCommitter) Option {
	return func(e *Executor) {
		e.perasCommitter = committer
	}
}

// New constructs an fsmeta executor.
func New(runner TxnRunner, opts ...Option) (*Executor, error) {
	if runner == nil {
		return nil, errRunnerRequired
	}
	executor := &Executor{
		runner:         runner,
		lockTTL:        defaultLockTTL,
		atomicOnePhase: newAtomicOnePhaseCounters(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(executor)
		}
	}
	return executor, nil
}

// Stats returns executor counters suitable for expvar export.
func (e *Executor) Stats() map[string]any {
	if e == nil {
		return map[string]any{
			"read_retries_total":         uint64(0),
			"read_retry_exhausted_total": uint64(0),
			"txn_retries_total":          uint64(0),
			"txn_retry_exhausted_total":  uint64(0),
			"create_total":               uint64(0),
			"peras_admission":            perasAdmissionStats(nil, false),
			"peras_visible_commit":       perasVisibleStats(nil, false),
			"atomic_one_phase":           atomicOnePhaseStats(nil),
			"negative_cache_enabled":     false,
			"dirpage_cache_enabled":      false,
		}
	}
	out := map[string]any{
		"read_retries_total":         e.readRetriesTotal.Load(),
		"read_retry_exhausted_total": e.readRetryExhaustedTotal.Load(),
		"txn_retries_total":          e.txnRetriesTotal.Load(),
		"txn_retry_exhausted_total":  e.txnRetryExhaustedTotal.Load(),
		"create_total":               e.createTotal.Load(),
		"peras_admission":            perasAdmissionStats(&e.perasAdmission, e.perasAuthority != nil),
		"peras_visible_commit":       perasVisibleStats(&e.perasVisible, e.perasCommitter != nil),
		"atomic_one_phase":           atomicOnePhaseStats(e.atomicOnePhase),
		"negative_cache_enabled":     e.negCache != nil,
		"dirpage_cache_enabled":      e.dirPages != nil,
	}
	if stats, ok := e.runner.(statsProvider); ok {
		out["runner"] = stats.Stats()
	}
	if stats, ok := e.perasCommitter.(statsProvider); ok {
		out["peras_committer"] = stats.Stats()
	}
	if e.dirPages != nil {
		stats := e.dirPages.Stats()
		out["dirpage_hits"] = stats.Hits
		out["dirpage_misses"] = stats.Misses
		out["dirpage_stale"] = stats.Stale
		out["dirpage_store_ok"] = stats.StoreOK
		out["dirpage_dropped"] = stats.Dropped
	}
	if stats, ok := e.inodes.(statsProvider); ok {
		out["inode_allocator"] = stats.Stats()
	}
	return out
}

func perasAdmissionStats(counters *perasAdmissionCounters, enabled bool) map[string]any {
	if counters == nil {
		return map[string]any{
			"enabled":        enabled,
			"eligible_total": uint64(0),
			"slow_total":     uint64(0),
			"slow_by_reason": perasAdmissionSlowReasonStats(nil),
			"acquire_total":  uint64(0),
			"owned_total":    uint64(0),
			"held_total":     uint64(0),
			"error_total":    uint64(0),
		}
	}
	return map[string]any{
		"enabled":        enabled,
		"eligible_total": counters.eligibleTotal.Load(),
		"slow_total":     counters.slowTotal.Load(),
		"slow_by_reason": perasAdmissionSlowReasonStats(counters),
		"acquire_total":  counters.acquireTotal.Load(),
		"owned_total":    counters.ownedTotal.Load(),
		"held_total":     counters.heldTotal.Load(),
		"error_total":    counters.errorTotal.Load(),
	}
}

func perasVisibleStats(counters *perasVisibleCounters, enabled bool) map[string]any {
	if counters == nil {
		return map[string]any{
			"enabled":                    enabled,
			"attempt_total":              uint64(0),
			"success_total":              uint64(0),
			"error_total":                uint64(0),
			"skip_ineligible_total":      uint64(0),
			"skip_no_authority_total":    uint64(0),
			"skip_non_concrete_total":    uint64(0),
			"skip_placement_total":       uint64(0),
			"skip_predicate_total":       uint64(0),
			"latency_total_nanosecond":   uint64(0),
			"latency_average_nanosecond": uint64(0),
		}
	}
	attempts := counters.attemptTotal.Load()
	latency := counters.latencyTotalNanosecond.Load()
	average := uint64(0)
	if attempts > 0 {
		average = latency / attempts
	}
	return map[string]any{
		"enabled":                    enabled,
		"attempt_total":              attempts,
		"success_total":              counters.successTotal.Load(),
		"error_total":                counters.errorTotal.Load(),
		"skip_ineligible_total":      counters.skipIneligibleTotal.Load(),
		"skip_no_authority_total":    counters.skipNoAuthorityTotal.Load(),
		"skip_non_concrete_total":    counters.skipNonConcreteTotal.Load(),
		"skip_placement_total":       counters.skipPlacementTotal.Load(),
		"skip_predicate_total":       counters.skipPredicateTotal.Load(),
		"latency_total_nanosecond":   latency,
		"latency_average_nanosecond": average,
	}
}

func perasAdmissionSlowReasonStats(counters *perasAdmissionCounters) map[string]uint64 {
	if counters == nil {
		return map[string]uint64{
			string(compile.SlowReasonReadOnly):          0,
			string(compile.SlowReasonRangeRead):         0,
			string(compile.SlowReasonDurabilityBarrier): 0,
			string(compile.SlowReasonCrossBucket):       0,
			string(compile.SlowReasonSharedQuota):       0,
			string(compile.SlowReasonDynamicWriteSet):   0,
			string(compile.SlowReasonMaintenanceScan):   0,
			"unknown": 0,
		}
	}
	return map[string]uint64{
		string(compile.SlowReasonReadOnly):          counters.slowReadOnlyTotal.Load(),
		string(compile.SlowReasonRangeRead):         counters.slowRangeReadTotal.Load(),
		string(compile.SlowReasonDurabilityBarrier): counters.slowDurabilityTotal.Load(),
		string(compile.SlowReasonCrossBucket):       counters.slowCrossBucketTotal.Load(),
		string(compile.SlowReasonSharedQuota):       counters.slowSharedQuotaTotal.Load(),
		string(compile.SlowReasonDynamicWriteSet):   counters.slowDynamicWriteTotal.Load(),
		string(compile.SlowReasonMaintenanceScan):   counters.slowMaintenanceTotal.Load(),
		"unknown": counters.slowUnknownTotal.Load(),
	}
}

func (s *perasAdmissionCounters) recordSlow(reason compile.SlowReason) {
	s.slowTotal.Add(1)
	switch reason {
	case compile.SlowReasonReadOnly:
		s.slowReadOnlyTotal.Add(1)
	case compile.SlowReasonRangeRead:
		s.slowRangeReadTotal.Add(1)
	case compile.SlowReasonDurabilityBarrier:
		s.slowDurabilityTotal.Add(1)
	case compile.SlowReasonCrossBucket:
		s.slowCrossBucketTotal.Add(1)
	case compile.SlowReasonSharedQuota:
		s.slowSharedQuotaTotal.Add(1)
	case compile.SlowReasonDynamicWriteSet:
		s.slowDynamicWriteTotal.Add(1)
	case compile.SlowReasonMaintenanceScan:
		s.slowMaintenanceTotal.Add(1)
	default:
		s.slowUnknownTotal.Add(1)
	}
}

var atomicOnePhaseKinds = [...]fsmeta.OperationKind{
	fsmeta.OperationCreate,
	fsmeta.OperationUpdateInode,
	fsmeta.OperationRename,
	fsmeta.OperationLink,
	fsmeta.OperationUnlink,
	fsmeta.OperationOpenWriteSession,
	fsmeta.OperationHeartbeatSession,
	fsmeta.OperationCloseSession,
}

func newAtomicOnePhaseCounters() map[fsmeta.OperationKind]*atomicOnePhaseCounters {
	out := make(map[fsmeta.OperationKind]*atomicOnePhaseCounters, len(atomicOnePhaseKinds))
	for _, kind := range atomicOnePhaseKinds {
		out[kind] = &atomicOnePhaseCounters{fallbacksByAffinity: make(map[string]uint64)}
	}
	return out
}

func atomicOnePhaseStats(counters map[fsmeta.OperationKind]*atomicOnePhaseCounters) map[string]any {
	out := make(map[string]any, len(atomicOnePhaseKinds))
	for _, kind := range atomicOnePhaseKinds {
		var stats *atomicOnePhaseCounters
		if counters != nil {
			stats = counters[kind]
		}
		out[string(kind)] = atomicOnePhaseStatsFor(stats)
	}
	return out
}

func atomicOnePhaseStatsFor(stats *atomicOnePhaseCounters) map[string]uint64 {
	if stats == nil {
		return map[string]uint64{
			"attempt_total":            0,
			"skip_total":               0,
			"backoff_skip_total":       0,
			"runner_unsupported_total": 0,
			"fallback_total":           0,
			"success_total":            0,
			"consecutive_fallbacks":    0,
		}
	}
	return map[string]uint64{
		"attempt_total":            stats.attemptTotal.Load(),
		"skip_total":               stats.skipTotal.Load(),
		"backoff_skip_total":       stats.backoffSkipTotal.Load(),
		"runner_unsupported_total": stats.runnerUnsupportedTotal.Load(),
		"fallback_total":           stats.fallbackTotal.Load(),
		"success_total":            stats.successTotal.Load(),
		"consecutive_fallbacks":    stats.consecutiveFallbacks.Load(),
	}
}

// Create creates one dentry and its inode record in a single transaction.
func (e *Executor) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	if e.inodes == nil {
		return fsmeta.CreateResult{}, errInodeAllocatorRequired
	}
	if _, err := fsmeta.EncodeInodeValue(req.Attrs.InodeRecord(fsmeta.RootInode)); err != nil {
		return fsmeta.CreateResult{}, err
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	mount := mountRecord.Identity()
	// Allocate after cheap semantic validation and mount admission. Transaction
	// retries below reuse this single ID; failed creates may leave coordinator
	// ID gaps, but they cannot publish a different inode on retry.
	inodeID, err := e.inodes.AllocateCreateInode(ctx, mount, req.Parent, req.Name)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	delta, err := compile.Create(req, mount, inodeID, compile.WithQuotaMode(e.perasQuotaMode()))
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.CreateResult{}, err
	}
	plan := delta.Plan
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inodeID,
		Type:   inode.Type,
	}
	dentryValue, err := fsmeta.EncodeDentryValue(dentry)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	inodeValue, err := fsmeta.EncodeInodeValue(inode)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	e.createTotal.Add(1)
	quotaChanges := []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      inodeSizeDelta(inode.Size),
		Inodes:     1,
	}}
	quotaOK := true
	if e.perasCommitter != nil && e.perasAuthority != nil && delta.Eligibility == compile.EligibilityVisibleCommit {
		var err error
		quotaOK, err = e.perasQuotaAllowsVisibleCommit(ctx, quotaChanges)
		if err != nil {
			return fsmeta.CreateResult{}, err
		}
	}
	if quotaOK {
		if committed, err := e.tryPerasVisibleCommit(ctx, delta); committed || err != nil {
			if err != nil {
				return fsmeta.CreateResult{}, err
			}
			e.rememberPerasCreate(mount, plan, inode)
			e.invalidateNegative(plan.MutateKeys[0])
			e.invalidateDirPages(req.Mount, req.Parent)
			return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
		}
	}
	mutations := []*kvrpcpb.Mutation{
		{
			Op:                kvrpcpb.Mutation_Put,
			Key:               cloneBytes(plan.MutateKeys[0]),
			Value:             dentryValue,
			AssertionNotExist: true,
		},
		{
			Op:                kvrpcpb.Mutation_Put,
			Key:               cloneBytes(plan.MutateKeys[1]),
			Value:             inodeValue,
			AssertionNotExist: true,
		},
	}
	predicates := []*kvrpcpb.AtomicPredicate{atomicNotExists(plan.MutateKeys[0]), atomicNotExists(plan.MutateKeys[1])}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.Parent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}}, startVersion)
		if err != nil {
			return err
		}
		all := append(cloneMutations(mutations), quotaMutations...)
		if len(quotaMutations) == 0 {
			// One-phase counters are per transaction attempt, not per logical
			// Create, so contention retries and admission misses stay visible.
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, all, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, all, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return fsmeta.CreateResult{}, err
	}
	// The new dentry replaces a previously-missing key; drop any negative
	// memo a prior Lookup may have planted, and bump the parent's dirpage
	// epoch so a stale ReadDirPlus result cannot mask the new entry.
	e.rememberPerasCreate(mount, plan, inode)
	e.invalidateNegative(plan.MutateKeys[0])
	e.invalidateDirPages(req.Mount, req.Parent)
	return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
}

func (e *Executor) mutateWithAtomicOnePhase(ctx context.Context, kind fsmeta.OperationKind, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) error {
	stats := e.atomicOnePhaseCounters(kind)
	onePhase, ok := e.runner.(AtomicMutateOnePhase)
	if !ok {
		if stats != nil {
			stats.runnerUnsupportedTotal.Add(1)
		}
		_, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
		return err
	}
	affinity := atomicOnePhaseAffinity(primary, mutations)
	if stats != nil && !stats.allowAttempt(affinity) {
		stats.skipTotal.Add(1)
		_, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
		return err
	}
	if stats != nil {
		stats.attemptTotal.Add(1)
	}
	handled, err := onePhase.TryAtomicMutate(ctx, primary, cloneAtomicPredicates(predicates), cloneMutations(mutations), startVersion, commitVersion)
	if err != nil || handled {
		if err == nil && stats != nil {
			stats.successTotal.Add(1)
			stats.recordSuccess(affinity)
		}
		return err
	}
	if stats != nil {
		stats.fallbackTotal.Add(1)
		stats.recordFallback(affinity)
	}
	_, err = e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
	return err
}

const (
	atomicOnePhaseBackoffAfter = 16
	atomicOnePhaseProbeEvery   = 128
)

func (s *atomicOnePhaseCounters) allowAttempt(affinity string) bool {
	if s == nil {
		return true
	}
	if s.affinityFallbacks(affinity) < atomicOnePhaseBackoffAfter {
		return true
	}
	// Some plans are only conditionally co-located. Back off after repeated
	// admission misses for the same placement pattern, but do not let unrelated
	// patterns force all operations of this kind back onto 2PC.
	return s.backoffSkipTotal.Add(1)%atomicOnePhaseProbeEvery == 0
}

func (s *atomicOnePhaseCounters) affinityFallbacks(affinity string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fallbacksByAffinity[affinity]
}

func (s *atomicOnePhaseCounters) recordFallback(affinity string) {
	s.mu.Lock()
	next := s.fallbacksByAffinity[affinity] + 1
	s.fallbacksByAffinity[affinity] = next
	s.mu.Unlock()
	s.consecutiveFallbacks.Store(next)
}

func (s *atomicOnePhaseCounters) recordSuccess(affinity string) {
	s.mu.Lock()
	delete(s.fallbacksByAffinity, affinity)
	s.mu.Unlock()
	s.consecutiveFallbacks.Store(0)
}

func atomicOnePhaseAffinity(primary []byte, mutations []*kvrpcpb.Mutation) string {
	const virtualShards = 64
	shards := make([]int, 0, 1+len(mutations))
	if len(primary) > 0 {
		shards = append(shards, fsmeta.ShardForUserKey(primary, virtualShards))
	}
	for _, mutation := range mutations {
		if mutation == nil || len(mutation.GetKey()) == 0 {
			continue
		}
		shards = append(shards, fsmeta.ShardForUserKey(mutation.GetKey(), virtualShards))
	}
	if len(shards) == 0 {
		return "empty"
	}
	sort.Ints(shards)
	out := make([]byte, 0, len(shards)*3)
	for i, shard := range shards {
		if i > 0 {
			out = append(out, ',')
		}
		out = fmt.Appendf(out, "%02d", shard)
	}
	return string(out)
}

func (e *Executor) mutateWithoutAtomicOnePhase(ctx context.Context, kind fsmeta.OperationKind, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) error {
	if stats := e.atomicOnePhaseCounters(kind); stats != nil {
		stats.skipTotal.Add(1)
	}
	_, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
	return err
}

func (e *Executor) atomicOnePhaseCounters(kind fsmeta.OperationKind) *atomicOnePhaseCounters {
	if e == nil {
		return nil
	}
	return e.atomicOnePhase[kind]
}

func (e *Executor) admitPerasAuthority(ctx context.Context, delta compile.SemanticDelta) error {
	if e == nil || e.perasAuthority == nil {
		return nil
	}
	if delta.Eligibility != compile.EligibilityVisibleCommit {
		e.perasAdmission.recordSlow(delta.SlowReason)
		return nil
	}
	e.perasAdmission.eligibleTotal.Add(1)
	if e.perasCommitter != nil {
		return nil
	}
	e.perasAdmission.acquireTotal.Add(1)
	owned, err := e.perasAuthority.AcquirePerasAuthority(ctx, delta.Authority)
	if err != nil {
		e.perasAdmission.errorTotal.Add(1)
		return nil
	}
	if !owned {
		e.perasAdmission.heldTotal.Add(1)
		return nil
	}
	e.perasAdmission.ownedTotal.Add(1)
	return nil
}

func (e *Executor) tryPerasVisibleCommit(ctx context.Context, delta compile.SemanticDelta) (bool, error) {
	if e == nil || e.perasCommitter == nil {
		return false, nil
	}
	if e.perasAuthority == nil {
		e.perasVisible.skipNoAuthorityTotal.Add(1)
		return false, nil
	}
	if delta.Eligibility != compile.EligibilityVisibleCommit {
		e.perasVisible.skipIneligibleTotal.Add(1)
		return false, nil
	}
	if !perasDeltaHasConcreteWrites(delta) {
		e.perasVisible.skipNonConcreteTotal.Add(1)
		return false, nil
	}
	id := e.nextPerasOperationID(delta.Kind)
	e.perasVisible.attemptTotal.Add(1)
	start := time.Now()
	_, err := e.perasCommitter.CommitPeras(ctx, id, delta, e.perasPredicatesHold)
	e.perasVisible.latencyTotalNanosecond.Add(uint64(time.Since(start).Nanoseconds()))
	if err != nil {
		if errors.Is(err, fsperas.ErrAdmissionRejected) ||
			errors.Is(err, fsperas.ErrIneligibleOperation) ||
			errors.Is(err, errPerasAuthorityNotHeld) ||
			nokverrors.KindOf(err) == nokverrors.KindNotLeader {
			e.perasVisible.skipPredicateTotal.Add(1)
			return false, nil
		}
		if isPerasAdmissionTerminalError(err) {
			e.perasVisible.skipPredicateTotal.Add(1)
			return true, err
		}
		e.perasVisible.errorTotal.Add(1)
		return true, err
	}
	e.perasVisible.successTotal.Add(1)
	return true, nil
}

func (e *Executor) perasPredicatesHold(ctx context.Context, delta compile.SemanticDelta) (bool, error) {
	if len(delta.ReadPredicates) == 0 {
		return true, nil
	}
	index := e.perasPredicateIndex()
	var version uint64
	var haveVersion bool
	read := func(key []byte) (bool, error) {
		if !haveVersion {
			var err error
			version, err = e.reserveReadVersion(ctx)
			if err != nil {
				return false, err
			}
			haveVersion = true
		}
		_, ok, err := e.getMergedValue(ctx, key, version)
		return ok, err
	}
	for _, predicate := range delta.ReadPredicates {
		switch predicate.Kind {
		case compile.PredicateExists:
			if index != nil {
				present, known := index.KeyState(predicate.Key)
				if known {
					if !present {
						return false, fsmeta.ErrNotFound
					}
					continue
				}
			}
			ok, err := read(predicate.Key)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, fsmeta.ErrNotFound
			}
		case compile.PredicateNotExists:
			if index != nil {
				present, known := index.KeyState(predicate.Key)
				if known {
					if present {
						return false, fsmeta.ErrExists
					}
					continue
				}
				if e.perasNotExistsKnown(delta.Authority, predicate.Key, index) ||
					perasNotExistsDerivedFromDelta(delta, predicate, index) {
					continue
				}
			}
			ok, err := read(predicate.Key)
			if err != nil {
				return false, err
			}
			if ok {
				return false, fsmeta.ErrExists
			}
		case compile.PredicateObservedValue:
			// Concrete Peras operation builders read these keys before
			// constructing WriteEffects. The compiler does not yet carry the
			// observed value digest needed for a generic equality check here.
			continue
		case compile.PredicatePrefixScan:
			return false, nil
		default:
			return false, nil
		}
	}
	return true, nil
}

func (e *Executor) perasPredicateIndex() fsperas.PredicateIndex {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	index, ok := e.perasCommitter.(fsperas.PredicateIndex)
	if !ok {
		return nil
	}
	return index
}

func (e *Executor) rememberPerasCreate(mount fsmeta.MountIdentity, plan fsmeta.OperationPlan, inode fsmeta.InodeRecord) {
	index := e.perasPredicateIndex()
	if index == nil {
		return
	}
	if len(plan.MutateKeys) > 0 {
		index.RememberKey(plan.MutateKeys[0], true)
	}
	if len(plan.MutateKeys) > 1 {
		index.RememberKey(plan.MutateKeys[1], true)
	}
	if inode.Type == fsmeta.InodeTypeDirectory {
		index.RememberEmptyDirectory(mount, inode.Inode)
		return
	}
	ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, inode.Inode)
	if err == nil {
		index.RememberKey(ownerKey, false)
	}
}

func perasNotExistsDerivedFromDelta(delta compile.SemanticDelta, predicate compile.Predicate, index fsperas.PredicateIndex) bool {
	if delta.Kind != fsmeta.OperationCreate || len(delta.Plan.MutateKeys) < 2 {
		return false
	}
	if bytes.Equal(predicate.Key, delta.Plan.MutateKeys[1]) {
		return true
	}
	if !bytes.Equal(predicate.Key, delta.Plan.MutateKeys[0]) || len(delta.Authority.Parents) != 1 {
		return false
	}
	return index.DirectoryEmpty(fsmeta.MountIdentity{
		MountID:    delta.Authority.Mount,
		MountKeyID: delta.Authority.MountKeyID,
	}, delta.Authority.Parents[0])
}

func (e *Executor) perasNotExistsKnown(scope compile.AuthorityScope, key []byte, index fsperas.PredicateIndex) bool {
	if index == nil || len(key) == 0 || scope.Mount == "" || scope.MountKeyID == 0 {
		return false
	}
	present, known := index.KeyState(key)
	if known {
		return !present
	}
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.Kind != fsmeta.KeyKindDentry || parts.MountKeyID != scope.MountKeyID {
		return false
	}
	return index.DirectoryEmpty(fsmeta.MountIdentity{
		MountID:    scope.Mount,
		MountKeyID: scope.MountKeyID,
	}, parts.Parent)
}

func isPerasAdmissionTerminalError(err error) bool {
	return errors.Is(err, fsmeta.ErrExists) ||
		errors.Is(err, fsmeta.ErrNotFound) ||
		errors.Is(err, fsmeta.ErrInvalidRequest) ||
		errors.Is(err, fsmeta.ErrInvalidValue)
}

func concretePerasDelta(delta compile.SemanticDelta, effects []compile.WriteEffect) compile.SemanticDelta {
	delta.WriteEffects = effects
	return delta
}

func runtimeCheckedPerasDelta(delta compile.SemanticDelta, effects []compile.WriteEffect) compile.SemanticDelta {
	delta = concretePerasDelta(delta, effects)
	for i := range delta.ReadPredicates {
		if delta.ReadPredicates[i].Kind != compile.PredicatePrefixScan {
			delta.ReadPredicates[i].Kind = compile.PredicateObservedValue
		}
	}
	return delta
}

func perasPutEffect(key, value []byte) compile.WriteEffect {
	return compile.WriteEffect{Kind: compile.EffectPut, Key: cloneBytes(key), Value: cloneBytes(value)}
}

func perasDeleteEffect(key []byte) compile.WriteEffect {
	return compile.WriteEffect{Kind: compile.EffectDelete, Key: cloneBytes(key)}
}

func (e *Executor) tryPerasVisibleOpenWriteSession(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, mount fsmeta.MountIdentity, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return fsmeta.SessionRecord{}, false, nil
	}
	view := e.newPerasReadView(ctx)
	inode, ok, err := view.readInode(mount, req.Inode)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	if !ok {
		return fsmeta.SessionRecord{}, false, nil
	}
	if inode.Type != fsmeta.InodeTypeFile {
		return fsmeta.SessionRecord{}, false, nil
	}
	nowTime := e.clock()
	expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
	if !ok {
		return fsmeta.SessionRecord{}, false, nil
	}
	now := nowTime.UnixNano()
	if existing, ok, err := view.readSession(plan.ReadKeys[1]); err != nil {
		return fsmeta.SessionRecord{}, false, err
	} else if ok {
		if sessionLive(existing, now) {
			return fsmeta.SessionRecord{}, false, nil
		}
		// Stale cleanup is value-sensitive and may touch an old session-id key
		// outside this request's concrete write-set. Keep it on the transaction
		// runner.
		return fsmeta.SessionRecord{}, false, nil
	}
	if index := e.perasPredicateIndex(); !e.perasNotExistsKnown(delta.Authority, plan.ReadKeys[2], index) {
		if owner, ok, err := view.readSession(plan.ReadKeys[2]); err != nil {
			return fsmeta.SessionRecord{}, false, err
		} else if ok {
			if sessionLive(owner, now) {
				return fsmeta.SessionRecord{}, false, nil
			}
			return fsmeta.SessionRecord{}, false, nil
		}
	}
	record := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
	value, err := fsmeta.EncodeSessionValue(record)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	concrete := runtimeCheckedPerasDelta(delta, []compile.WriteEffect{
		perasPutEffect(plan.MutateKeys[0], value),
		perasPutEffect(plan.MutateKeys[1], value),
	})
	committed, err := e.tryPerasVisibleCommit(ctx, concrete)
	if err != nil {
		return fsmeta.SessionRecord{}, committed, err
	}
	if !committed {
		return fsmeta.SessionRecord{}, false, nil
	}
	return record, true, nil
}

func (e *Executor) tryPerasVisibleHeartbeatWriteSession(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return fsmeta.SessionRecord{}, false, nil
	}
	view := e.newPerasReadView(ctx)
	nowTime := e.clock()
	expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
	if !ok {
		return fsmeta.SessionRecord{}, false, nil
	}
	now := nowTime.UnixNano()
	session, ok, err := view.readSession(plan.ReadKeys[0])
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	if !ok || !sessionLive(session, now) || session.Inode != req.Inode {
		return fsmeta.SessionRecord{}, false, nil
	}
	owner, ok, err := view.readSession(plan.ReadKeys[1])
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	if !ok || !sessionLive(owner, now) || owner.Session != req.Session || owner.Inode != req.Inode {
		return fsmeta.SessionRecord{}, false, nil
	}
	record := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
	value, err := fsmeta.EncodeSessionValue(record)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	concrete := runtimeCheckedPerasDelta(delta, []compile.WriteEffect{
		perasPutEffect(plan.MutateKeys[0], value),
		perasPutEffect(plan.MutateKeys[1], value),
	})
	committed, err := e.tryPerasVisibleCommit(ctx, concrete)
	if err != nil {
		return fsmeta.SessionRecord{}, committed, err
	}
	if !committed {
		return fsmeta.SessionRecord{}, false, nil
	}
	return record, true, nil
}

func (e *Executor) tryPerasVisibleCloseWriteSession(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, mount fsmeta.MountIdentity, req fsmeta.CloseWriteSessionRequest) (bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newPerasReadView(ctx)
	session, ok, err := view.readSession(plan.ReadKeys[0])
	if err != nil {
		return false, err
	}
	if !ok || session.Inode != req.Inode {
		return false, nil
	}
	effects := []compile.WriteEffect{perasDeleteEffect(plan.MutateKeys[0])}
	ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, session.Inode)
	if err != nil {
		return false, err
	}
	if owner, ok, err := view.readSession(ownerKey); err != nil {
		return false, err
	} else if ok && owner.Session == req.Session && owner.Inode == session.Inode {
		effects = append(effects, perasDeleteEffect(ownerKey))
	}
	concrete := runtimeCheckedPerasDelta(delta, effects)
	return e.tryPerasVisibleCommit(ctx, concrete)
}

func (e *Executor) tryPerasVisibleUpdateInode(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, mount fsmeta.MountIdentity, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return fsmeta.InodeRecord{}, false, nil
	}
	view := e.newPerasReadView(ctx)
	dentry, err := view.readDentry(plan.ReadKeys[0])
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	if dentry.Inode != req.Inode {
		return fsmeta.InodeRecord{}, false, fsmeta.ErrInvalidRequest
	}
	inode, ok, err := view.readInode(mount, req.Inode)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	if !ok {
		return fsmeta.InodeRecord{}, false, fsmeta.ErrNotFound
	}
	if dentry.Type != inode.Type {
		return fsmeta.InodeRecord{}, false, fsmeta.ErrInvalidValue
	}
	if inode.LinkCount != 1 {
		return fsmeta.InodeRecord{}, false, fsmeta.ErrInvalidRequest
	}
	sizeDelta := int64(0)
	if req.SetSize {
		sizeDelta = inodeSizeChange(inode.Size, req.Size)
		if sizeDelta != 0 {
			quotaOK, err := e.perasQuotaAllowsVisibleCommit(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      sizeDelta,
			}})
			if err != nil {
				return fsmeta.InodeRecord{}, false, err
			}
			if !quotaOK {
				return fsmeta.InodeRecord{}, false, nil
			}
		}
	}
	if req.SetMode {
		inode.Mode = req.Mode
	}
	if req.SetSize {
		inode.Size = req.Size
	}
	if req.SetUpdatedUnixNs {
		inode.UpdatedUnixNs = req.UpdatedUnixNs
	}
	if req.SetOpaqueAttrs {
		inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
	}
	value, err := fsmeta.EncodeInodeValue(inode)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	concrete := runtimeCheckedPerasDelta(delta, []compile.WriteEffect{perasPutEffect(plan.MutateKeys[0], value)})
	committed, err := e.tryPerasVisibleCommit(ctx, concrete)
	if err != nil {
		return fsmeta.InodeRecord{}, committed, err
	}
	if !committed {
		return fsmeta.InodeRecord{}, false, nil
	}
	return inode, true, nil
}

func (e *Executor) tryPerasVisibleRename(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, move renameMove) (bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newPerasReadView(ctx)
	record, err := view.readDentry(plan.ReadKeys[0])
	if err != nil {
		return false, err
	}
	if !e.perasNotExistsKnown(delta.Authority, plan.ReadKeys[1], e.perasPredicateIndex()) {
		if _, err := view.readDentry(plan.ReadKeys[1]); err == nil {
			return false, fsmeta.ErrExists
		} else if !errors.Is(err, fsmeta.ErrNotFound) {
			return false, err
		}
	}
	if move.fromParent != move.toParent {
		if inode, ok, err := view.readInode(move.identity, record.Inode); err != nil {
			return false, err
		} else if ok {
			quotaOK, err := e.perasQuotaAllowsVisibleCommit(ctx, []QuotaChange{
				{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(inode.Size), Inodes: -1},
				{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(inode.Size), Inodes: 1},
			})
			if err != nil {
				return false, err
			}
			if !quotaOK {
				return false, nil
			}
		}
	}
	record.Parent = move.toParent
	record.Name = move.toName
	value, err := fsmeta.EncodeDentryValue(record)
	if err != nil {
		return false, err
	}
	concrete := runtimeCheckedPerasDelta(delta, []compile.WriteEffect{
		perasDeleteEffect(plan.MutateKeys[0]),
		perasPutEffect(plan.MutateKeys[1], value),
	})
	return e.tryPerasVisibleCommit(ctx, concrete)
}

func (e *Executor) tryPerasVisibleLink(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, mount fsmeta.MountIdentity, req fsmeta.LinkRequest) (bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newPerasReadView(ctx)
	record, err := view.readDentry(plan.ReadKeys[0])
	if err != nil {
		return false, err
	}
	if record.Type == fsmeta.InodeTypeDirectory {
		return false, fsmeta.ErrInvalidRequest
	}
	if !e.perasNotExistsKnown(delta.Authority, plan.ReadKeys[1], e.perasPredicateIndex()) {
		if _, err := view.readDentry(plan.ReadKeys[1]); err == nil {
			return false, fsmeta.ErrExists
		} else if !errors.Is(err, fsmeta.ErrNotFound) {
			return false, err
		}
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fsmeta.ErrNotFound
	}
	if inode.Type == fsmeta.InodeTypeDirectory || inode.LinkCount == ^uint32(0) {
		return false, fsmeta.ErrInvalidRequest
	}
	if inode.LinkCount == 0 {
		inode.LinkCount = 1
	}
	quotaOK, err := e.perasQuotaAllowsVisibleCommit(ctx, []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.ToParent,
		Bytes:      inodeSizeDelta(inode.Size),
		Inodes:     1,
	}})
	if err != nil {
		return false, err
	}
	if !quotaOK {
		return false, nil
	}
	inode.LinkCount++
	dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: req.ToParent,
		Name:   req.ToName,
		Inode:  record.Inode,
		Type:   record.Type,
	})
	if err != nil {
		return false, err
	}
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return false, err
	}
	inodeValue, err := fsmeta.EncodeInodeValue(inode)
	if err != nil {
		return false, err
	}
	concrete := runtimeCheckedPerasDelta(delta, []compile.WriteEffect{
		perasPutEffect(plan.ReadKeys[1], dentryValue),
		perasPutEffect(inodeKey, inodeValue),
	})
	return e.tryPerasVisibleCommit(ctx, concrete)
}

func (e *Executor) tryPerasVisibleUnlink(ctx context.Context, delta compile.SemanticDelta, plan fsmeta.OperationPlan, mount fsmeta.MountIdentity, req fsmeta.UnlinkRequest) (bool, error) {
	if e == nil || e.perasCommitter == nil || e.perasAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newPerasReadView(ctx)
	record, err := view.readDentry(plan.PrimaryKey)
	if err != nil {
		return false, err
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok || inode.Type == fsmeta.InodeTypeDirectory {
		return false, nil
	}
	quotaOK, err := e.perasQuotaAllowsVisibleCommit(ctx, []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      -inodeSizeDelta(inode.Size),
		Inodes:     -1,
	}})
	if err != nil {
		return false, err
	}
	if !quotaOK {
		return false, nil
	}
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return false, err
	}
	effects := []compile.WriteEffect{perasDeleteEffect(plan.MutateKeys[0])}
	if inode.LinkCount <= 1 {
		effects = append(effects, perasDeleteEffect(inodeKey))
	} else {
		inode.LinkCount--
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return false, err
		}
		effects = append(effects, perasPutEffect(inodeKey, inodeValue))
	}
	concrete := runtimeCheckedPerasDelta(delta, effects)
	return e.tryPerasVisibleCommit(ctx, concrete)
}

func (e *Executor) perasOverlay() PerasOverlayReader {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	overlay, ok := e.perasCommitter.(PerasOverlayReader)
	if !ok {
		return nil
	}
	return overlay
}

func (e *Executor) flushPeras(ctx context.Context) error {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	flusher, ok := e.perasCommitter.(PerasFlusher)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return flusher.Flush(context.WithoutCancel(ctx))
}

func (e *Executor) flushPerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	if len(scopes) == 0 {
		return e.flushPeras(ctx)
	}
	if scoped, ok := e.perasCommitter.(PerasAuthorityFlusher); ok {
		for _, scope := range scopes {
			if authorityScopeEmpty(scope) {
				return e.flushPeras(ctx)
			}
			if err := scoped.FlushAuthority(context.WithoutCancel(ctx), scope); err != nil {
				return err
			}
		}
		return nil
	}
	return e.flushPeras(ctx)
}

func (e *Executor) retirePerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil || e.perasAuthority == nil {
		return nil
	}
	retirer, ok := e.perasAuthority.(PerasAuthorityRetirer)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return retirer.RetirePerasAuthority(context.WithoutCancel(ctx), scopes...)
}

func (e *Executor) drainPerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if e == nil {
		return nil
	}
	if e.perasCommitter != nil && e.perasAuthority != nil {
		drainer, drainOK := e.perasCommitter.(PerasAuthorityDrainer)
		retirer, retireOK := e.perasAuthority.(PerasAuthorityRetirer)
		if drainOK && retireOK {
			if ctx == nil {
				ctx = context.Background()
			}
			return drainer.DrainAuthority(context.WithoutCancel(ctx), retirer, scopes...)
		}
	}
	if err := e.flushPerasAuthority(ctx, scopes...); err != nil {
		return err
	}
	return e.retirePerasAuthority(ctx, scopes...)
}

func authorityScopeEmpty(scope compile.AuthorityScope) bool {
	return scope.Mount == "" || scope.MountKeyID == 0
}

func (e *Executor) perasOverlayGet(key []byte) ([]byte, bool, bool) {
	overlay := e.perasOverlay()
	if overlay == nil {
		return nil, false, false
	}
	return overlay.GetPerasOverlay(key)
}

func (e *Executor) getMergedValue(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	if value, deleted, ok := e.perasOverlayGet(key); ok {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	return e.runner.Get(ctx, key, version)
}

type perasReadView struct {
	executor    *Executor
	ctx         context.Context
	version     uint64
	haveVersion bool
}

func (e *Executor) newPerasReadView(ctx context.Context) *perasReadView {
	if ctx == nil {
		ctx = context.Background()
	}
	return &perasReadView{executor: e, ctx: ctx}
}

func (v *perasReadView) get(key []byte) ([]byte, bool, error) {
	if v == nil || v.executor == nil {
		return nil, false, fsmeta.ErrInvalidRequest
	}
	if value, deleted, ok := v.executor.perasOverlayGet(key); ok {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	if !v.haveVersion {
		version, err := v.executor.reserveReadVersion(v.ctx)
		if err != nil {
			return nil, false, err
		}
		v.version = version
		v.haveVersion = true
	}
	return v.executor.runner.Get(v.ctx, key, v.version)
}

func (v *perasReadView) readDentry(key []byte) (fsmeta.DentryRecord, error) {
	value, ok, err := v.get(key)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

func (v *perasReadView) readInode(mount fsmeta.MountIdentity, inodeID fsmeta.InodeID) (fsmeta.InodeRecord, bool, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := v.get(key)
	if err != nil || !ok {
		return fsmeta.InodeRecord{}, ok, err
	}
	inode, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (v *perasReadView) readSession(key []byte) (fsmeta.SessionRecord, bool, error) {
	value, ok, err := v.get(key)
	if err != nil || !ok {
		return fsmeta.SessionRecord{}, ok, err
	}
	session, err := fsmeta.DecodeSessionValue(value)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	return session, true, nil
}

func (e *Executor) mergePerasOverlayValues(keys [][]byte, values map[string][]byte) {
	overlay := e.perasOverlay()
	if overlay == nil {
		return
	}
	for _, key := range keys {
		value, deleted, ok := overlay.GetPerasOverlay(key)
		if !ok {
			continue
		}
		if deleted {
			delete(values, string(key))
		} else {
			values[string(key)] = value
		}
	}
}

func (e *Executor) mergePerasOverlayScan(kvs []KV, start []byte, limit uint32) []KV {
	overlay := e.perasOverlay()
	if overlay == nil || limit == 0 {
		return kvs
	}
	overlayKVs := overlay.ScanPerasOverlay(start, limit)
	if len(overlayKVs) == 0 {
		return kvs
	}
	out := make([]KV, 0, int(limit))
	base, peras := 0, 0
	for len(out) < int(limit) && (base < len(kvs) || peras < len(overlayKVs)) {
		switch {
		case base >= len(kvs):
			out = appendOverlayScanKV(out, overlayKVs[peras])
			peras++
		case peras >= len(overlayKVs):
			out = append(out, kvs[base])
			base++
		default:
			cmp := bytes.Compare(kvs[base].Key, overlayKVs[peras].Key)
			switch {
			case cmp < 0:
				out = append(out, kvs[base])
				base++
			case cmp > 0:
				out = appendOverlayScanKV(out, overlayKVs[peras])
				peras++
			default:
				out = appendOverlayScanKV(out, overlayKVs[peras])
				base++
				peras++
			}
		}
	}
	return out
}

func appendOverlayScanKV(out []KV, kv fsperas.OverlayKV) []KV {
	if kv.Delete {
		return out
	}
	return append(out, KV{Key: kv.Key, Value: kv.Value})
}

func (e *Executor) nextPerasOperationID(kind fsmeta.OperationKind) fsperas.OperationID {
	seq := uint64(1)
	if e != nil {
		seq = e.perasSeq.Add(1)
	}
	return fsperas.OperationID{ClientID: perasOperationClientID(kind), Seq: seq}
}

func perasOperationClientID(kind fsmeta.OperationKind) string {
	switch kind {
	case fsmeta.OperationCreate:
		return "fsmeta-exec/create"
	case fsmeta.OperationUpdateInode:
		return "fsmeta-exec/update_inode"
	case fsmeta.OperationRename:
		return "fsmeta-exec/rename"
	case fsmeta.OperationLink:
		return "fsmeta-exec/link"
	case fsmeta.OperationUnlink:
		return "fsmeta-exec/unlink"
	case fsmeta.OperationOpenWriteSession:
		return "fsmeta-exec/open_write_session"
	case fsmeta.OperationHeartbeatSession:
		return "fsmeta-exec/heartbeat_write_session"
	case fsmeta.OperationCloseSession:
		return "fsmeta-exec/close_write_session"
	default:
		return "fsmeta-exec/" + string(kind)
	}
}

func perasDeltaHasConcreteWrites(delta compile.SemanticDelta) bool {
	if len(delta.WriteEffects) == 0 {
		return false
	}
	for _, effect := range delta.WriteEffects {
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return false
			}
		case compile.EffectDelete:
		default:
			return false
		}
	}
	return true
}

func (e *Executor) perasQuotaMode() compile.QuotaMode {
	if e != nil && e.perasCommitter == nil && e.quotas != nil {
		return compile.QuotaModeShared
	}
	return compile.QuotaModeNone
}

func (e *Executor) perasQuotaAllowsVisibleCommit(ctx context.Context, changes []QuotaChange) (bool, error) {
	if e == nil || e.quotas == nil || len(changes) == 0 {
		return true, nil
	}
	admitter, ok := e.quotas.(PerasQuotaAdmitter)
	if !ok {
		return false, nil
	}
	return admitter.AllowPerasVisibleQuota(ctx, changes)
}

func atomicExists(key []byte) *kvrpcpb.AtomicPredicate {
	return &kvrpcpb.AtomicPredicate{Key: cloneBytes(key), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_EXISTS}
}

func atomicNotExists(key []byte) *kvrpcpb.AtomicPredicate {
	return &kvrpcpb.AtomicPredicate{Key: cloneBytes(key), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS}
}

func atomicValueEquals(key, value []byte) *kvrpcpb.AtomicPredicate {
	return &kvrpcpb.AtomicPredicate{
		Key:           cloneBytes(key),
		Kind:          kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS,
		ExpectedValue: cloneBytes(value),
	}
}

// UpdateInode updates mutable inode attributes and applies the size quota delta
// in the same transaction. The parent field is required because quota and
// DirPage invalidation are directory-scoped by parent inode and page token.
func (e *Executor) UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.InodeRecord{}, err
	}
	mount := mountRecord.Identity()
	delta, err := compile.UpdateInode(req, mount, compile.WithQuotaMode(e.perasQuotaMode()))
	if err != nil {
		return fsmeta.InodeRecord{}, err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.InodeRecord{}, err
	}
	plan := delta.Plan
	if !req.SetSize && !req.SetMode && !req.SetUpdatedUnixNs && !req.SetOpaqueAttrs {
		return fsmeta.InodeRecord{}, fsmeta.ErrInvalidRequest
	}
	if updated, committed, err := e.tryPerasVisibleUpdateInode(ctx, delta, plan, mount, req); committed || err != nil {
		if err != nil {
			return fsmeta.InodeRecord{}, err
		}
		e.invalidateDirPages(req.Mount, req.Parent)
		return updated, nil
	}
	var updated fsmeta.InodeRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		dentry, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		dentryValue, err := fsmeta.EncodeDentryValue(dentry)
		if err != nil {
			return err
		}
		if dentry.Inode != req.Inode {
			return fsmeta.ErrInvalidRequest
		}
		inode, ok, err := e.readInode(ctx, mount, req.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if dentry.Type != inode.Type {
			return fsmeta.ErrInvalidValue
		}
		// fsmeta does not maintain an inode->parents reverse index. Updating a
		// hard-linked inode would require invalidating and quota-adjusting every
		// parent, so reject it rather than silently corrupting accounting.
		if inode.LinkCount != 1 {
			return fsmeta.ErrInvalidRequest
		}
		oldInodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		sizeDelta := int64(0)
		if req.SetSize {
			sizeDelta = inodeSizeChange(inode.Size, req.Size)
			inode.Size = req.Size
		}
		if req.SetMode {
			inode.Mode = req.Mode
		}
		if req.SetUpdatedUnixNs {
			inode.UpdatedUnixNs = req.UpdatedUnixNs
		}
		if req.SetOpaqueAttrs {
			inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
		}
		value, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   cloneBytes(plan.MutateKeys[0]),
			Value: value,
		}}
		if sizeDelta != 0 {
			quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      sizeDelta,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		if sizeDelta == 0 || len(mutations) == 1 {
			predicates := []*kvrpcpb.AtomicPredicate{
				atomicValueEquals(plan.ReadKeys[0], dentryValue),
				atomicValueEquals(plan.MutateKeys[0], oldInodeValue),
			}
			if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
				return err
			}
		} else if err := e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		updated = inode
		return nil
	}, delta.Authority); err != nil {
		return fsmeta.InodeRecord{}, err
	}
	e.invalidateDirPages(req.Mount, req.Parent)
	return updated, nil
}

// Lookup returns the dentry record for parent/name. When a negative cache
// is wired (WithNegativeCache), Lookup short-circuits a previously-known
// missing key into ErrNotFound without round-tripping through the runner.
// Misses observed by the runner are recorded so the next Lookup hits the
// visible commit; subsequent Create/Link/Rename for the same key Invalidate the
// entry so the negative memo cannot mask a now-existing dentry.
func (e *Executor) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	plan, err := fsmeta.PlanLookup(req, mountRecord.Identity())
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if e.negCache != nil && e.negCache.Has(plan.PrimaryKey) {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if value, deleted, ok := e.perasOverlayGet(plan.PrimaryKey); ok {
		if deleted {
			return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
		}
		return fsmeta.DecodeDentryValue(value)
	}
	value, ok, err := e.runner.Get(ctx, plan.PrimaryKey, version)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		if e.negCache != nil {
			e.negCache.Remember(plan.PrimaryKey)
		}
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

// invalidateNegative drops cached "missing" memos for every dentry key that
// was just mutated, so the next Lookup re-issues against the runner instead
// of returning a stale ErrNotFound. Safe with a nil cache.
func (e *Executor) invalidateNegative(keys ...[]byte) {
	if e == nil || e.negCache == nil {
		return
	}
	for _, k := range keys {
		if len(k) > 0 {
			e.negCache.Invalidate(k)
		}
	}
}

// dirPageDirectoryKey hashes (mount, parent) into the dirpage cache's
// directory invalidation key. fsmeta.MountID is a string; we use xxhash.Sum64
// to fold it into a uint64 mount slot. Collision probability across reasonable
// mount counts (<= 10K) is ~5e-12, well below "fallback re-warm" tolerance.
func dirPageDirectoryKey(mount fsmeta.MountID, parent fsmeta.InodeID) dirpage.DirectoryKey {
	return dirpage.DirectoryKey{
		Mount:  xxhash.Sum64String(string(mount)),
		Parent: uint64(parent),
	}
}

// dirPageKey includes the caller-visible page cursor. ReadDirPlus cache hits
// are only valid for the exact StartAfter/Limit shape that produced them.
func dirPageKey(mount fsmeta.MountID, parent fsmeta.InodeID, startAfter string, limit uint32) dirpage.PageKey {
	return dirpage.PageKey{
		Mount:      xxhash.Sum64String(string(mount)),
		Parent:     uint64(parent),
		StartAfter: startAfter,
		Limit:      limit,
	}
}

// invalidateDirPages bumps the dirpage cache's epoch for every parent
// directory the just-committed mutation touched. Safe with a nil cache.
// Caller passes (mount, parent) tuples — the helper folds duplicates so
// rename across a single parent doesn't double-bump.
func (e *Executor) invalidateDirPages(mount fsmeta.MountID, parents ...fsmeta.InodeID) {
	if e == nil || e.dirPages == nil {
		return
	}
	seen := make(map[fsmeta.InodeID]struct{}, len(parents))
	for _, p := range parents {
		if p == 0 {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		e.dirPages.Invalidate(dirPageDirectoryKey(mount, p))
	}
}

// ReadDir returns one directory page from a dentry prefix scan.
func (e *Executor) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	plan, err := fsmeta.PlanReadDir(req, mountRecord.Identity())
	if err != nil {
		return nil, err
	}
	var out []fsmeta.DentryRecord
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		var err error
		out, err = e.scanDentries(ctx, plan, version, req.SnapshotVersion == 0)
		return err
	})
	return out, err
}

// ReadDirPlus returns one directory page fused with inode attributes at the
// same snapshot version. This is the first native fsmeta operation that avoids
// client-side dentry scan plus N point reads.
//
// When a dirpage cache is wired and the request omits an explicit
// SnapshotVersion (i.e. the caller is asking for "latest"), Lookup checks
// the cache first against the parent's current invalidation epoch. On hit
// the runner-side dentry scan + N inode BatchGet are skipped; on miss the
// runner path runs as today and the assembled pairs are asynchronously
// materialized into the cache for the next caller.
//
// Snapshot-versioned reads bypass the cache: pages are tagged with the
// "latest" frontier and a stale snapshot-versioned read might disagree
// with the live cache, so we keep that path on the authoritative LSM
// route.
func (e *Executor) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	mount := mountRecord.Identity()
	plan, err := fsmeta.PlanReadDir(req, mount)
	if err != nil {
		return nil, err
	}

	useDirPage := e.dirPages != nil && req.SnapshotVersion == 0
	var pageKey dirpage.PageKey
	var frontier uint64
	if useDirPage {
		pageKey = dirPageKey(req.Mount, req.Parent, req.StartAfter, plan.Limit)
		frontier = e.dirPages.CurrentEpoch(pageKey.Directory())
		if entries, ok := e.dirPages.Lookup(pageKey, frontier); ok {
			if cached, err := decodeDirPageEntries(pageKey, entries); err == nil {
				return cached, nil
			}
		}
	}

	var out []fsmeta.DentryAttrPair
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		includeOverlay := req.SnapshotVersion == 0
		dentries, err := e.scanDentries(ctx, plan, version, includeOverlay)
		if err != nil {
			return err
		}
		if len(dentries) == 0 {
			out = []fsmeta.DentryAttrPair{}
			return nil
		}
		inodeKeys := make([][]byte, 0, len(dentries))
		for _, dentry := range dentries {
			key, err := fsmeta.EncodeInodeKey(mount, dentry.Inode)
			if err != nil {
				return err
			}
			inodeKeys = append(inodeKeys, key)
		}
		inodeValues, err := e.runner.BatchGet(ctx, inodeKeys, version)
		if err != nil {
			return err
		}
		if includeOverlay {
			e.mergePerasOverlayValues(inodeKeys, inodeValues)
		}
		pairs := make([]fsmeta.DentryAttrPair, 0, len(dentries))
		for i, dentry := range dentries {
			value, ok := inodeValues[string(inodeKeys[i])]
			if !ok {
				return fmt.Errorf("%w: inode %d", fsmeta.ErrNotFound, dentry.Inode)
			}
			inode, err := fsmeta.DecodeInodeValue(value)
			if err != nil {
				return err
			}
			if inode.Inode != dentry.Inode {
				return fmt.Errorf("%w: dentry inode=%d value inode=%d", fsmeta.ErrInvalidValue, dentry.Inode, inode.Inode)
			}
			pairs = append(pairs, fsmeta.DentryAttrPair{
				Dentry: dentry,
				Inode:  inode,
			})
		}
		out = pairs
		return nil
	})
	if err != nil {
		return nil, err
	}
	if useDirPage {
		// Materialize is best-effort: if Invalidate fired since we read,
		// the cache drops the write and the next call re-fetches. Encoding must
		// be all-or-none: a partial cached page would be worse than a miss.
		if entries, err := encodeDirPageEntries(out); err == nil {
			_ = e.dirPages.MaterializeAsync(pageKey, frontier, entries)
		}
	}
	return out, nil
}

// encodeDirPageEntries converts assembled DentryAttrPairs into the
// generic dirpage Entry shape. AttrBlob is the encoded InodeRecord; if any
// entry cannot be encoded, the whole materialization is skipped so the cache
// never serves a truncated page as complete.
func encodeDirPageEntries(pairs []fsmeta.DentryAttrPair) ([]dirpage.Entry, error) {
	out := make([]dirpage.Entry, 0, len(pairs))
	for _, p := range pairs {
		blob, err := fsmeta.EncodeInodeValue(p.Inode)
		if err != nil {
			return nil, err
		}
		out = append(out, dirpage.Entry{
			Name:     []byte(p.Dentry.Name),
			Inode:    uint64(p.Dentry.Inode),
			AttrBlob: blob,
		})
	}
	return out, nil
}

// decodeDirPageEntries reverses encodeDirPageEntries. Decode failure on
// any entry treats the whole page set as corrupt and forces a fallback
// to the runner.
func decodeDirPageEntries(key dirpage.PageKey, entries []dirpage.Entry) ([]fsmeta.DentryAttrPair, error) {
	out := make([]fsmeta.DentryAttrPair, 0, len(entries))
	for _, e := range entries {
		inode, err := fsmeta.DecodeInodeValue(e.AttrBlob)
		if err != nil {
			return nil, err
		}
		out = append(out, fsmeta.DentryAttrPair{
			Dentry: fsmeta.DentryRecord{
				Parent: fsmeta.InodeID(key.Parent),
				Name:   string(e.Name),
				Inode:  fsmeta.InodeID(e.Inode),
				Type:   inode.Type,
			},
			Inode: inode,
		})
	}
	return out, nil
}

// GetReadVersion returns an ephemeral MVCC read version. It is intentionally
// cheaper than SnapshotSubtree: no root event is published and no GC-retention
// promise is made.
func (e *Executor) GetReadVersion(ctx context.Context, req fsmeta.ReadVersionRequest) (uint64, error) {
	if req.Mount == "" {
		return 0, fsmeta.ErrInvalidMountID
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return 0, err
	}
	return e.reserveReadVersion(ctx)
}

// SnapshotSubtree reserves a durable MVCC read version for one direct subtree
// root. The service boundary publishes the returned token into rooted truth so
// GC can treat it as a retained snapshot until RetireSnapshotSubtree.
func (e *Executor) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	delta, err := compile.SnapshotSubtree(req, mountRecord.Identity())
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if err := e.flushPerasAuthority(ctx, delta.Authority); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	return fsmeta.SnapshotSubtreeToken{
		Mount:       req.Mount,
		MountKeyID:  mountRecord.MountKeyID,
		RootInode:   req.RootInode,
		ReadVersion: version,
	}, nil
}

func (e *Executor) ResolveSnapshotSubtreeToken(ctx context.Context, token fsmeta.SnapshotSubtreeToken) (fsmeta.SnapshotSubtreeToken, error) {
	record, err := e.resolveKnownMount(ctx, token.Mount)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if token.RootInode == 0 || token.ReadVersion == 0 {
		return fsmeta.SnapshotSubtreeToken{}, fsmeta.ErrInvalidRequest
	}
	token.MountKeyID = record.MountKeyID
	return token, nil
}

// GetQuotaUsage returns the current persisted usage counter for one quota
// subject. Missing usage keys represent zero usage.
func (e *Executor) GetQuotaUsage(ctx context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	if req.Mount == "" {
		return fsmeta.UsageRecord{}, fsmeta.ErrInvalidMountID
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.UsageRecord{}, err
	}
	key, err := fsmeta.EncodeUsageKey(mountRecord.Identity(), req.Scope)
	if err != nil {
		return fsmeta.UsageRecord{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.UsageRecord{}, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil {
		return fsmeta.UsageRecord{}, err
	}
	if !ok {
		return fsmeta.UsageRecord{}, nil
	}
	return fsmeta.DecodeUsageValue(value)
}

func (e *Executor) scanDentries(ctx context.Context, plan fsmeta.OperationPlan, version uint64, includeOverlay bool) ([]fsmeta.DentryRecord, error) {
	kvs, err := e.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
	if err != nil {
		return nil, err
	}
	prefix := plan.ReadPrefixes[0]
	if includeOverlay {
		kvs = e.mergePerasOverlayScan(kvs, plan.StartKey, plan.Limit)
	}
	out := make([]fsmeta.DentryRecord, 0, len(kvs))
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, prefix) {
			break
		}
		record, err := fsmeta.DecodeDentryValue(kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

// Link creates a second dentry for an existing non-directory inode and bumps
// the inode link count in the same transaction.
func (e *Executor) Link(ctx context.Context, req fsmeta.LinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	delta, err := compile.Link(req, mount, compile.WithQuotaMode(e.perasQuotaMode()))
	if err != nil {
		return err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryPerasVisibleLink(ctx, delta, plan, mount, req); committed || err != nil {
		if err != nil {
			return err
		}
		e.invalidateNegative(plan.ReadKeys[1])
		e.invalidateDirPages(req.Mount, req.ToParent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		sourceDentryValue, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		if record.Type == fsmeta.InodeTypeDirectory {
			return fsmeta.ErrInvalidRequest
		}
		if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
			return fsmeta.ErrExists
		} else if !errors.Is(err, fsmeta.ErrNotFound) {
			return err
		}
		inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if inode.Type == fsmeta.InodeTypeDirectory {
			return fsmeta.ErrInvalidRequest
		}
		if inode.LinkCount == ^uint32(0) {
			return fsmeta.ErrInvalidRequest
		}
		if inode.LinkCount == 0 {
			inode.LinkCount = 1
		}
		oldInodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		inode.LinkCount++

		dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
			Parent: req.ToParent,
			Name:   req.ToName,
			Inode:  record.Inode,
			Type:   record.Type,
		})
		if err != nil {
			return err
		}
		inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{
			{
				Op:                kvrpcpb.Mutation_Put,
				Key:               cloneBytes(plan.ReadKeys[1]),
				Value:             dentryValue,
				AssertionNotExist: true,
			},
			{
				Op:    kvrpcpb.Mutation_Put,
				Key:   inodeKey,
				Value: inodeValue,
			},
		}
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.ToParent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}}, startVersion)
		if err != nil {
			return err
		}
		mutations = append(mutations, quotaMutations...)
		if len(quotaMutations) == 0 {
			// Link is safe on 1PC only when the source dentry and inode still
			// equal the records read by this attempt. These value predicates are
			// the correctness boundary that prevents overwriting a concurrent
			// UpdateInode with an older inode body.
			predicates := []*kvrpcpb.AtomicPredicate{
				atomicValueEquals(plan.ReadKeys[0], sourceDentryValue),
				atomicNotExists(plan.ReadKeys[1]),
				atomicValueEquals(inodeKey, oldInodeValue),
			}
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	// Link writes a fresh dentry at ReadKeys[1]; drop any negative memo
	// and bump the destination parent's dirpage epoch so the new dentry
	// shows up on the next ReadDirPlus.
	e.invalidateNegative(plan.ReadKeys[1])
	e.invalidateDirPages(req.Mount, req.ToParent)
	return nil
}

// Unlink removes one dentry, decrements its inode link count, and deletes the
// inode record when the last dentry goes away.
func (e *Executor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	delta, err := compile.Unlink(req, mount, compile.WithQuotaMode(e.perasQuotaMode()))
	if err != nil {
		return err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryPerasVisibleUnlink(ctx, delta, plan, mount, req); committed || err != nil {
		if err != nil {
			return err
		}
		e.invalidateNegative(plan.MutateKeys[0])
		e.invalidateDirPages(req.Mount, req.Parent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		dentryValue, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		}}
		predicates := []*kvrpcpb.AtomicPredicate{atomicValueEquals(plan.PrimaryKey, dentryValue)}
		if inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion); err != nil {
			return err
		} else if ok {
			inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
			if err != nil {
				return err
			}
			oldInodeValue, err := fsmeta.EncodeInodeValue(inode)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(inodeKey, oldInodeValue))
			if inode.LinkCount <= 1 {
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: inodeKey})
			} else {
				inode.LinkCount--
				inodeValue, err := fsmeta.EncodeInodeValue(inode)
				if err != nil {
					return err
				}
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: inodeKey, Value: inodeValue})
			}
			quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      -inodeSizeDelta(inode.Size),
				Inodes:     -1,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		if len(mutations) == len(predicates) {
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	// Unlink removed the dentry; the next Lookup must observe ErrNotFound
	// from the runner instead of any prior positive memo (we do not cache
	// hits today, but Invalidate is also the right thing for any future
	// hit-cache layering). Bump the parent's dirpage epoch so a cached
	// ReadDirPlus does not still surface the dentry.
	e.invalidateNegative(plan.MutateKeys[0])
	e.invalidateDirPages(req.Mount, req.Parent)
	return nil
}

// OpenWriteSession records one exclusive writer lease for an inode. It writes
// both a session-id key and an inode-owner key so concurrent opens for the same
// inode conflict on one Percolator key.
func (e *Executor) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	mount := mountRecord.Identity()
	delta, err := compile.OpenWriteSession(req, mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	plan := delta.Plan
	if req.TTL <= 0 {
		return fsmeta.SessionRecord{}, fsmeta.ErrInvalidRequest
	}
	if record, committed, err := e.tryPerasVisibleOpenWriteSession(ctx, delta, plan, mount, req); committed || err != nil {
		if err != nil {
			return fsmeta.SessionRecord{}, err
		}
		return record, nil
	}
	var record fsmeta.SessionRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		inode, ok, err := e.readInode(ctx, mount, req.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if inode.Type != fsmeta.InodeTypeFile {
			return fsmeta.ErrInvalidRequest
		}
		inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		nowTime := e.clock()
		expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
		if !ok {
			return fsmeta.ErrInvalidRequest
		}
		candidate := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
		now := nowTime.UnixNano()
		predicates := make([]*kvrpcpb.AtomicPredicate, 0, 4)
		if existing, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[1], startVersion); err != nil {
			return err
		} else if ok && sessionLive(existing, now) {
			return fsmeta.ErrExists
		} else if ok {
			existingValue, err := fsmeta.EncodeSessionValue(existing)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(plan.ReadKeys[1], existingValue))
		} else {
			predicates = append(predicates, atomicNotExists(plan.ReadKeys[1]))
		}
		mutations := make([]*kvrpcpb.Mutation, 0, 3)
		if owner, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[2], startVersion); err != nil {
			return err
		} else if ok {
			if sessionLive(owner, now) {
				return fsmeta.ErrExists
			}
			ownerValue, err := fsmeta.EncodeSessionValue(owner)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(plan.ReadKeys[2], ownerValue))
			staleSessionKey, err := fsmeta.EncodeSessionKey(mount, owner.Inode, owner.Session)
			if err != nil {
				return err
			}
			if string(staleSessionKey) != string(plan.ReadKeys[1]) {
				if value, ok, err := e.runner.Get(ctx, staleSessionKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, ownerValue) {
					predicates = append(predicates, atomicValueEquals(staleSessionKey, ownerValue))
					mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: staleSessionKey})
				}
			}
		} else {
			predicates = append(predicates, atomicNotExists(plan.ReadKeys[2]))
		}
		value, err := fsmeta.EncodeSessionValue(candidate)
		if err != nil {
			return err
		}
		mutations = append(mutations,
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[0]), Value: value},
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[1]), Value: value},
		)
		predicates = append(predicates, atomicValueEquals(inodeKey, inodeValue))
		// Open is a value-sensitive admission path: the session-id key, owner
		// key, inode key, and any stale cleanup key must still match the values
		// read above. Value predicates make the 1PC attempt a real CAS instead
		// of an existence-only overwrite.
		if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		record = candidate
		return nil
	}, delta.Authority); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return record, nil
}

// HeartbeatWriteSession extends a live writer lease. Both session records must
// agree, otherwise the session is considered lost and the caller must reopen.
func (e *Executor) HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	mount := mountRecord.Identity()
	delta, err := compile.HeartbeatWriteSession(req, mount)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	plan := delta.Plan
	if req.TTL <= 0 {
		return fsmeta.SessionRecord{}, fsmeta.ErrInvalidRequest
	}
	if record, committed, err := e.tryPerasVisibleHeartbeatWriteSession(ctx, delta, plan, req); committed || err != nil {
		if err != nil {
			return fsmeta.SessionRecord{}, err
		}
		return record, nil
	}
	var record fsmeta.SessionRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		nowTime := e.clock()
		expiresUnixNs, ok := sessionExpiryUnixNs(nowTime, req.TTL)
		if !ok {
			return fsmeta.ErrInvalidRequest
		}
		candidate := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: expiresUnixNs}
		now := nowTime.UnixNano()
		session, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if !ok || !sessionLive(session, now) || session.Inode != req.Inode {
			return fsmeta.ErrNotFound
		}
		sessionValue, err := fsmeta.EncodeSessionValue(session)
		if err != nil {
			return err
		}
		owner, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[1], startVersion)
		if err != nil {
			return err
		}
		if !ok || !sessionLive(owner, now) || owner.Session != req.Session || owner.Inode != req.Inode {
			return fsmeta.ErrNotFound
		}
		ownerValue, err := fsmeta.EncodeSessionValue(owner)
		if err != nil {
			return err
		}
		value, err := fsmeta.EncodeSessionValue(candidate)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[0]), Value: value},
			{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[1]), Value: value},
		}
		predicates := []*kvrpcpb.AtomicPredicate{
			atomicValueEquals(plan.ReadKeys[0], sessionValue),
			atomicValueEquals(plan.ReadKeys[1], ownerValue),
		}
		if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		record = candidate
		return nil
	}, delta.Authority); err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return record, nil
}

// CloseWriteSession releases one writer lease. It deletes the owner key only
// when it still points at the closing session.
func (e *Executor) CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	delta, err := compile.CloseWriteSession(req, mount)
	if err != nil {
		return err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryPerasVisibleCloseWriteSession(ctx, delta, plan, mount, req); committed || err != nil {
		return err
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		session, ok, err := e.readSessionByKey(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if session.Inode != req.Inode {
			return fsmeta.ErrNotFound
		}
		sessionValue, err := fsmeta.EncodeSessionValue(session)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Delete, Key: cloneBytes(plan.MutateKeys[0])}}
		predicates := []*kvrpcpb.AtomicPredicate{atomicValueEquals(plan.ReadKeys[0], sessionValue)}
		ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, session.Inode)
		if err != nil {
			return err
		}
		if owner, ok, err := e.readSessionByKey(ctx, ownerKey, startVersion); err != nil {
			return err
		} else if ok && owner.Session == req.Session && owner.Inode == session.Inode {
			ownerValue, err := fsmeta.EncodeSessionValue(owner)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(ownerKey, ownerValue))
			mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: ownerKey})
		}
		return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	return nil
}

// ExpireWriteSessions removes stale session-id and inode-owner records for one
// mount. It is a bounded maintenance primitive; callers should repeat until
// Expired is zero when draining a large backlog.
func (e *Executor) ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	mount := mountRecord.Identity()
	delta, err := compile.ExpireWriteSessions(req, mount)
	if err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	plan := delta.Plan
	now := e.clock().UnixNano()
	var expired uint64
	// Expiration is base-LSM maintenance, but it still writes fsmeta session
	// keys. Drain the active Peras authority before mutating so storage-side
	// authority fences stay fail-closed for callers that bypass this executor.
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		deletes := make(map[string][]byte)
		type expiredSessionKey struct {
			inode   fsmeta.InodeID
			session fsmeta.SessionID
		}
		expiredSessions := make(map[expiredSessionKey]struct{})
		remaining := plan.Limit
		for _, scanPrefix := range plan.ReadPrefixes {
			if remaining == 0 {
				break
			}
			kvs, err := e.runner.Scan(ctx, scanPrefix, remaining, startVersion)
			if err != nil {
				return err
			}
			var matched uint32
			for _, kv := range kvs {
				if !bytes.HasPrefix(kv.Key, scanPrefix) {
					break
				}
				matched++
				kind, err := fsmeta.KeyKindOf(kv.Key)
				if err != nil {
					return err
				}
				if kind != fsmeta.KeyKindSession {
					continue
				}
				record, err := fsmeta.DecodeSessionValue(kv.Value)
				if err != nil {
					return err
				}
				if sessionLive(record, now) {
					continue
				}
				deletes[string(kv.Key)] = cloneBytes(kv.Key)
				sessionKey, err := fsmeta.EncodeSessionKey(mount, record.Inode, record.Session)
				if err != nil {
					return err
				}
				ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, record.Inode)
				if err != nil {
					return err
				}
				if value, ok, err := e.runner.Get(ctx, sessionKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, kv.Value) {
					deletes[string(sessionKey)] = sessionKey
					expiredSessions[expiredSessionKey{inode: record.Inode, session: record.Session}] = struct{}{}
				}
				if value, ok, err := e.runner.Get(ctx, ownerKey, startVersion); err != nil {
					return err
				} else if ok && bytes.Equal(value, kv.Value) {
					deletes[string(ownerKey)] = ownerKey
				}
			}
			remaining -= matched
		}
		if len(deletes) == 0 {
			expired = 0
			return nil
		}
		keys := make([]string, 0, len(deletes))
		for key := range deletes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		mutations := make([]*kvrpcpb.Mutation, 0, len(deletes))
		for _, key := range keys {
			mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: deletes[key]})
		}
		primary := deletes[keys[0]]
		if _, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL); err != nil {
			return err
		}
		expired = uint64(len(expiredSessions))
		return nil
	}, delta.Authority); err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, err
	}
	return fsmeta.ExpireWriteSessionsResult{Expired: expired}, nil
}

type renameMove struct {
	mount      fsmeta.MountID
	identity   fsmeta.MountIdentity
	fromParent fsmeta.InodeID
	fromName   string
	toParent   fsmeta.InodeID
	toName     string
}

func renameMoveFromRename(req fsmeta.RenameRequest, identity fsmeta.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

func renameMoveFromRenameSubtree(req fsmeta.RenameSubtreeRequest, identity fsmeta.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

// Rename moves one dentry inside the same subtree authority. It is deliberately
// a data-plane transaction: no rooted handoff is published, so common staged
// publish paths do not serialize through the control plane.
func (e *Executor) Rename(ctx context.Context, req fsmeta.RenameRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	delta, err := compile.Rename(req, mount)
	if err != nil {
		return err
	}
	if err := e.requireSameAuthority(ctx, req.Mount, req.FromParent, req.ToParent); err != nil {
		return err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	move := renameMoveFromRename(req, mount)
	var movedSize uint64
	var movedInode bool
	if committed, err := e.tryPerasVisibleRename(ctx, delta, plan, move); committed || err != nil {
		if err != nil {
			return err
		}
		e.invalidateNegative(plan.ReadKeys...)
		e.invalidateNegative(plan.MutateKeys...)
		e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, err := e.prepareRenameMutations(ctx, plan, move, startVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if len(mutations) == 2 {
			predicates := []*kvrpcpb.AtomicPredicate{atomicExists(plan.ReadKeys[0]), atomicNotExists(plan.ReadKeys[1])}
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	e.invalidateNegative(plan.ReadKeys...)
	e.invalidateNegative(plan.MutateKeys...)
	e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
	return nil
}

// RenameSubtree moves the subtree root dentry from source to destination.
// Descendants follow through inode parent links rather than key rewrites.
func (e *Executor) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	delta, err := compile.RenameSubtree(req, mount)
	if err != nil {
		return err
	}
	if err := e.admitPerasAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	authorityRoot := mountRecord.RootInode
	if e.subtrees != nil && authorityRoot == 0 {
		return fsmeta.ErrInvalidInodeID
	}
	var movedSize uint64
	var movedInode bool
	var committedAt uint64
	var handoffStarted bool
	move := renameMoveFromRenameSubtree(req, mount)
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, err := e.prepareRenameMutations(ctx, plan, move, startVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if err := e.startSubtreeHandoff(ctx, req.Mount, authorityRoot, commitVersion); err != nil {
			return err
		}
		handoffStarted = true
		actualCommitVersion, mutationErr := e.runner.MutateAtCommit(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, e.lockTTL)
		// Subtree handoff start publishes a rooted predecessor frontier before the
		// data mutation runs. That external frontier must be the same commit_ts
		// used by the data transaction; otherwise concurrent handoffs can observe a
		// later completed frontier and reject the older pending handoff.
		// Once StartSubtreeHandoff is rooted, a Mutate error may still be
		// ambiguous with respect to primary commit. Complete closes the rooted
		// pending state; at worst this advances an empty era rather than leaving
		// an unrecoverable handoff.
		completeErr := e.completeSubtreeHandoff(ctx, req.Mount, authorityRoot, actualCommitVersion)
		if mutationErr != nil {
			if completeErr != nil {
				return errors.Join(mutationErr, fmt.Errorf("complete subtree handoff: %w", completeErr))
			}
			return mutationErr
		}
		if completeErr != nil {
			return completeErr
		}
		committedAt = actualCommitVersion
		return nil
	}, delta.Authority); err != nil {
		return err
	}
	if handoffStarted && committedAt == 0 {
		return errSubtreeHandoffWithoutFrontier
	}
	// Only the subtree root dentry moves; descendants follow inode parent links.
	// Invalidate both old and new dentry keys plus the two parent directory
	// epochs so negative and materialized directory-page caches cannot serve the
	// pre-rename view.
	e.invalidateNegative(plan.ReadKeys...)
	e.invalidateNegative(plan.MutateKeys...)
	e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
	return nil
}

func (e *Executor) prepareRenameMutations(ctx context.Context, plan fsmeta.OperationPlan, move renameMove, startVersion uint64, movedSize *uint64, movedInode *bool) ([]*kvrpcpb.Mutation, error) {
	record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
	if err != nil {
		return nil, err
	}
	if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
		return nil, fsmeta.ErrExists
	} else if !errors.Is(err, fsmeta.ErrNotFound) {
		return nil, err
	}
	record.Parent = move.toParent
	record.Name = move.toName
	value, err := fsmeta.EncodeDentryValue(record)
	if err != nil {
		return nil, err
	}
	*movedSize = 0
	*movedInode = false
	if inode, ok, err := e.readInode(ctx, move.identity, record.Inode, startVersion); err != nil {
		return nil, err
	} else if ok {
		*movedSize = inode.Size
		*movedInode = true
	}
	mutations := []*kvrpcpb.Mutation{
		{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		},
		{
			Op:                kvrpcpb.Mutation_Put,
			Key:               cloneBytes(plan.MutateKeys[1]),
			Value:             value,
			AssertionNotExist: true,
		},
	}
	if *movedInode {
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{
			{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(*movedSize), Inodes: -1},
			{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(*movedSize), Inodes: 1},
		}, startVersion)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, quotaMutations...)
	}
	return mutations, nil
}

func (e *Executor) startSubtreeHandoff(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	if e == nil || e.subtrees == nil || mount == "" || root == 0 || frontier == 0 {
		return nil
	}
	return e.subtrees.StartSubtreeHandoff(ctx, mount, root, frontier)
}

func (e *Executor) completeSubtreeHandoff(ctx context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	if e == nil || e.subtrees == nil || mount == "" || root == 0 || frontier == 0 {
		return nil
	}
	return e.subtrees.CompleteSubtreeHandoff(ctx, mount, root, frontier)
}

func (e *Executor) requireActiveMount(ctx context.Context, mount fsmeta.MountID) error {
	_, err := e.resolveActiveMount(ctx, mount)
	return err
}

func (e *Executor) requireSameAuthority(ctx context.Context, mount fsmeta.MountID, fromParent, toParent fsmeta.InodeID) error {
	if e == nil || e.authorities == nil {
		return nil
	}
	same, err := e.authorities.SameAuthority(ctx, mount, fromParent, toParent)
	if err != nil {
		return err
	}
	if !same {
		return fsmeta.ErrCrossAuthorityRename
	}
	return nil
}

func (e *Executor) resolveActiveMount(ctx context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	record, err := e.resolveKnownMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.Retired {
		return MountAdmission{}, fsmeta.ErrMountRetired
	}
	return record, nil
}

func (e *Executor) resolveKnownMount(ctx context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	if e == nil || e.mounts == nil {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	record, err := e.mounts.ResolveMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.MountID == "" {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	if record.MountKeyID == 0 {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	return record, nil
}

func (e *Executor) reserveQuota(ctx context.Context, changes []QuotaChange, startVersion uint64) ([]*kvrpcpb.Mutation, error) {
	if e == nil || e.quotas == nil {
		return nil, nil
	}
	return e.quotas.ReserveQuota(ctx, e.runner, changes, startVersion)
}

func (e *Executor) reserveReadVersion(ctx context.Context) (uint64, error) {
	return e.reserveTimestampWithRetry(ctx, 1, maxReadContentionRetries, &e.readRetriesTotal, &e.readRetryExhaustedTotal)
}

func (e *Executor) readVersion(ctx context.Context, snapshotVersion uint64) (uint64, error) {
	if snapshotVersion != 0 {
		return snapshotVersion, nil
	}
	return e.reserveReadVersion(ctx)
}

// reserveTxnVersions reserves start_ts plus a speculative commit_ts in one TSO
// hop. AtomicMutate and in-memory runners use the speculative commit version.
// The real raftstore runner obtains commit_ts after prewrite for regular 2PC,
// which is the strict Percolator boundary under read/write contention.
//
// When a path does use the speculative commit_ts, two server-side safety nets
// keep pre-allocation from silently violating snapshot isolation:
//
//  1. When a concurrent reader at start_ts > our commit_ts encounters our
//     prewrite lock, it pushes lock.MinCommitTs = reader_start_ts + 1 via
//     CheckTxnStatus (see txn/percolator/txn.go: CallerStartTs handling).
//  2. commitKey rejects the commit with keyErrorCommitTsExpired when
//     lock.MinCommitTs > commitVersion (see txn/percolator/txn.go:373-375).
//
// Together these force a retry-with-fresh-ts under contention: incorrect
// speculative commit_ts is detected at commit time, never silently accepted.
// CommitTsExpired is retried transparently by withTxnRetry below.
func (e *Executor) reserveTxnVersions(ctx context.Context) (uint64, uint64, error) {
	startVersion, err := e.reserveTimestampWithRetry(ctx, 2, maxTxnContentionRetries, &e.txnRetriesTotal, &e.txnRetryExhaustedTotal)
	if err != nil {
		return 0, 0, err
	}
	return startVersion, startVersion + 1, nil
}

func (e *Executor) reserveTimestampWithRetry(ctx context.Context, count uint64, maxRetries int, retryTotal, exhaustedTotal *atomic.Uint64) (uint64, error) {
	var last error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		version, err := e.runner.ReserveTimestamp(ctx, count)
		if err == nil {
			return version, nil
		}
		if !nokverrors.Retryable(err) {
			return 0, err
		}
		last = err
		if attempt == maxRetries {
			if exhaustedTotal != nil {
				exhaustedTotal.Add(1)
			}
			break
		}
		if retryTotal != nil {
			retryTotal.Add(1)
		}
		// Coordinator duty handoff can reject a timestamp request with stale
		// evidence before any fsmeta mutation has started. Retrying here keeps
		// transient authority churn below the namespace API boundary.
		if err := waitTxnContentionRetryDelay(ctx, txnContentionRetryDelay(attempt)); err != nil {
			return 0, err
		}
	}
	return 0, last
}

func (e *Executor) withTxnRetry(ctx context.Context, run func(startVersion, commitVersion uint64) error, scopes ...compile.AuthorityScope) error {
	if err := e.drainPerasAuthority(ctx, scopes...); err != nil {
		return err
	}
	return e.withTxnRetryNoPerasFlush(ctx, run)
}

func (e *Executor) withTxnRetryNoPerasFlush(ctx context.Context, run func(startVersion, commitVersion uint64) error) error {
	var last error
	started := time.Now()
	for attempt := 0; ; attempt++ {
		startVersion, commitVersion, err := e.reserveTxnVersions(ctx)
		if err != nil {
			return err
		}
		err = run(startVersion, commitVersion)
		if err == nil {
			return nil
		}
		if !isRetryableTxnAttempt(err) {
			return translateMutateError(err)
		}
		last = err
		if !canRetryTxnAttempt(attempt, started, err, e.lockTTL) {
			e.txnRetryExhaustedTotal.Add(1)
			break
		}
		// A live Percolator lock or a coordinator/region route refresh can race
		// with the same semantic fsmeta operation. Retrying at this boundary
		// keeps transient MVCC and route churn below the API contract.
		e.txnRetriesTotal.Add(1)
		delay := txnContentionRetryDelay(attempt)
		if budget := txnRetryBudget(err, e.lockTTL); budget > 0 {
			remaining := budget - time.Since(started)
			if remaining <= 0 {
				e.txnRetryExhaustedTotal.Add(1)
				break
			}
			delay = min(delay, remaining)
		}
		if err := waitTxnContentionRetryDelay(ctx, delay); err != nil {
			return err
		}
	}
	return translateMutateError(last)
}

func (e *Executor) withReadRetry(ctx context.Context, snapshotVersion uint64, run func(version uint64) error) error {
	var last error
	for attempt := 0; attempt <= maxReadContentionRetries; attempt++ {
		version, err := e.readVersion(ctx, snapshotVersion)
		if err != nil {
			return err
		}
		err = run(version)
		if err == nil {
			return nil
		}
		if !isRetryableReadAttempt(err) {
			return err
		}
		last = err
		if attempt == maxReadContentionRetries {
			e.readRetryExhaustedTotal.Add(1)
			break
		}
		// ReadDir and ReadDirPlus may race with live Percolator locks or region
		// route refresh. Retrying keeps the external API at the fsmeta level
		// instead of leaking transient storage details to callers.
		e.readRetriesTotal.Add(1)
		if err := waitTxnContentionRetryDelay(ctx, txnContentionRetryDelay(attempt)); err != nil {
			return err
		}
	}
	return last
}

func canRetryTxnAttempt(attempt int, started time.Time, err error, fallbackLockTTL uint64) bool {
	if budget := txnRetryBudget(err, fallbackLockTTL); budget > 0 {
		return time.Since(started) < budget
	}
	return attempt < maxTxnContentionRetries
}

func txnRetryBudget(err error, fallbackLockTTL uint64) time.Duration {
	switch {
	case nokverrors.IsKind(err, nokverrors.KindLockConflict):
	case nokverrors.IsKind(err, nokverrors.KindRetryable):
		// Percolator uses Retryable for a dead start_ts, for example when
		// commit finds that the prewrite lock was already rolled back. The
		// fsmeta semantic operation can safely re-read and re-plan, but under
		// raft/store congestion it needs the same bounded liveness window as a
		// visible live-lock wait.
	default:
		return 0
	}
	ttlMillis := txnLockTTLMillis(err)
	if ttlMillis == 0 {
		ttlMillis = fallbackLockTTL
	}
	if ttlMillis == 0 {
		return txnContentionRetryMaxBackoff
	}
	budget := time.Duration(ttlMillis) * time.Millisecond
	if budget <= 0 || budget > maxTxnLockRetryBudget {
		return maxTxnLockRetryBudget
	}
	return budget + txnContentionRetryMaxBackoff
}

func txnLockTTLMillis(err error) uint64 {
	var carrier nokverrors.KeyErrorCarrier
	if !errors.As(err, &carrier) {
		return 0
	}
	var maxTTL uint64
	for _, keyErr := range carrier.KeyErrors() {
		if ttl := keyErr.GetLocked().GetLockTtl(); ttl > maxTTL {
			maxTTL = ttl
		}
	}
	return maxTTL
}

func txnContentionRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return txnContentionRetryBaseBackoff
	}
	if attempt >= 7 {
		return txnContentionRetryMaxBackoff
	}
	return min(txnContentionRetryBaseBackoff<<attempt, txnContentionRetryMaxBackoff)
}

func waitTxnContentionRetryDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneMutations(in []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	out := make([]*kvrpcpb.Mutation, 0, len(in))
	for _, mut := range in {
		if mut == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, &kvrpcpb.Mutation{
			Op:                mut.GetOp(),
			Key:               cloneBytes(mut.GetKey()),
			Value:             cloneBytes(mut.GetValue()),
			AssertionNotExist: mut.GetAssertionNotExist(),
			ExpiresAt:         mut.GetExpiresAt(),
		})
	}
	return out
}

func cloneAtomicPredicates(in []*kvrpcpb.AtomicPredicate) []*kvrpcpb.AtomicPredicate {
	out := make([]*kvrpcpb.AtomicPredicate, 0, len(in))
	for _, pred := range in {
		if pred == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, &kvrpcpb.AtomicPredicate{
			Key:           cloneBytes(pred.GetKey()),
			Kind:          pred.GetKind(),
			ReadVersion:   pred.GetReadVersion(),
			ExpectedValue: cloneBytes(pred.GetExpectedValue()),
		})
	}
	return out
}

func (e *Executor) readDentry(ctx context.Context, key []byte, version uint64) (fsmeta.DentryRecord, error) {
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

func (e *Executor) readInode(ctx context.Context, mount fsmeta.MountIdentity, inodeID fsmeta.InodeID, version uint64) (fsmeta.InodeRecord, bool, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil || !ok {
		return fsmeta.InodeRecord{}, ok, err
	}
	inode, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (e *Executor) readSessionByKey(ctx context.Context, key []byte, version uint64) (fsmeta.SessionRecord, bool, error) {
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil || !ok {
		return fsmeta.SessionRecord{}, ok, err
	}
	record, err := fsmeta.DecodeSessionValue(value)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	return record, true, nil
}

func sessionExpiryUnixNs(now time.Time, ttl time.Duration) (int64, bool) {
	if ttl <= 0 {
		return 0, false
	}
	const maxInt64 = int64(1<<63 - 1)
	nowUnixNs := now.UnixNano()
	ttlUnixNs := int64(ttl)
	if nowUnixNs > maxInt64-ttlUnixNs {
		return 0, false
	}
	return nowUnixNs + ttlUnixNs, true
}

func (e *Executor) clock() time.Time {
	if e != nil && e.now != nil {
		return e.now()
	}
	return time.Now()
}

func sessionLive(record fsmeta.SessionRecord, nowUnixNs int64) bool {
	return record.ExpiresUnixNs > nowUnixNs
}

func translateMutateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, fsmeta.ErrExists) {
		return err
	}
	if nokverrors.HasKeyErrorKind(err, nokverrors.KindAlreadyExists) {
		return fmt.Errorf("%w: %v", fsmeta.ErrExists, err)
	}
	return err
}

func isRetryableTxnContention(err error) bool {
	return nokverrors.IsTxnContention(err)
}

func isRetryableTxnAttempt(err error) bool {
	return isRetryableTxnContention(err) || isRetryableRouteRefresh(err)
}

func isRetryableReadAttempt(err error) bool {
	return isRetryableTxnContention(err) || isRetryableRouteRefresh(err)
}

func isRetryableRouteRefresh(err error) bool {
	switch nokverrors.KindOf(err) {
	case nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindStaleEpoch:
		return true
	default:
		return false
	}
}
