// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
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

// AtomicMutateOnePhase is a raw one-phase mutation capability. It is not enough
// by itself for fsmeta semantic execution: caller-allocated commit timestamps
// can otherwise let a read at a later timestamp race ahead of a delayed 1PC
// apply and observe a fractured directory/inode view.
type AtomicMutateOnePhase interface {
	TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (handled bool, err error)
}

// ReadOrderedAtomicMutateOnePhase is the one-phase mutation contract the
// fsmeta executor may consume. Implementations must guarantee that a read at
// version T cannot miss any successful 1PC write whose commit version is <= T
// merely because the write had not reached the storage apply boundary yet.
type ReadOrderedAtomicMutateOnePhase interface {
	AtomicMutateOnePhase
	AtomicMutatePreservesReadOrder() bool
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

// PerasCommitter is the experimental, opt-in Peras visible commit boundary.
// Success replaces the ordinary Percolator/Raft commit for this fsmeta
// operation, so errors are returned and never silently fall back after the
// holder overlay may already include the operation.
type PerasCommitter interface {
	SubmitVisible(context.Context, fsperas.OperationID, compile.MaterializedOp, fsperas.AdmissionFunc) (fsperas.VisibleAck, error)
}

type PerasOverlayReader interface {
	GetPerasOverlay(key []byte) (value []byte, deleted bool, ok bool)
	// GetPerasOverlayView returns overlay-owned bytes. Callers must not mutate
	// the returned value.
	GetPerasOverlayView(key []byte) (value []byte, deleted bool, ok bool)
	ScanPerasOverlay(start []byte, limit uint32) []fsperas.OverlayKV
}

type PerasOverlayReadSnapshotReader interface {
	CapturePerasOverlayRead() (overlayGeneration, sealedGeneration uint64)
	GetPerasOverlayViewAt(overlayGeneration, sealedGeneration uint64, key []byte) (value []byte, deleted bool, ok bool)
	ScanPerasDirectoryAt(overlayGeneration, sealedGeneration uint64, prefix, start []byte, limit uint32) []fsperas.OverlayKV
}

type PerasDirectoryOverlayReader interface {
	ScanPerasDirectory(prefix, start []byte, limit uint32) []fsperas.OverlayKV
}

type PerasDirectoryOverlayPresence interface {
	HasPerasDirectory(prefix []byte) bool
}

type PerasVisibleDirectoryPresence interface {
	HasPerasVisibleDirectory(prefix []byte) bool
}

type PerasDirectoryCacheFrontier interface {
	PerasDirectoryCacheFrontier(prefix []byte) uint64
}

// PerasSnapshotCapturer records the installed Peras overlay visible at an MVCC
// snapshot token so later snapshot reads do not consult live overlay state.
type PerasSnapshotCapturer interface {
	CapturePerasSnapshot(version uint64) error
}

// VisibleSnapshotCapturer lets a runtime capture visible Peras state
// without forcing an authority flush. Runtimes return captured=false when the
// snapshot cannot be made durable at the visible boundary.
type VisibleSnapshotCapturer interface {
	CapturePerasVisibleSnapshot(context.Context, uint64, compile.AuthorityScope) (fsmeta.VisibleSnapshotCapture, bool, error)
}

// PerasSnapshotOverlayReader serves a captured Peras overlay for a snapshot
// version. It is intentionally separate from the live overlay reader.
type PerasSnapshotOverlayReader interface {
	GetPerasSnapshotOverlayView(version uint64, key []byte) (value []byte, deleted bool, ok bool)
	ScanPerasSnapshotDirectory(version uint64, prefix, start []byte, limit uint32) []fsperas.OverlayKV
	HasPerasSnapshotDirectory(version uint64, prefix []byte) bool
}

type perasSnapshotRetirer interface {
	RetirePerasSnapshot(version uint64)
}

type PerasFlusher interface {
	FlushDurable(context.Context) error
}

type PerasAuthorityFlusher interface {
	FlushAuthority(context.Context, compile.AuthorityScope) error
}

type PerasAuthorityDrainer interface {
	DrainAuthority(context.Context, fsperas.AuthorityRetirer, ...compile.AuthorityScope) error
}

type VisibleQuotaAdmitter interface {
	AllowVisibleQuota(context.Context, []QuotaChange) (bool, error)
}

// NegativeCache is the dentry-miss memo surface used by Lookup.
type NegativeCache interface {
	Has([]byte) bool
	Remember([]byte)
	Invalidate([]byte)
}

type negativeCacheClearer interface {
	Clear()
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
	perasClientID           string
	negCache                NegativeCache
	dirPages                DirPageCache
	dirPagePerasMu          sync.Mutex
	dirPagePerasFrontier    map[dirpage.DirectoryKey]uint64
	lockTTL                 uint64
	now                     func() time.Time
	readRetriesTotal        atomic.Uint64
	readRetryExhaustedTotal atomic.Uint64
	txnRetriesTotal         atomic.Uint64
	txnRetryExhaustedTotal  atomic.Uint64
	createTotal             atomic.Uint64
	perasAdmission          perasAdmissionCounters
	perasVisible            perasVisibleCounters
	perasDirectoryRead      perasDirectoryReadCounters
	perasSeq                atomic.Uint64
	atomicOnePhase          map[fsmeta.OperationKind]*atomicOnePhaseCounters
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
// Lookup checks Peras overlay first, then Has on the dentry primary key before
// consulting the runner; misses are recorded via Remember; mutating ops call
// Invalidate on the touched dentry keys after a successful commit.
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
		perasClientID:  newPerasClientID(),
		atomicOnePhase: newAtomicOnePhaseCounters(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(executor)
		}
	}
	if executor.perasCommitter != nil {
		executor.clearNegativeCache()
	}
	return executor, nil
}
