package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const defaultLockTTL uint64 = 3000
const maxCommitTsExpiredRetries = 3

// KV is the minimal key/value tuple the fsmeta executor consumes from scans.
type KV struct {
	Key   []byte
	Value []byte
}

// TxnRunner is the NoKV transaction surface required by fsmeta execution.
//
// ReserveTimestamp returns the first timestamp in a consecutive range of count
// timestamps. Mutate must provide Percolator-style atomicity for all mutations.
type TxnRunner interface {
	ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
	Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
	Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
}

type keyConflictError interface {
	KeyErrors() []*kvrpcpb.KeyError
}

// MountAdmission is the executor's mount-admission view.
type MountAdmission struct {
	MountID       fsmeta.MountID
	RootInode     fsmeta.InodeID
	SchemaVersion uint32
	Retired       bool
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

// Executor interprets fsmeta operation plans against a TxnRunner.
type Executor struct {
	runner                 TxnRunner
	mounts                 MountResolver
	quotas                 QuotaResolver
	subtrees               SubtreeHandoffPublisher
	lockTTL                uint64
	txnRetriesTotal        atomic.Uint64
	txnRetryExhaustedTotal atomic.Uint64
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

// WithSubtreeHandoffPublisher enables rooted subtree authority era advancement
// for RenameSubtree.
func WithSubtreeHandoffPublisher(publisher SubtreeHandoffPublisher) Option {
	return func(e *Executor) {
		e.subtrees = publisher
	}
}

// New constructs an fsmeta executor.
func New(runner TxnRunner, opts ...Option) (*Executor, error) {
	if runner == nil {
		return nil, errors.New("fsmeta/exec: runner required")
	}
	executor := &Executor{
		runner:  runner,
		lockTTL: defaultLockTTL,
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
			"txn_retries_total":         uint64(0),
			"txn_retry_exhausted_total": uint64(0),
		}
	}
	return map[string]any{
		"txn_retries_total":         e.txnRetriesTotal.Load(),
		"txn_retry_exhausted_total": e.txnRetryExhaustedTotal.Load(),
	}
}

// Create creates one dentry and its inode record in a single transaction.
func (e *Executor) Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	plan, err := fsmeta.PlanCreate(req)
	if err != nil {
		return err
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return err
	}
	inode.Inode = req.Inode
	if inode.LinkCount == 0 {
		inode.LinkCount = 1
	}
	dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   inode.Type,
	})
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
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:  req.Mount,
			Scope:  req.Parent,
			Bytes:  inodeSizeDelta(inode.Size),
			Inodes: 1,
		}}, startVersion)
		if err != nil {
			return err
		}
		all := append(cloneMutations(mutations), quotaMutations...)
		return e.runner.Mutate(ctx, plan.PrimaryKey, all, startVersion, commitVersion, e.lockTTL)
	}); err != nil {
		return err
	}
	return nil
}

// Lookup returns the dentry record for parent/name.
func (e *Executor) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	plan, err := fsmeta.PlanLookup(req)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	value, ok, err := e.runner.Get(ctx, plan.PrimaryKey, version)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

// ReadDir returns one directory page from a dentry prefix scan.
func (e *Executor) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	plan, err := fsmeta.PlanReadDir(req)
	if err != nil {
		return nil, err
	}
	version, err := e.readVersion(ctx, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	return e.scanDentries(ctx, plan, version)
}

// ReadDirPlus returns one directory page fused with inode attributes at the
// same snapshot version. This is the first native fsmeta operation that avoids
// client-side dentry scan plus N point reads.
func (e *Executor) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	plan, err := fsmeta.PlanReadDir(req)
	if err != nil {
		return nil, err
	}
	version, err := e.readVersion(ctx, req.SnapshotVersion)
	if err != nil {
		return nil, err
	}
	dentries, err := e.scanDentries(ctx, plan, version)
	if err != nil {
		return nil, err
	}
	if len(dentries) == 0 {
		return []fsmeta.DentryAttrPair{}, nil
	}
	inodeKeys := make([][]byte, 0, len(dentries))
	for _, dentry := range dentries {
		key, err := fsmeta.EncodeInodeKey(req.Mount, dentry.Inode)
		if err != nil {
			return nil, err
		}
		inodeKeys = append(inodeKeys, key)
	}
	inodeValues, err := e.runner.BatchGet(ctx, inodeKeys, version)
	if err != nil {
		return nil, err
	}
	out := make([]fsmeta.DentryAttrPair, 0, len(dentries))
	for i, dentry := range dentries {
		value, ok := inodeValues[string(inodeKeys[i])]
		if !ok {
			return nil, fmt.Errorf("%w: inode %d", fsmeta.ErrNotFound, dentry.Inode)
		}
		inode, err := fsmeta.DecodeInodeValue(value)
		if err != nil {
			return nil, err
		}
		if inode.Inode != dentry.Inode {
			return nil, fmt.Errorf("%w: dentry inode=%d value inode=%d", fsmeta.ErrInvalidValue, dentry.Inode, inode.Inode)
		}
		out = append(out, fsmeta.DentryAttrPair{
			Dentry: dentry,
			Inode:  inode,
		})
	}
	return out, nil
}

// SnapshotSubtree publishes a stable MVCC read version for one direct subtree
// root. The returned token is consumed by ReadDir / ReadDirPlus through
// ReadDirRequest.SnapshotVersion.
func (e *Executor) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	if _, err := fsmeta.PlanSnapshotSubtree(req); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	return fsmeta.SnapshotSubtreeToken{
		Mount:       req.Mount,
		RootInode:   req.RootInode,
		ReadVersion: version,
	}, nil
}

// GetQuotaUsage returns the current persisted usage counter for one quota
// subject. Missing usage keys represent zero usage.
func (e *Executor) GetQuotaUsage(ctx context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	if req.Mount == "" {
		return fsmeta.UsageRecord{}, fsmeta.ErrInvalidMountID
	}
	key, err := fsmeta.EncodeUsageKey(req.Mount, req.Scope)
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

func (e *Executor) scanDentries(ctx context.Context, plan fsmeta.OperationPlan, version uint64) ([]fsmeta.DentryRecord, error) {
	kvs, err := e.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
	if err != nil {
		return nil, err
	}
	prefix := plan.ReadPrefixes[0]
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
	plan, err := fsmeta.PlanLink(req)
	if err != nil {
		return err
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return err
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
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
		inode, ok, err := e.readInode(ctx, req.Mount, record.Inode, startVersion)
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
		inodeKey, err := fsmeta.EncodeInodeKey(req.Mount, inode.Inode)
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
			Mount:  req.Mount,
			Scope:  req.ToParent,
			Bytes:  inodeSizeDelta(inode.Size),
			Inodes: 1,
		}}, startVersion)
		if err != nil {
			return err
		}
		mutations = append(mutations, quotaMutations...)
		return e.runner.Mutate(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, e.lockTTL)
	}); err != nil {
		return err
	}
	return nil
}

// Unlink removes one dentry, decrements its inode link count, and deletes the
// inode record when the last dentry goes away.
func (e *Executor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	plan, err := fsmeta.PlanUnlink(req)
	if err != nil {
		return err
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return err
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		}}
		if inode, ok, err := e.readInode(ctx, req.Mount, record.Inode, startVersion); err != nil {
			return err
		} else if ok {
			inodeKey, err := fsmeta.EncodeInodeKey(req.Mount, inode.Inode)
			if err != nil {
				return err
			}
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
				Mount:  req.Mount,
				Scope:  req.Parent,
				Bytes:  -inodeSizeDelta(inode.Size),
				Inodes: -1,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		return e.runner.Mutate(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, e.lockTTL)
	}); err != nil {
		return err
	}
	return nil
}

// RenameSubtree moves the subtree root dentry from source to destination.
// Descendants follow through inode parent links rather than key rewrites.
func (e *Executor) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	plan, err := fsmeta.PlanRenameSubtree(req)
	if err != nil {
		return err
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	authorityRoot := mountRecord.RootInode
	if e.subtrees != nil && authorityRoot == 0 {
		return fsmeta.ErrInvalidInodeID
	}
	var movedSize uint64
	var movedInode bool
	var committedAt uint64
	var handoffStarted bool
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, err := e.prepareRenameSubtreeMutations(ctx, plan, req, startVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if err := e.startSubtreeHandoff(ctx, req.Mount, authorityRoot, commitVersion); err != nil {
			return err
		}
		handoffStarted = true
		mutationErr := e.runner.Mutate(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, e.lockTTL)
		// Once StartSubtreeHandoff is rooted, a Mutate error may still be
		// ambiguous with respect to primary commit. Complete closes the rooted
		// pending state; at worst this advances an empty era rather than leaving
		// an unrecoverable handoff.
		completeErr := e.completeSubtreeHandoff(ctx, req.Mount, authorityRoot, commitVersion)
		if mutationErr != nil {
			if completeErr != nil {
				return errors.Join(mutationErr, fmt.Errorf("complete subtree handoff: %w", completeErr))
			}
			return mutationErr
		}
		if completeErr != nil {
			return completeErr
		}
		committedAt = commitVersion
		return nil
	}); err != nil {
		return err
	}
	if handoffStarted && committedAt == 0 {
		return fmt.Errorf("subtree handoff started without committed frontier")
	}
	return nil
}

func (e *Executor) prepareRenameSubtreeMutations(ctx context.Context, plan fsmeta.OperationPlan, req fsmeta.RenameSubtreeRequest, startVersion uint64, movedSize *uint64, movedInode *bool) ([]*kvrpcpb.Mutation, error) {
	record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
	if err != nil {
		return nil, err
	}
	if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
		return nil, fsmeta.ErrExists
	} else if !errors.Is(err, fsmeta.ErrNotFound) {
		return nil, err
	}
	record.Parent = req.ToParent
	record.Name = req.ToName
	value, err := fsmeta.EncodeDentryValue(record)
	if err != nil {
		return nil, err
	}
	*movedSize = 0
	*movedInode = false
	if inode, ok, err := e.readInode(ctx, req.Mount, record.Inode, startVersion); err != nil {
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
			{Mount: req.Mount, Scope: req.FromParent, Bytes: -inodeSizeDelta(*movedSize), Inodes: -1},
			{Mount: req.Mount, Scope: req.ToParent, Bytes: inodeSizeDelta(*movedSize), Inodes: 1},
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

func (e *Executor) resolveActiveMount(ctx context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	if e == nil || e.mounts == nil {
		return MountAdmission{}, nil
	}
	record, err := e.mounts.ResolveMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.MountID == "" {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	if record.Retired {
		return MountAdmission{}, fsmeta.ErrMountRetired
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
	return e.runner.ReserveTimestamp(ctx, 1)
}

func (e *Executor) readVersion(ctx context.Context, snapshotVersion uint64) (uint64, error) {
	if snapshotVersion != 0 {
		return snapshotVersion, nil
	}
	return e.reserveReadVersion(ctx)
}

// reserveTxnVersions pre-allocates both start_ts and commit_ts in a single TSO
// hop. In strict Percolator the commit_ts must be obtained AFTER prewrite to
// guarantee snapshot isolation; here we rely on two server-side safety nets to
// make pre-allocation safe in practice:
//
//  1. When a concurrent reader at start_ts > our commit_ts encounters our
//     prewrite lock, it pushes lock.MinCommitTs = reader_start_ts + 1 via
//     CheckTxnStatus (see percolator/txn.go: CallerStartTs handling).
//  2. commitKey rejects the commit with keyErrorCommitTsExpired when
//     lock.MinCommitTs > commitVersion (see percolator/txn.go:373-375).
//
// Together these force a retry-with-fresh-ts under contention — incorrect
// pre-allocation is detected at commit time, never silently violated. The
// optimization saves one TSO RPC per fsmeta operation under the common
// contention-free path. CommitTsExpired is retried transparently by
// withTxnRetry below.
func (e *Executor) reserveTxnVersions(ctx context.Context) (uint64, uint64, error) {
	startVersion, err := e.runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return 0, 0, err
	}
	return startVersion, startVersion + 1, nil
}

func (e *Executor) withTxnRetry(ctx context.Context, run func(startVersion, commitVersion uint64) error) error {
	var last error
	for attempt := 0; attempt <= maxCommitTsExpiredRetries; attempt++ {
		startVersion, commitVersion, err := e.reserveTxnVersions(ctx)
		if err != nil {
			return err
		}
		err = run(startVersion, commitVersion)
		if err == nil {
			return nil
		}
		if !isCommitTsExpired(err) {
			return translateMutateError(err)
		}
		last = err
		if attempt == maxCommitTsExpiredRetries {
			e.txnRetryExhaustedTotal.Add(1)
			break
		}
		e.txnRetriesTotal.Add(1)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	return translateMutateError(last)
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

func (e *Executor) readDentry(ctx context.Context, key []byte, version uint64) (fsmeta.DentryRecord, error) {
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

func (e *Executor) readInode(ctx context.Context, mount fsmeta.MountID, inodeID fsmeta.InodeID, version uint64) (fsmeta.InodeRecord, bool, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil || !ok {
		return fsmeta.InodeRecord{}, ok, err
	}
	inode, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func translateMutateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, fsmeta.ErrExists) {
		return err
	}
	var conflict keyConflictError
	if errors.As(err, &conflict) {
		for _, keyErr := range conflict.KeyErrors() {
			if keyErr != nil && keyErr.GetAlreadyExists() != nil {
				return fmt.Errorf("%w: %v", fsmeta.ErrExists, err)
			}
		}
	}
	return err
}

func isCommitTsExpired(err error) bool {
	var conflict keyConflictError
	if !errors.As(err, &conflict) {
		return false
	}
	for _, keyErr := range conflict.KeyErrors() {
		if keyErr != nil && keyErr.GetCommitTsExpired() != nil {
			return true
		}
	}
	return false
}
