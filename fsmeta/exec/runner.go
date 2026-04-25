package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"

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

// Executor interprets fsmeta operation plans against a TxnRunner.
type Executor struct {
	runner  TxnRunner
	lockTTL uint64
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

// Create creates one dentry and its inode record in a single transaction.
func (e *Executor) Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	plan, err := fsmeta.PlanCreate(req)
	if err != nil {
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
	return e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		return e.runner.Mutate(ctx, plan.PrimaryKey, cloneMutations(mutations), startVersion, commitVersion, e.lockTTL)
	})
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

// Unlink removes one dentry. V0 intentionally leaves inode link-count and GC
// outside this operation slice.
func (e *Executor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	plan, err := fsmeta.PlanUnlink(req)
	if err != nil {
		return err
	}
	mutations := []*kvrpcpb.Mutation{{
		Op:  kvrpcpb.Mutation_Delete,
		Key: cloneBytes(plan.MutateKeys[0]),
	}}
	return e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		if _, err := e.readDentry(ctx, plan.PrimaryKey, startVersion); err != nil {
			return err
		}
		return e.runner.Mutate(ctx, plan.PrimaryKey, cloneMutations(mutations), startVersion, commitVersion, e.lockTTL)
	})
}

// Rename moves one dentry from source to destination. V0 rejects destination
// overwrite; POSIX replacement and type checks are intentionally deferred.
func (e *Executor) Rename(ctx context.Context, req fsmeta.RenameRequest) error {
	plan, err := fsmeta.PlanRename(req)
	if err != nil {
		return err
	}
	return e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
			return fsmeta.ErrExists
		} else if !errors.Is(err, fsmeta.ErrNotFound) {
			return err
		}
		record.Parent = req.ToParent
		record.Name = req.ToName
		value, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
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
		return e.runner.Mutate(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, e.lockTTL)
	})
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
