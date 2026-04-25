package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const genericKVDefaultLockTTL uint64 = 3000

// GenericKVDriver implements the same workload surface by treating fsmeta as a
// plain key/value schema. It is intentionally not the native path: Create uses
// read-then-write checks, and ReadDirPlus uses scan plus N point reads.
type GenericKVDriver struct {
	runner  fsmetaexec.TxnRunner
	lockTTL uint64
}

// GenericKVOption configures a GenericKVDriver.
type GenericKVOption func(*GenericKVDriver)

// WithGenericKVLockTTL overrides the Percolator lock TTL used by generic-KV
// mutating operations.
func WithGenericKVLockTTL(ttl uint64) GenericKVOption {
	return func(d *GenericKVDriver) {
		if ttl > 0 {
			d.lockTTL = ttl
		}
	}
}

// NewGenericKVDriver constructs the generic-KV baseline used by fsmeta
// benchmarks. The runner is shared with native fsmeta execution to keep the
// storage cluster and transaction implementation constant.
func NewGenericKVDriver(runner fsmetaexec.TxnRunner, opts ...GenericKVOption) (*GenericKVDriver, error) {
	if runner == nil {
		return nil, errors.New("benchmark/fsmeta/workload: txn runner required")
	}
	driver := &GenericKVDriver{
		runner:  runner,
		lockTTL: genericKVDefaultLockTTL,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(driver)
		}
	}
	return driver, nil
}

// Create creates one dentry and inode by composing plain KV reads and a 2PC
// mutation. Unlike native fsmeta, this path does not use server-side
// AssertionNotExist; it models the common "schema over KV" baseline.
func (d *GenericKVDriver) Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	plan, err := fsmeta.PlanCreate(req)
	if err != nil {
		return err
	}
	startVersion, commitVersion, err := d.reserveTxnVersions(ctx)
	if err != nil {
		return err
	}
	if _, ok, err := d.runner.Get(ctx, plan.MutateKeys[0], startVersion); err != nil {
		return err
	} else if ok {
		return fsmeta.ErrExists
	}
	if _, ok, err := d.runner.Get(ctx, plan.MutateKeys[1], startVersion); err != nil {
		return err
	} else if ok {
		return fsmeta.ErrExists
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
			Op:    kvrpcpb.Mutation_Put,
			Key:   cloneBytes(plan.MutateKeys[0]),
			Value: dentryValue,
		},
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   cloneBytes(plan.MutateKeys[1]),
			Value: inodeValue,
		},
	}
	return d.runner.Mutate(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, d.lockTTL)
}

// ReadDir scans the dentry prefix directly from the generic KV schema.
func (d *GenericKVDriver) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	plan, err := fsmeta.PlanReadDir(req)
	if err != nil {
		return nil, err
	}
	version, err := d.reserveReadVersion(ctx)
	if err != nil {
		return nil, err
	}
	return d.scanDentries(ctx, plan, version)
}

// ReadDirPlus models the generic-KV implementation of readdir+stat: one prefix
// scan followed by one point Get per returned dentry, all at the same snapshot.
func (d *GenericKVDriver) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	plan, err := fsmeta.PlanReadDir(req)
	if err != nil {
		return nil, err
	}
	version, err := d.reserveReadVersion(ctx)
	if err != nil {
		return nil, err
	}
	dentries, err := d.scanDentries(ctx, plan, version)
	if err != nil {
		return nil, err
	}
	out := make([]fsmeta.DentryAttrPair, 0, len(dentries))
	for _, dentry := range dentries {
		key, err := fsmeta.EncodeInodeKey(req.Mount, dentry.Inode)
		if err != nil {
			return nil, err
		}
		value, ok, err := d.runner.Get(ctx, key, version)
		if err != nil {
			return nil, err
		}
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
		out = append(out, fsmeta.DentryAttrPair{Dentry: dentry, Inode: inode})
	}
	return out, nil
}

func (d *GenericKVDriver) scanDentries(ctx context.Context, plan fsmeta.OperationPlan, version uint64) ([]fsmeta.DentryRecord, error) {
	kvs, err := d.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
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

func (d *GenericKVDriver) reserveReadVersion(ctx context.Context) (uint64, error) {
	return d.runner.ReserveTimestamp(ctx, 1)
}

func (d *GenericKVDriver) reserveTxnVersions(ctx context.Context) (uint64, uint64, error) {
	startVersion, err := d.runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return 0, 0, err
	}
	return startVersion, startVersion + 1, nil
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
