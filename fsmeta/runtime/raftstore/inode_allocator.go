package raftstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultInodeAllocBatchSize = 256
	defaultInodeAffinityShards = 4
)

// IDAllocatorClient is the rooted coordinator ID surface used by fsmeta. The
// allocator deliberately depends on the RPC contract, not coordinator/idalloc,
// so coordinator remains the only production authority for globally unique IDs.
type IDAllocatorClient interface {
	AllocID(ctx context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error)
}

// ShardAffineInodeAllocator prefetches coordinator IDs and returns an inode ID
// whose fsmeta placement shard matches the target dentry when possible. A miss
// is still correct: Create keeps the existing 1PC safety gate and falls back to
// Percolator 2PC when local atomicity cannot be proven.
type ShardAffineInodeAllocator struct {
	client    IDAllocatorClient
	shards    int
	batchSize uint64

	mu    sync.Mutex
	pools map[fsmeta.MountID][][]fsmeta.InodeID

	total         atomic.Uint64
	affinityHit   atomic.Uint64
	affinityMiss  atomic.Uint64
	refillTotal   atomic.Uint64
	reservedTotal atomic.Uint64
}

func NewShardAffineInodeAllocator(client IDAllocatorClient, shardCount int) (*ShardAffineInodeAllocator, error) {
	return NewShardAffineInodeAllocatorWithBatch(client, shardCount, defaultInodeAllocBatchSize)
}

func NewShardAffineInodeAllocatorWithBatch(client IDAllocatorClient, shardCount int, batchSize uint64) (*ShardAffineInodeAllocator, error) {
	if client == nil {
		return nil, errIDAllocatorClientRequired
	}
	if batchSize == 0 {
		return nil, errInodeAllocBatchRequired
	}
	shards := utils.NormalizeShardCount(shardCount)
	return &ShardAffineInodeAllocator{
		client:    client,
		shards:    shards,
		batchSize: batchSize,
		pools:     make(map[fsmeta.MountID][][]fsmeta.InodeID),
	}, nil
}

func (a *ShardAffineInodeAllocator) AllocateCreateInode(ctx context.Context, mount fsmeta.MountID, parent fsmeta.InodeID, name string) (fsmeta.InodeID, error) {
	if a == nil {
		return 0, errIDAllocatorClientRequired
	}
	target, err := createDentryShard(mount, parent, name, a.shards)
	if err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if inode, ok := a.popShardLocked(mount, target); ok {
		a.total.Add(1)
		a.affinityHit.Add(1)
		return inode, nil
	}
	if err := a.refillLocked(ctx, mount); err != nil {
		return 0, err
	}
	if inode, ok := a.popShardLocked(mount, target); ok {
		a.total.Add(1)
		a.affinityHit.Add(1)
		return inode, nil
	}
	if inode, ok := a.popAnyLocked(mount); ok {
		a.total.Add(1)
		a.affinityMiss.Add(1)
		return inode, nil
	}
	return 0, errNoUsableInodeID
}

func (a *ShardAffineInodeAllocator) Stats() map[string]any {
	if a == nil {
		return map[string]any{
			"inode_alloc_total":               uint64(0),
			"inode_alloc_affinity_hit_total":  uint64(0),
			"inode_alloc_affinity_miss_total": uint64(0),
			"inode_alloc_refill_total":        uint64(0),
			"inode_alloc_reserved_total":      uint64(0),
		}
	}
	return map[string]any{
		"inode_alloc_total":               a.total.Load(),
		"inode_alloc_affinity_hit_total":  a.affinityHit.Load(),
		"inode_alloc_affinity_miss_total": a.affinityMiss.Load(),
		"inode_alloc_refill_total":        a.refillTotal.Load(),
		"inode_alloc_reserved_total":      a.reservedTotal.Load(),
	}
}

func (a *ShardAffineInodeAllocator) refillLocked(ctx context.Context, mount fsmeta.MountID) error {
	resp, err := a.client.AllocID(ctx, &coordpb.AllocIDRequest{Count: a.batchSize})
	if err != nil {
		return err
	}
	if resp == nil {
		return errNilAllocIDResponse
	}
	if resp.GetCount() == 0 {
		return errEmptyAllocIDResponse
	}
	a.refillTotal.Add(1)
	pool := a.ensurePoolsLocked(mount)
	first := resp.GetFirstId()
	for i := uint64(0); i < resp.GetCount(); i++ {
		id := first + i
		if id < first || fsmeta.InodeID(id) <= fsmeta.RootInode {
			continue
		}
		shard, err := createInodeShard(mount, fsmeta.InodeID(id), a.shards)
		if err != nil {
			return err
		}
		pool[shard] = append(pool[shard], fsmeta.InodeID(id))
		a.reservedTotal.Add(1)
	}
	return nil
}

func (a *ShardAffineInodeAllocator) ensurePoolsLocked(mount fsmeta.MountID) [][]fsmeta.InodeID {
	if pool := a.pools[mount]; len(pool) == a.shards {
		return pool
	}
	pool := make([][]fsmeta.InodeID, a.shards)
	a.pools[mount] = pool
	return pool
}

func (a *ShardAffineInodeAllocator) popShardLocked(mount fsmeta.MountID, shard int) (fsmeta.InodeID, bool) {
	pool := a.ensurePoolsLocked(mount)
	if shard < 0 || shard >= len(pool) || len(pool[shard]) == 0 {
		return 0, false
	}
	last := len(pool[shard]) - 1
	inode := pool[shard][last]
	pool[shard] = pool[shard][:last]
	return inode, true
}

func (a *ShardAffineInodeAllocator) popAnyLocked(mount fsmeta.MountID) (fsmeta.InodeID, bool) {
	pool := a.ensurePoolsLocked(mount)
	for shard := range pool {
		if inode, ok := a.popShardLocked(mount, shard); ok {
			return inode, true
		}
	}
	return 0, false
}

func createDentryShard(mount fsmeta.MountID, parent fsmeta.InodeID, name string, shards int) (int, error) {
	key, err := fsmeta.EncodeDentryKey(mount, parent, name)
	if err != nil {
		return 0, err
	}
	return fsmeta.ShardForUserKey(key, shards), nil
}

func createInodeShard(mount fsmeta.MountID, inode fsmeta.InodeID, shards int) (int, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inode)
	if err != nil {
		return 0, err
	}
	return fsmeta.ShardForUserKey(key, shards), nil
}
