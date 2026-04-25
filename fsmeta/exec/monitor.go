package exec

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultMonitorInterval = time.Second

type mountList interface {
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
	ListSubtreeAuthorities(context.Context, *coordpb.ListSubtreeAuthoritiesRequest) (*coordpb.ListSubtreeAuthoritiesResponse, error)
	ListQuotaFences(context.Context, *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error)
}

type retireRouter interface {
	RetireMount(fsmeta.MountID) int
}

// monitor polls coordinator ListMounts for retired mounts and fans the retire
// signal to the router (close watchers) and the mount cache (flip admission
// immediately). It is owned by Runtime and shut down by Runtime.Close.
type monitor struct {
	coord    mountList
	router   retireRouter
	cache    *mountCache
	quotas   *quotaCache
	subtrees SubtreeHandoffPublisher
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func startMonitor(ctx context.Context, coord mountList, router retireRouter, cache *mountCache, quotas *quotaCache, subtrees SubtreeHandoffPublisher, interval time.Duration) *monitor {
	if coord == nil || router == nil {
		return nil
	}
	if interval <= 0 {
		interval = defaultMonitorInterval
	}
	m := &monitor{
		coord:    coord,
		router:   router,
		cache:    cache,
		quotas:   quotas,
		subtrees: subtrees,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go m.run(ctx)
	return m
}

func (m *monitor) run(ctx context.Context) {
	defer close(m.done)
	_ = m.poll(ctx)
	tick := time.NewTicker(m.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stop:
			return
		case <-tick.C:
			if err := m.poll(ctx); err != nil && ctx.Err() == nil {
				log.Printf("fsmeta monitor: %v", err)
			}
		}
	}
}

func (m *monitor) poll(ctx context.Context) error {
	if m == nil || m.coord == nil || m.router == nil {
		return nil
	}
	resp, err := m.coord.ListMounts(ctx, &coordpb.ListMountsRequest{})
	if err != nil {
		return err
	}
	for _, mount := range resp.GetMounts() {
		if mount.GetMountId() == "" || mount.GetState() != coordpb.MountState_MOUNT_STATE_RETIRED {
			continue
		}
		id := fsmeta.MountID(mount.GetMountId())
		if m.cache != nil {
			m.cache.markRetired(id)
		}
		if m.quotas != nil {
			m.quotas.purgeMount(id)
		}
		m.router.RetireMount(id)
	}
	quotas, err := m.coord.ListQuotaFences(ctx, &coordpb.ListQuotaFencesRequest{})
	if err != nil {
		return err
	}
	if m.quotas != nil {
		for _, fence := range quotas.GetFences() {
			m.quotas.markFenceUpdated(fence)
		}
	}
	subtrees, err := m.coord.ListSubtreeAuthorities(ctx, &coordpb.ListSubtreeAuthoritiesRequest{})
	if err != nil {
		return err
	}
	if m.subtrees != nil {
		for _, subtree := range subtrees.GetSubtrees() {
			if subtree.GetState() != coordpb.SubtreeAuthorityState_SUBTREE_AUTHORITY_STATE_HANDOFF {
				continue
			}
			if subtree.GetMountId() == "" || subtree.GetRootInode() == 0 || subtree.GetPredecessorFrontier() == 0 {
				continue
			}
			if err := m.subtrees.CompleteSubtreeHandoff(ctx, fsmeta.MountID(subtree.GetMountId()), fsmeta.InodeID(subtree.GetRootInode()), subtree.GetPredecessorFrontier()); err != nil {
				log.Printf("fsmeta monitor: complete pending subtree handoff mount=%s root=%d frontier=%d: %v",
					subtree.GetMountId(), subtree.GetRootInode(), subtree.GetPredecessorFrontier(), err)
			}
		}
	}
	return nil
}

func (m *monitor) Close() error {
	if m == nil {
		return nil
	}
	m.once.Do(func() { close(m.stop) })
	<-m.done
	return nil
}
