package exec

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultMountTTL = time.Second

type mountLookup interface {
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
}

// mountCache implements MountResolver against the coordinator GetMount RPC
// with a small TTL cache. Retire events bypass the TTL via markRetired so
// admission flips immediately.
type mountCache struct {
	coord mountLookup
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	entries map[fsmeta.MountID]mountEntry
}

type mountEntry struct {
	record    MountAdmission
	err       error
	expiresAt time.Time
}

func (c *mountCache) ResolveMount(ctx context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	if c.coord == nil {
		return MountAdmission{}, errors.New("mount cache is not configured")
	}
	now := c.clock()
	if record, err, ok := c.lookup(mount, now); ok {
		return record, err
	}
	resp, err := c.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return MountAdmission{}, err
	}
	record, err := mountFromProto(resp)
	c.put(mount, now, record, err)
	return record, err
}

// markRetired forces the cached view of mount to retired. The monitor calls
// this when it observes a rooted MountRetired event so admission flips before
// the next ResolveMount poll.
func (c *mountCache) markRetired(mount fsmeta.MountID) {
	if mount == "" {
		return
	}
	c.put(mount, c.clock(), MountAdmission{MountID: mount, Retired: true}, nil)
}

func (c *mountCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *mountCache) lookup(mount fsmeta.MountID, now time.Time) (MountAdmission, error, bool) {
	if c.ttl <= 0 {
		return MountAdmission{}, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[mount]
	if !ok || !now.Before(entry.expiresAt) {
		return MountAdmission{}, nil, false
	}
	return entry.record, entry.err, true
}

func (c *mountCache) put(mount fsmeta.MountID, now time.Time, record MountAdmission, err error) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[fsmeta.MountID]mountEntry)
	}
	c.entries[mount] = mountEntry{record: record, err: err, expiresAt: now.Add(c.ttl)}
}

func mountFromProto(resp *coordpb.GetMountResponse) (MountAdmission, error) {
	if resp == nil || resp.GetNotFound() {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	info := resp.GetMount()
	if info == nil {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	return MountAdmission{
		MountID:       fsmeta.MountID(info.GetMountId()),
		RootInode:     fsmeta.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}, nil
}
