package client

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/pb"
)

type regionState struct {
	meta   *pb.RegionMeta
	leader uint64
}

type regionSnapshot struct {
	meta   *pb.RegionMeta
	leader uint64
}

type regionRange struct {
	regionID uint64
	startKey []byte
	endKey   []byte
}

func (c *Client) regionSnapshot(regionID uint64) (regionSnapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	state, ok := c.regions[regionID]
	if !ok || state == nil {
		return regionSnapshot{}, false
	}
	return regionSnapshot{
		meta:   cloneRegionMeta(state.meta),
		leader: state.leader,
	}, true
}

func (c *Client) regionForKey(ctx context.Context, key []byte) (regionSnapshot, error) {
	if region, ok := c.regionForKeyFromCache(key); ok {
		return region, nil
	}
	return c.regionForKeyFromResolver(ctx, key)
}

func (c *Client) routeKeyWithRetry(ctx context.Context, key []byte) (regionSnapshot, error) {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.regionForKey(ctx, key)
		if err == nil {
			return region, nil
		}
		if !IsRouteUnavailable(err) {
			return regionSnapshot{}, err
		}
		lastErr = err
		if err := c.waitRetry(ctx, attempt, retryRouteUnavailable); err != nil {
			return regionSnapshot{}, err
		}
	}
	if lastErr != nil {
		return regionSnapshot{}, lastErr
	}
	return regionSnapshot{}, fmt.Errorf("client: route retries exhausted for key %q", key)
}

// regionForKeyFromCache returns a cached Region snapshot when the key is
// covered by an indexed Region range.
func (c *Client) regionForKeyFromCache(key []byte) (regionSnapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	idx := c.regionIndexLookupLocked(key)
	if idx < 0 {
		return regionSnapshot{}, false
	}
	entry := c.regionIndex[idx]
	region, ok := c.regions[entry.regionID]
	if !ok || region == nil || !containsKey(region.meta, key) {
		return regionSnapshot{}, false
	}
	return regionSnapshot{
		meta:   cloneRegionMeta(region.meta),
		leader: region.leader,
	}, true
}

// regionForKeyFromResolver requests Region metadata from the external resolver,
// then caches the result for subsequent lookups.
func (c *Client) regionForKeyFromResolver(ctx context.Context, key []byte) (regionSnapshot, error) {
	ctx, cancel := contextWithTimeout(ctx, c.routeLookupTimeout)
	defer cancel()
	resp, err := c.regionResolver.GetRegionByKey(ctx, &pb.GetRegionByKeyRequest{Key: append([]byte(nil), key...)})
	if err != nil {
		return regionSnapshot{}, &RouteUnavailableError{
			Key: append([]byte(nil), key...),
			Err: normalizeRPCError(err),
		}
	}
	if resp == nil || resp.GetNotFound() || resp.GetRegion() == nil {
		return regionSnapshot{}, &RegionNotFoundError{Key: append([]byte(nil), key...)}
	}
	meta := cloneRegionMeta(resp.GetRegion())
	if meta.GetId() == 0 {
		return regionSnapshot{}, errors.New("client: resolved region id missing")
	}
	if len(meta.GetPeers()) == 0 {
		return regionSnapshot{}, fmt.Errorf("client: resolved region %d missing peers", meta.GetId())
	}
	leader := defaultLeaderStoreID(meta)
	if old, ok := c.regionSnapshot(meta.GetId()); ok && old.leader != 0 {
		leader = old.leader
	}
	c.mu.Lock()
	c.upsertRegionLocked(meta, leader)
	c.mu.Unlock()
	if !containsKey(meta, key) {
		return regionSnapshot{}, fmt.Errorf("client: resolved region %d does not contain key %q", meta.GetId(), key)
	}
	return regionSnapshot{
		meta:   meta,
		leader: leader,
	}, nil
}

func (c *Client) handleRegionError(regionID uint64, err *pb.RegionError) error {
	if err == nil {
		return nil
	}
	if notLeader := err.GetNotLeader(); notLeader != nil {
		if leader := notLeader.GetLeader(); leader != nil {
			c.mu.Lock()
			if region, ok := c.regions[regionID]; ok && region != nil {
				region.leader = leader.GetStoreId()
			}
			c.mu.Unlock()
		}
		return nil
	}
	if epochMismatch := err.GetEpochNotMatch(); epochMismatch != nil {
		c.mu.Lock()
		c.removeRegionLocked(regionID)
		for _, meta := range epochMismatch.GetRegions() {
			if meta == nil {
				continue
			}
			c.upsertRegionLocked(meta, defaultLeaderStoreID(meta))
		}
		c.mu.Unlock()
		return nil
	}
	if err.GetKeyNotInRegion() != nil || err.GetRegionNotFound() != nil {
		c.mu.Lock()
		c.removeRegionLocked(regionID)
		c.mu.Unlock()
		return nil
	}
	if storeMismatch := err.GetStoreNotMatch(); storeMismatch != nil {
		c.mu.Lock()
		c.removeRegionLocked(regionID)
		c.mu.Unlock()
		return fmt.Errorf(
			"client: region %d store mismatch: requested store %d, actual store %d",
			regionID,
			storeMismatch.GetRequestStoreId(),
			storeMismatch.GetActualStoreId(),
		)
	}
	return fmt.Errorf("client: region %d error: %v", regionID, err)
}

func (c *Client) upsertRegionLocked(meta *pb.RegionMeta, leader uint64) {
	if meta == nil || meta.GetId() == 0 {
		return
	}
	cloned := cloneRegionMeta(meta)
	c.regions[cloned.GetId()] = &regionState{
		meta:   cloned,
		leader: leader,
	}
	entry := regionRange{
		regionID: cloned.GetId(),
		startKey: append([]byte(nil), cloned.GetStartKey()...),
		endKey:   append([]byte(nil), cloned.GetEndKey()...),
	}
	insertAt := sort.Search(len(c.regionIndex), func(i int) bool {
		return bytesCompare(c.regionIndex[i].startKey, entry.startKey) >= 0
	})
	for insertAt < len(c.regionIndex) && c.regionIndex[insertAt].regionID != cloned.GetId() &&
		bytesCompare(c.regionIndex[insertAt].startKey, entry.startKey) == 0 {
		insertAt++
	}
	if idx := c.regionIndexIndexByIDLocked(cloned.GetId()); idx >= 0 {
		c.regionIndex = append(c.regionIndex[:idx], c.regionIndex[idx+1:]...)
		if idx < insertAt {
			insertAt--
		}
	}
	c.regionIndex = append(c.regionIndex, regionRange{})
	copy(c.regionIndex[insertAt+1:], c.regionIndex[insertAt:])
	c.regionIndex[insertAt] = entry
}

func (c *Client) removeRegionLocked(regionID uint64) {
	delete(c.regions, regionID)
	if idx := c.regionIndexIndexByIDLocked(regionID); idx >= 0 {
		c.regionIndex = append(c.regionIndex[:idx], c.regionIndex[idx+1:]...)
	}
}

func (c *Client) regionIndexLookupLocked(key []byte) int {
	if len(c.regionIndex) == 0 {
		return -1
	}
	idx := sort.Search(len(c.regionIndex), func(i int) bool {
		return bytesCompare(c.regionIndex[i].startKey, key) > 0
	})
	if idx == 0 {
		return -1
	}
	return idx - 1
}

func (c *Client) regionIndexIndexByIDLocked(regionID uint64) int {
	for i := range c.regionIndex {
		if c.regionIndex[i].regionID == regionID {
			return i
		}
	}
	return -1
}

func contextWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return context.WithCancel(parent)
}

func buildContext(region regionSnapshot) (*pb.Context, error) {
	if region.meta == nil {
		return nil, errors.New("client: region meta missing")
	}
	leaderStoreID := region.leader
	if leaderStoreID == 0 {
		leaderStoreID = defaultLeaderStoreID(region.meta)
	}
	if leaderStoreID == 0 {
		return nil, errors.New("client: leader unknown")
	}
	var peerMeta *pb.RegionPeer
	for _, peer := range region.meta.GetPeers() {
		if peer.GetStoreId() == leaderStoreID {
			peerMeta = peer
			break
		}
	}
	if peerMeta == nil {
		return nil, fmt.Errorf("client: leader store %d not found in region %d peers", leaderStoreID, region.meta.GetId())
	}
	return &pb.Context{
		RegionId: region.meta.GetId(),
		RegionEpoch: &pb.RegionEpoch{
			Version: region.meta.GetEpochVersion(),
			ConfVer: region.meta.GetEpochConfVersion(),
		},
		Peer: peerMeta,
	}, nil
}

// defaultLeaderStoreID derives a usable leader store from Region peers when no
// explicit leader information is available.
func defaultLeaderStoreID(meta *pb.RegionMeta) uint64 {
	if meta == nil {
		return 0
	}
	for _, peer := range meta.GetPeers() {
		if peer != nil && peer.GetStoreId() != 0 {
			return peer.GetStoreId()
		}
	}
	return 0
}

func containsKey(meta *pb.RegionMeta, key []byte) bool {
	if meta == nil {
		return false
	}
	start := meta.GetStartKey()
	end := meta.GetEndKey()
	if len(start) > 0 && bytesCompare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytesCompare(key, end) >= 0 {
		return false
	}
	return true
}

func bytesCompare(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func incrementKey(key []byte) []byte {
	out := append([]byte(nil), key...)
	for i := len(out) - 1; i >= 0; i-- {
		out[i]++
		if out[i] != 0 {
			return out
		}
	}
	return append(out, 0)
}

func cloneRegionMeta(meta *pb.RegionMeta) *pb.RegionMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*pb.RegionMeta)
}
