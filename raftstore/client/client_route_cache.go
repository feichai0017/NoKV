package client

import (
	"context"
	"errors"
	"fmt"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	"sort"
	"time"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

type regionState struct {
	desc   descriptor.Descriptor
	leader uint64
}

type regionSnapshot struct {
	desc   descriptor.Descriptor
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
		desc:   state.desc.Clone(),
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
	if err := ctx.Err(); err != nil {
		return regionSnapshot{}, err
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
	if !ok || region == nil || !containsKey(region.desc, key) {
		return regionSnapshot{}, false
	}
	return regionSnapshot{
		desc:   region.desc.Clone(),
		leader: region.leader,
	}, true
}

// regionForKeyFromResolver requests Region metadata from the external resolver,
// then caches the result for subsequent lookups.
func (c *Client) regionForKeyFromResolver(ctx context.Context, key []byte) (regionSnapshot, error) {
	ctx, cancel := contextWithTimeout(ctx, c.routeLookupTimeout)
	defer cancel()
	resp, err := c.regionResolver.GetRegionByKey(ctx, &pdpb.GetRegionByKeyRequest{Key: append([]byte(nil), key...)})
	if err != nil {
		return regionSnapshot{}, &RouteUnavailableError{
			Key: append([]byte(nil), key...),
			Err: normalizeRPCError(err),
		}
	}
	if resp == nil || resp.GetNotFound() || resp.GetRegionDescriptor() == nil {
		return regionSnapshot{}, &RegionNotFoundError{Key: append([]byte(nil), key...)}
	}
	desc := metacodec.DescriptorFromProto(resp.GetRegionDescriptor())
	if desc.RegionID == 0 {
		return regionSnapshot{}, errors.New("client: resolved region id missing")
	}
	if len(desc.Peers) == 0 {
		return regionSnapshot{}, fmt.Errorf("client: resolved region %d missing peers", desc.RegionID)
	}
	leader := defaultLeaderStoreID(desc)
	if old, ok := c.regionSnapshot(desc.RegionID); ok && old.leader != 0 && regionHasStoreID(desc, old.leader) {
		leader = old.leader
	}
	c.mu.Lock()
	c.upsertRegionLocked(desc, leader)
	c.mu.Unlock()
	if !containsKey(desc, key) {
		return regionSnapshot{}, fmt.Errorf("client: resolved region %d does not contain key %q", desc.RegionID, key)
	}
	return regionSnapshot{
		desc:   desc,
		leader: leader,
	}, nil
}

func (c *Client) handleRegionError(regionID uint64, err *errorpb.RegionError) error {
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
			desc := metacodec.DescriptorFromProto(meta)
			c.upsertRegionLocked(desc, defaultLeaderStoreID(desc))
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

func (c *Client) upsertRegionLocked(desc descriptor.Descriptor, leader uint64) {
	if desc.RegionID == 0 {
		return
	}
	cloned := desc.Clone()
	c.regions[cloned.RegionID] = &regionState{
		desc:   cloned,
		leader: leader,
	}
	entry := regionRange{
		regionID: cloned.RegionID,
		startKey: append([]byte(nil), cloned.StartKey...),
		endKey:   append([]byte(nil), cloned.EndKey...),
	}
	insertAt := sort.Search(len(c.regionIndex), func(i int) bool {
		return bytesCompare(c.regionIndex[i].startKey, entry.startKey) >= 0
	})
	for insertAt < len(c.regionIndex) && c.regionIndex[insertAt].regionID != cloned.RegionID &&
		bytesCompare(c.regionIndex[insertAt].startKey, entry.startKey) == 0 {
		insertAt++
	}
	if idx := c.regionIndexIndexByIDLocked(cloned.RegionID); idx >= 0 {
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

func buildContext(region regionSnapshot) (*kvrpcpb.Context, error) {
	if region.desc.RegionID == 0 {
		return nil, errors.New("client: region meta missing")
	}
	leaderStoreID := region.leader
	if leaderStoreID == 0 {
		leaderStoreID = defaultLeaderStoreID(region.desc)
	}
	if leaderStoreID == 0 {
		return nil, errors.New("client: leader unknown")
	}
	var peerMeta *metapb.RegionPeer
	for _, peer := range region.desc.Peers {
		if peer.StoreID == leaderStoreID {
			peerMeta = &metapb.RegionPeer{StoreId: peer.StoreID, PeerId: peer.PeerID}
			break
		}
	}
	if peerMeta == nil {
		return nil, fmt.Errorf("client: leader store %d not found in region %d peers", leaderStoreID, region.desc.RegionID)
	}
	return &kvrpcpb.Context{
		RegionId: region.desc.RegionID,
		RegionEpoch: &metapb.RegionEpoch{
			Version:     region.desc.Epoch.Version,
			ConfVersion: region.desc.Epoch.ConfVersion,
		},
		Peer: peerMeta,
	}, nil
}

// defaultLeaderStoreID derives a usable leader store from Region peers when no
// explicit leader information is available.
func defaultLeaderStoreID(desc descriptor.Descriptor) uint64 {
	if desc.RegionID == 0 {
		return 0
	}
	for _, peer := range desc.Peers {
		if peer.StoreID != 0 {
			return peer.StoreID
		}
	}
	return 0
}

func regionHasStoreID(desc descriptor.Descriptor, storeID uint64) bool {
	if desc.RegionID == 0 || storeID == 0 {
		return false
	}
	for _, peer := range desc.Peers {
		if peer.StoreID == storeID {
			return true
		}
	}
	return false
}

func containsKey(desc descriptor.Descriptor, key []byte) bool {
	if desc.RegionID == 0 {
		return false
	}
	start := desc.StartKey
	end := desc.EndKey
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
