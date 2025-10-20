package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/pb"
)

// StoreEndpoint describes a reachable store in the cluster.
type StoreEndpoint struct {
	StoreID uint64
	Addr    string
}

// RegionConfig seeds the client with Region metadata and the known leader.
type RegionConfig struct {
	Meta          *pb.RegionMeta
	LeaderStoreID uint64
}

// Config configures the TinyKv distributed client.
type Config struct {
	Stores      []StoreEndpoint
	Regions     []RegionConfig
	DialTimeout time.Duration
	DialOptions []grpc.DialOption
	MaxRetries  int
}

type storeConn struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.TinyKvClient
}

type regionState struct {
	meta   *pb.RegionMeta
	leader uint64
}

type regionSnapshot struct {
	meta   *pb.RegionMeta
	leader uint64
}

// Client provides Region-aware helpers for TinyKv RPCs, including 2PC.
type Client struct {
	mu         sync.RWMutex
	stores     map[uint64]*storeConn
	regions    map[uint64]*regionState
	maxRetries int
}

// New constructs a Client using the provided configuration.
func New(cfg Config) (*Client, error) {
	if len(cfg.Stores) == 0 {
		return nil, errors.New("client: at least one store endpoint required")
	}
	if len(cfg.Regions) == 0 {
		return nil, errors.New("client: at least one region required")
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	dialOpts := cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	stores := make(map[uint64]*storeConn, len(cfg.Stores))
	for _, endpoint := range cfg.Stores {
		if endpoint.StoreID == 0 || endpoint.Addr == "" {
			return nil, fmt.Errorf("client: invalid store endpoint %+v", endpoint)
		}
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := grpc.DialContext(ctx, endpoint.Addr, dialOpts...)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("client: dial %s: %w", endpoint.Addr, err)
		}
		stores[endpoint.StoreID] = &storeConn{
			addr:   endpoint.Addr,
			conn:   conn,
			client: pb.NewTinyKvClient(conn),
		}
	}
	regions := make(map[uint64]*regionState, len(cfg.Regions))
	for _, region := range cfg.Regions {
		if region.Meta == nil {
			return nil, errors.New("client: region meta missing")
		}
		id := region.Meta.GetId()
		if id == 0 {
			return nil, errors.New("client: region id missing")
		}
		if len(region.Meta.GetPeers()) == 0 {
			return nil, fmt.Errorf("client: region %d missing peers", id)
		}
		regions[id] = &regionState{
			meta:   cloneRegionMeta(region.Meta),
			leader: region.LeaderStoreID,
		}
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	return &Client{
		stores:     stores,
		regions:    regions,
		maxRetries: maxRetries,
	}, nil
}

// Close terminates outstanding store connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for id, st := range c.stores {
		if st == nil || st.conn == nil {
			continue
		}
		if err := st.conn.Close(); err != nil && first == nil {
			first = fmt.Errorf("client: close store %d: %w", id, err)
		}
	}
	return first
}

// Get issues a KvGet for the provided key/version. It retries on region errors.
func (c *Client) Get(ctx context.Context, key []byte, version uint64) (*pb.GetResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		region, err := c.regionForKey(key)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.callGet(ctx, region, key, version)
		if err != nil {
			return nil, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(region.meta.GetId(), regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("client: kv get retries exhausted for key %q", key)
}

func (c *Client) callGet(ctx context.Context, region regionSnapshot, key []byte, version uint64) (*pb.GetResponse, *pb.RegionError, error) {
	st, err := c.store(region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	resp, err := st.client.KvGet(ctx, &pb.KvGetRequest{
		Context: header,
		Request: &pb.GetRequest{
			Key:     append([]byte(nil), key...),
			Version: version,
		},
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

// Scan issues a forward KvScan starting at startKey, reading up to limit keys.
func (c *Client) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*pb.KV, error) {
	if limit == 0 {
		return nil, errors.New("client: scan limit must be > 0")
	}
	collected := make([]*pb.KV, 0, limit)
	currentKey := append([]byte(nil), startKey...)
	remaining := limit
	for remaining > 0 {
		region, err := c.regionForKey(currentKey)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.callScan(ctx, region, currentKey, remaining, version)
		if err != nil {
			return nil, err
		}
		if regionErr != nil {
			if err := c.handleRegionError(region.meta.GetId(), regionErr); err != nil {
				return nil, err
			}
			continue
		}
		kvs := resp.GetKvs()
		collected = append(collected, kvs...)
		if len(kvs) == 0 {
			endKey := region.meta.GetEndKey()
			if len(endKey) == 0 {
				break
			}
			currentKey = append([]byte(nil), endKey...)
			continue
		}
		remaining -= uint32(len(kvs))
		if remaining == 0 {
			break
		}
		endKey := region.meta.GetEndKey()
		nextKey := incrementKey(kvs[len(kvs)-1].GetKey())
		if len(endKey) > 0 && bytesCompare(nextKey, endKey) >= 0 {
			currentKey = append([]byte(nil), endKey...)
			continue
		}
		currentKey = nextKey
	}
	return collected, nil
}

func (c *Client) callScan(ctx context.Context, region regionSnapshot, startKey []byte, limit uint32, version uint64) (*pb.ScanResponse, *pb.RegionError, error) {
	st, err := c.store(region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	resp, err := st.client.KvScan(ctx, &pb.KvScanRequest{
		Context: header,
		Request: &pb.ScanRequest{
			StartKey:     append([]byte(nil), startKey...),
			Limit:        limit,
			Version:      version,
			IncludeStart: true,
		},
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

// Mutate wraps TwoPhaseCommit with a ready-made mutation slice. The caller must
// ensure the primary key is part of the mutation set.
func (c *Client) Mutate(ctx context.Context, primary []byte, mutations []*pb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	if len(primary) == 0 {
		return fmt.Errorf("client: primary key required")
	}
	cleaned := make([]*pb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		cleaned = append(cleaned, cloneMutation(mut))
	}
	if len(cleaned) == 0 {
		return nil
	}
	if !mutationHasPrimary(cleaned, primary) {
		return fmt.Errorf("client: primary key %q not present in mutations", primary)
	}
	return c.TwoPhaseCommit(ctx, append([]byte(nil), primary...), cleaned, startVersion, commitVersion, lockTTL)
}

// Put performs a single-key Put using the two-phase commit path.
func (c *Client) Put(ctx context.Context, key, value []byte, startVersion, commitVersion, lockTTL uint64) error {
	mut := &pb.Mutation{
		Op:    pb.Mutation_Put,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
	return c.Mutate(ctx, key, []*pb.Mutation{mut}, startVersion, commitVersion, lockTTL)
}

// Delete removes a key using a two-phase commit delete mutation.
func (c *Client) Delete(ctx context.Context, key []byte, startVersion, commitVersion, lockTTL uint64) error {
	mut := &pb.Mutation{
		Op:  pb.Mutation_Delete,
		Key: append([]byte(nil), key...),
	}
	return c.Mutate(ctx, key, []*pb.Mutation{mut}, startVersion, commitVersion, lockTTL)
}

// TwoPhaseCommit runs Prewrite followed by Commit across the supplied mutations.
func (c *Client) TwoPhaseCommit(ctx context.Context, primary []byte, mutations []*pb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	if len(mutations) == 0 {
		return nil
	}
	grouped := make(map[uint64][]*pb.Mutation)
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		region, err := c.regionForKey(mut.GetKey())
		if err != nil {
			return err
		}
		id := region.meta.GetId()
		grouped[id] = append(grouped[id], cloneMutation(mut))
	}
	primaryRegion, err := c.regionForKey(primary)
	if err != nil {
		return err
	}
	primaryID := primaryRegion.meta.GetId()
	primaryMutations, ok := grouped[primaryID]
	if !ok || len(primaryMutations) == 0 {
		return fmt.Errorf("client: primary key %q missing from mutations", primary)
	}
	if err := c.prewriteRegion(ctx, primaryID, primary, startVersion, lockTTL, primaryMutations); err != nil {
		return err
	}
	for regionID, muts := range grouped {
		if regionID == primaryID {
			continue
		}
		if err := c.prewriteRegion(ctx, regionID, primary, startVersion, lockTTL, muts); err != nil {
			return err
		}
	}
	if err := c.commitRegion(ctx, primaryID, collectKeys(primaryMutations), startVersion, commitVersion); err != nil {
		return err
	}
	for regionID, muts := range grouped {
		if regionID == primaryID {
			continue
		}
		if err := c.commitRegion(ctx, regionID, collectKeys(muts), startVersion, commitVersion); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) prewriteRegion(ctx context.Context, regionID uint64, primary []byte, startVersion, ttl uint64, muts []*pb.Mutation) error {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return fmt.Errorf("client: region %d missing for prewrite", regionID)
		}
		st, err := c.store(region.leader)
		if err != nil {
			return err
		}
		header, err := buildContext(region)
		if err != nil {
			return err
		}
		req := &pb.KvPrewriteRequest{
			Context: header,
			Request: &pb.PrewriteRequest{
				Mutations:    muts,
				PrimaryLock:  append([]byte(nil), primary...),
				StartVersion: startVersion,
				LockTtl:      ttl,
			},
		}
		resp, err := st.client.KvPrewrite(ctx, req)
		if err != nil {
			return normalizeRPCError(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return lastErr
			}
			continue
		}
		if pr := resp.GetResponse(); pr != nil && len(pr.GetErrors()) > 0 {
			return fmt.Errorf("client: prewrite key errors: %+v", pr.GetErrors())
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("client: prewrite retries exhausted for region %d", regionID)
}

func (c *Client) commitRegion(ctx context.Context, regionID uint64, keys [][]byte, startVersion, commitVersion uint64) error {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return fmt.Errorf("client: region %d missing for commit", regionID)
		}
		st, err := c.store(region.leader)
		if err != nil {
			return err
		}
		header, err := buildContext(region)
		if err != nil {
			return err
		}
		req := &pb.KvCommitRequest{
			Context: header,
			Request: &pb.CommitRequest{
				Keys:          cloneKeys(keys),
				StartVersion:  startVersion,
				CommitVersion: commitVersion,
			},
		}
		resp, err := st.client.KvCommit(ctx, req)
		if err != nil {
			return normalizeRPCError(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return lastErr
			}
			continue
		}
		if cr := resp.GetResponse(); cr != nil && cr.GetError() != nil {
			return fmt.Errorf("client: commit key error: %v", cr.GetError())
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("client: commit retries exhausted for region %d", regionID)
}

func (c *Client) store(storeID uint64) (*storeConn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if storeID == 0 {
		return nil, errors.New("client: store id not set")
	}
	st, ok := c.stores[storeID]
	if !ok || st == nil {
		return nil, fmt.Errorf("client: store %d not found", storeID)
	}
	return st, nil
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

func (c *Client) regionForKey(key []byte) (regionSnapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if region == nil {
			continue
		}
		if containsKey(region.meta, key) {
			return regionSnapshot{
				meta:   cloneRegionMeta(region.meta),
				leader: region.leader,
			}, nil
		}
	}
	return regionSnapshot{}, fmt.Errorf("client: region not found for key %q", key)
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
		delete(c.regions, regionID)
		for _, meta := range epochMismatch.GetRegions() {
			if meta == nil {
				continue
			}
			c.regions[meta.GetId()] = &regionState{
				meta:   cloneRegionMeta(meta),
				leader: 0,
			}
		}
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("client: region %d error: %v", regionID, err)
}

func buildContext(region regionSnapshot) (*pb.Context, error) {
	if region.meta == nil {
		return nil, errors.New("client: region meta missing")
	}
	if region.leader == 0 {
		return nil, errors.New("client: leader unknown")
	}
	var peerMeta *pb.RegionPeer
	for _, peer := range region.meta.GetPeers() {
		if peer.GetStoreId() == region.leader {
			peerMeta = peer
			break
		}
	}
	if peerMeta == nil {
		return nil, fmt.Errorf("client: leader store %d not found in region %d peers", region.leader, region.meta.GetId())
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
	cp := proto.Clone(meta).(*pb.RegionMeta)
	return cp
}

func cloneMutation(mut *pb.Mutation) *pb.Mutation {
	if mut == nil {
		return nil
	}
	return proto.Clone(mut).(*pb.Mutation)
}

func cloneKeys(keys [][]byte) [][]byte {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		out[i] = append([]byte(nil), key...)
	}
	return out
}

func collectKeys(muts []*pb.Mutation) [][]byte {
	out := make([][]byte, 0, len(muts))
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		out = append(out, append([]byte(nil), mut.GetKey()...))
	}
	return out
}

func mutationHasPrimary(muts []*pb.Mutation, primary []byte) bool {
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		if bytesCompare(mut.GetKey(), primary) == 0 {
			return true
		}
	}
	return false
}

func normalizeRPCError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == 0 {
		return err
	}
	return err
}
