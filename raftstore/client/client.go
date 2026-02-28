package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/pb"
)

// KeyConflictError represents prewrite-time key conflicts surfaced by the raft
// service. Callers can inspect the KeyErrors to resolve locks before retrying.
type KeyConflictError struct {
	Errors []*pb.KeyError
}

func (e *KeyConflictError) Error() string {
	return fmt.Sprintf("client: prewrite key errors: %+v", e.Errors)
}

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

// RegionResolver resolves Region metadata for an arbitrary key. A PD client
// implementation should satisfy this interface.
type RegionResolver interface {
	GetRegionByKey(ctx context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error)
	Close() error
}

// Config configures the TinyKv distributed client.
type Config struct {
	Stores             []StoreEndpoint
	Regions            []RegionConfig
	RegionResolver     RegionResolver
	RouteLookupTimeout time.Duration
	DialTimeout        time.Duration
	DialOptions        []grpc.DialOption
	MaxRetries         int
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
	mu                 sync.RWMutex
	stores             map[uint64]*storeConn
	regions            map[uint64]*regionState
	regionResolver     RegionResolver
	routeLookupTimeout time.Duration
	maxRetries         int
}

// New constructs a Client using the provided configuration.
func New(cfg Config) (*Client, error) {
	if len(cfg.Stores) == 0 {
		return nil, errors.New("client: at least one store endpoint required")
	}
	if len(cfg.Regions) == 0 && cfg.RegionResolver == nil {
		return nil, errors.New("client: at least one region or region resolver required")
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	routeLookupTimeout := cfg.RouteLookupTimeout
	if routeLookupTimeout <= 0 {
		routeLookupTimeout = 2 * time.Second
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
		conn, err := dialStore(ctx, endpoint.Addr, dialOpts...)
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
		stores:             stores,
		regions:            regions,
		regionResolver:     cfg.RegionResolver,
		routeLookupTimeout: routeLookupTimeout,
		maxRetries:         maxRetries,
	}, nil
}

func dialStore(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			_ = conn.Close()
			return nil, ctx.Err()
		}
	}
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
	if c.regionResolver != nil {
		if err := c.regionResolver.Close(); err != nil && first == nil {
			first = fmt.Errorf("client: close region resolver: %w", err)
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

// BatchGet fetches multiple keys using the same snapshot version. Keys are
// grouped by region so that each group shares a single KvBatchGet round-trip
// and read index.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*pb.GetResponse, error) {
	results := make(map[string]*pb.GetResponse, len(keys))
	if len(keys) == 0 {
		return results, nil
	}
	pending := make(map[string][]byte, len(keys))
	for _, key := range keys {
		keyCopy := append([]byte(nil), key...)
		pending[string(keyCopy)] = keyCopy
	}
	type regionBatch struct {
		region regionSnapshot
		keys   [][]byte
		ids    []string
	}
	var lastErr error
	for attempt := 0; attempt < c.maxRetries && len(pending) > 0; attempt++ {
		groups := make(map[uint64]*regionBatch)
		for keyID, key := range pending {
			region, err := c.regionForKey(key)
			if err != nil {
				return nil, err
			}
			regionID := region.meta.GetId()
			group := groups[regionID]
			if group == nil {
				group = &regionBatch{region: region}
				groups[regionID] = group
			}
			group.keys = append(group.keys, key)
			group.ids = append(group.ids, keyID)
		}
		var completed []string
		for regionID, group := range groups {
			resp, regionErr, err := c.callBatchGet(ctx, group.region, group.keys, version)
			if err != nil {
				return nil, err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(regionID, regionErr)
				if lastErr != nil {
					return nil, lastErr
				}
				continue
			}
			responses := resp.GetResponses()
			for i, keyID := range group.ids {
				var getResp *pb.GetResponse
				if i < len(responses) && responses[i] != nil {
					getResp = responses[i]
				} else {
					getResp = &pb.GetResponse{NotFound: true}
				}
				results[keyID] = getResp
				completed = append(completed, keyID)
			}
		}
		for _, keyID := range completed {
			delete(pending, keyID)
		}
	}
	if len(pending) > 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("client: kv batch get retries exhausted")
	}
	return results, nil
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

func (c *Client) callBatchGet(ctx context.Context, region regionSnapshot, keys [][]byte, version uint64) (*pb.BatchGetResponse, *pb.RegionError, error) {
	if len(keys) == 0 {
		return &pb.BatchGetResponse{}, nil, nil
	}
	st, err := c.store(region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	request := &pb.BatchGetRequest{
		Requests: make([]*pb.GetRequest, 0, len(keys)),
	}
	for _, key := range keys {
		request.Requests = append(request.Requests, &pb.GetRequest{
			Key:     append([]byte(nil), key...),
			Version: version,
		})
	}
	resp, err := st.client.KvBatchGet(ctx, &pb.KvBatchGetRequest{
		Context: header,
		Request: request,
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	out := resp.GetResponse()
	if out == nil {
		out = &pb.BatchGetResponse{}
	}
	return out, resp.GetRegionError(), nil
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
			return &KeyConflictError{Errors: pr.GetErrors()}
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

// CheckTxnStatus inspects the primary lock for a transaction and returns the
// scheduler's decision (rollback, still alive, or already committed).
func (c *Client) CheckTxnStatus(ctx context.Context, primary []byte, lockTs, currentTs uint64) (*pb.CheckTxnStatusResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		region, err := c.regionForKey(primary)
		if err != nil {
			return nil, err
		}
		st, err := c.store(region.leader)
		if err != nil {
			return nil, err
		}
		header, err := buildContext(region)
		if err != nil {
			return nil, err
		}
		req := &pb.KvCheckTxnStatusRequest{
			Context: header,
			Request: &pb.CheckTxnStatusRequest{
				PrimaryKey:         append([]byte(nil), primary...),
				LockTs:             lockTs,
				CurrentTs:          currentTs,
				CallerStartTs:      currentTs,
				RollbackIfNotExist: true,
				CurrentTime:        uint64(time.Now().Unix()),
			},
		}
		resp, err := st.client.KvCheckTxnStatus(ctx, req)
		if err != nil {
			return nil, normalizeRPCError(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(region.meta.GetId(), regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			continue
		}
		return resp.GetResponse(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("client: check txn status retries exhausted")
}

// ResolveLocks attempts to resolve (commit or rollback) the provided keys for
// the given transaction versions. Keys are grouped by region automatically.
func (c *Client) ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	grouped := make(map[uint64][][]byte)
	for _, key := range keys {
		region, err := c.regionForKey(key)
		if err != nil {
			return 0, err
		}
		id := region.meta.GetId()
		grouped[id] = append(grouped[id], append([]byte(nil), key...))
	}
	var resolved uint64
	for regionID, regionKeys := range grouped {
		count, err := c.resolveRegionLocks(ctx, regionID, startVersion, commitVersion, regionKeys)
		if err != nil {
			return resolved, err
		}
		resolved += count
	}
	return resolved, nil
}

func (c *Client) resolveRegionLocks(ctx context.Context, regionID uint64, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return 0, fmt.Errorf("client: region %d missing for resolve", regionID)
		}
		st, err := c.store(region.leader)
		if err != nil {
			return 0, err
		}
		header, err := buildContext(region)
		if err != nil {
			return 0, err
		}
		req := &pb.KvResolveLockRequest{
			Context: header,
			Request: &pb.ResolveLockRequest{
				StartVersion:  startVersion,
				CommitVersion: commitVersion,
				Keys:          cloneKeys(keys),
			},
		}
		resp, err := st.client.KvResolveLock(ctx, req)
		if err != nil {
			return 0, normalizeRPCError(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return 0, lastErr
			}
			continue
		}
		if out := resp.GetResponse(); out != nil {
			if keyErr := out.GetError(); keyErr != nil {
				return 0, fmt.Errorf("client: resolve lock key error: %v", keyErr)
			}
			return out.GetResolvedLocks(), nil
		}
		return 0, nil
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, fmt.Errorf("client: resolve lock retries exhausted for region %d", regionID)
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
	if region, ok := c.regionForKeyFromCache(key); ok {
		return region, nil
	}
	if c.regionResolver == nil {
		return regionSnapshot{}, fmt.Errorf("client: region not found for key %q", key)
	}
	return c.regionForKeyFromResolver(key)
}

// regionForKeyFromCache scans cached Region state and returns a snapshot when
// the key is covered by an existing Region.
func (c *Client) regionForKeyFromCache(key []byte) (regionSnapshot, bool) {
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
			}, true
		}
	}
	return regionSnapshot{}, false
}

// regionForKeyFromResolver requests Region metadata from the external resolver,
// then caches the result for subsequent lookups.
func (c *Client) regionForKeyFromResolver(key []byte) (regionSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.routeLookupTimeout)
	defer cancel()
	resp, err := c.regionResolver.GetRegionByKey(ctx, &pb.GetRegionByKeyRequest{Key: append([]byte(nil), key...)})
	if err != nil {
		return regionSnapshot{}, fmt.Errorf("client: resolve region for key %q: %w", key, normalizeRPCError(err))
	}
	if resp == nil || resp.GetNotFound() || resp.GetRegion() == nil {
		return regionSnapshot{}, fmt.Errorf("client: region not found for key %q", key)
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
	c.regions[meta.GetId()] = &regionState{
		meta:   cloneRegionMeta(meta),
		leader: leader,
	}
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
		delete(c.regions, regionID)
		for _, meta := range epochMismatch.GetRegions() {
			if meta == nil {
				continue
			}
			c.regions[meta.GetId()] = &regionState{
				meta:   cloneRegionMeta(meta),
				leader: defaultLeaderStoreID(meta),
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
