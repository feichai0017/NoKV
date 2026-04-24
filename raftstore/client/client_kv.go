package client

import (
	"context"
	"errors"
	"fmt"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"time"

	"google.golang.org/protobuf/proto"
)

// Get issues a KvGet for the provided key/version. It retries on region errors.
func (c *Client) Get(ctx context.Context, key []byte, version uint64) (*kvrpcpb.GetResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.routeKeyWithRetry(ctx, key)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.callGet(ctx, region, key, version)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("client: kv get retries exhausted for key %q", key)
}

// BatchGet fetches multiple keys using the same snapshot version. Keys are
// grouped by region so that each group shares a single KvBatchGet round-trip
// and read index.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error) {
	results := make(map[string]*kvrpcpb.GetResponse, len(keys))
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
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups := make(map[uint64]*regionBatch)
		for keyID, key := range pending {
			region, err := c.routeKeyWithRetry(ctx, key)
			if err != nil {
				return nil, err
			}
			regionID := region.desc.RegionID
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
				if isTransportUnavailable(err) {
					lastErr = err
					continue
				}
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
				var getResp *kvrpcpb.GetResponse
				if i < len(responses) && responses[i] != nil {
					getResp = responses[i]
				} else {
					getResp = &kvrpcpb.GetResponse{NotFound: true}
				}
				results[keyID] = getResp
				completed = append(completed, keyID)
			}
		}
		for _, keyID := range completed {
			delete(pending, keyID)
		}
		if len(pending) > 0 {
			kind := retryRegionError
			if IsRouteUnavailable(lastErr) {
				kind = retryRouteUnavailable
			} else if isTransportUnavailable(lastErr) {
				kind = retryTransportUnavailable
			}
			if err := c.waitRetry(ctx, attempt, kind); err != nil {
				return nil, err
			}
		}
	}
	if len(pending) > 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("client: kv batch get retries exhausted")
	}
	return results, nil
}

func (c *Client) callGet(ctx context.Context, region regionSnapshot, key []byte, version uint64) (*kvrpcpb.GetResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	resp, err := cl.KvGet(ctx, &kvrpcpb.KvGetRequest{
		Context: header,
		Request: &kvrpcpb.GetRequest{
			Key:     append([]byte(nil), key...),
			Version: version,
		},
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) callBatchGet(ctx context.Context, region regionSnapshot, keys [][]byte, version uint64) (*kvrpcpb.BatchGetResponse, *errorpb.RegionError, error) {
	if len(keys) == 0 {
		return &kvrpcpb.BatchGetResponse{}, nil, nil
	}
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	request := &kvrpcpb.BatchGetRequest{Requests: make([]*kvrpcpb.GetRequest, 0, len(keys))}
	for _, key := range keys {
		request.Requests = append(request.Requests, &kvrpcpb.GetRequest{
			Key:     append([]byte(nil), key...),
			Version: version,
		})
	}
	resp, err := cl.KvBatchGet(ctx, &kvrpcpb.KvBatchGetRequest{Context: header, Request: request})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	out := resp.GetResponse()
	if out == nil {
		out = &kvrpcpb.BatchGetResponse{}
	}
	return out, resp.GetRegionError(), nil
}

// Scan issues a forward KvScan starting at startKey, reading up to limit keys.
func (c *Client) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*kvrpcpb.KV, error) {
	if limit == 0 {
		return nil, errInvalidScanLimit
	}
	collected := make([]*kvrpcpb.KV, 0, limit)
	currentKey := append([]byte(nil), startKey...)
	remaining := limit
	for remaining > 0 {
		region, err := c.routeKeyWithRetry(ctx, currentKey)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.callScan(ctx, region, currentKey, remaining, version)
		if err != nil {
			if isTransportUnavailable(err) {
				if err := c.waitRetry(ctx, 0, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if regionErr != nil {
			if err := c.handleRegionError(region.desc.RegionID, regionErr); err != nil {
				return nil, err
			}
			if err := c.waitRetry(ctx, 0, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		kvs := resp.GetKvs()
		collected = append(collected, kvs...)
		if len(kvs) == 0 {
			endKey := region.desc.EndKey
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
		endKey := region.desc.EndKey
		nextKey := incrementKey(kvs[len(kvs)-1].GetKey())
		if len(endKey) > 0 && bytesCompare(nextKey, endKey) >= 0 {
			currentKey = append([]byte(nil), endKey...)
			continue
		}
		currentKey = nextKey
	}
	return collected, nil
}

func (c *Client) callScan(ctx context.Context, region regionSnapshot, startKey []byte, limit uint32, version uint64) (*kvrpcpb.ScanResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	resp, err := cl.KvScan(ctx, &kvrpcpb.KvScanRequest{
		Context: header,
		Request: &kvrpcpb.ScanRequest{
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
func (c *Client) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	if len(primary) == 0 {
		return fmt.Errorf("client: primary key required")
	}
	cleaned := make([]*kvrpcpb.Mutation, 0, len(mutations))
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
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Mutation_Put,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
	return c.Mutate(ctx, key, []*kvrpcpb.Mutation{mut}, startVersion, commitVersion, lockTTL)
}

// Delete removes a key using a two-phase commit delete mutation.
func (c *Client) Delete(ctx context.Context, key []byte, startVersion, commitVersion, lockTTL uint64) error {
	mut := &kvrpcpb.Mutation{
		Op:  kvrpcpb.Mutation_Delete,
		Key: append([]byte(nil), key...),
	}
	return c.Mutate(ctx, key, []*kvrpcpb.Mutation{mut}, startVersion, commitVersion, lockTTL)
}

// TwoPhaseCommit runs Prewrite followed by Commit across the supplied mutations.
func (c *Client) TwoPhaseCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	if len(mutations) == 0 {
		return nil
	}
	cleaned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	var primaryMutation *kvrpcpb.Mutation
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		cloned := cloneMutation(mut)
		if primaryMutation == nil && bytesCompare(cloned.GetKey(), primary) == 0 {
			primaryMutation = cloned
		}
		cleaned = append(cleaned, cloned)
	}
	if primaryMutation == nil {
		return fmt.Errorf("client: primary key %q missing from mutations", primary)
	}
	for _, mut := range cleaned {
		if _, err := c.routeKeyWithRetry(ctx, mut.GetKey()); err != nil {
			return err
		}
	}
	prewritten, err := c.prewriteMutationsByRoute(ctx, primary, startVersion, lockTTL, []*kvrpcpb.Mutation{primaryMutation})
	if err != nil {
		return err
	}
	secondaryMutations := make([]*kvrpcpb.Mutation, 0, len(cleaned)-1)
	primarySkipped := false
	for _, mut := range cleaned {
		if !primarySkipped && bytesCompare(mut.GetKey(), primary) == 0 {
			primarySkipped = true
			continue
		}
		secondaryMutations = append(secondaryMutations, mut)
	}
	if len(secondaryMutations) > 0 {
		secondaryPrewritten, err := c.prewriteMutationsByRoute(ctx, primary, startVersion, lockTTL, secondaryMutations)
		mergePrewritten(prewritten, secondaryPrewritten)
		if err != nil {
			if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("client: rollback after prewrite failure: %w", rollbackErr))
			}
			return err
		}
	}
	if err := c.commitKeysByRoute(ctx, [][]byte{append([]byte(nil), primary...)}, startVersion, commitVersion); err != nil {
		return err
	}
	secondaryKeys := collectKeys(secondaryMutations)
	if err := c.commitKeysByRoute(ctx, secondaryKeys, startVersion, commitVersion); err != nil {
		if resolveErr := c.resolveCommittedSecondaries(ctx, secondaryKeys, startVersion, commitVersion); resolveErr != nil {
			return errors.Join(err, fmt.Errorf("client: resolve committed secondaries: %w", resolveErr))
		}
		return nil
	}
	return nil
}

type mutationRouteBatch struct {
	region    regionSnapshot
	mutations []*kvrpcpb.Mutation
}

type keyRouteBatch struct {
	region regionSnapshot
	keys   [][]byte
}

func (c *Client) prewriteMutationsByRoute(ctx context.Context, primary []byte, startVersion, ttl uint64, mutations []*kvrpcpb.Mutation) (map[uint64][][]byte, error) {
	pending := cloneMutations(mutations)
	prewritten := make(map[uint64][][]byte)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupMutationsByRoute(ctx, pending)
		if err != nil {
			return prewritten, err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.prewriteRegionOnce(ctx, group.region, primary, startVersion, ttl, group.mutations)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return prewritten, err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return prewritten, lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if resp != nil && len(resp.GetErrors()) > 0 {
				return prewritten, &KeyConflictError{Errors: resp.GetErrors()}
			}
			prewritten[group.region.desc.RegionID] = append(prewritten[group.region.desc.RegionID], collectKeys(group.mutations)...)
			pending = removeMutationSet(pending, group.mutations)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return prewritten, err
			}
		}
	}
	if len(pending) == 0 {
		return prewritten, nil
	}
	if lastErr != nil {
		return prewritten, lastErr
	}
	if err := ctx.Err(); err != nil {
		return prewritten, err
	}
	return prewritten, fmt.Errorf("client: prewrite retries exhausted")
}

func (c *Client) rollbackPrewrites(ctx context.Context, prewritten map[uint64][][]byte, startVersion uint64) error {
	return c.rollbackKeysByRoute(ctx, flattenPrewritten(prewritten), startVersion)
}

func (c *Client) resolveCommittedSecondaries(ctx context.Context, keys [][]byte, startVersion, commitVersion uint64) error {
	_, err := c.ResolveLocks(ctx, startVersion, commitVersion, keys)
	return err
}

func (c *Client) prewriteRegion(ctx context.Context, regionID uint64, primary []byte, startVersion, ttl uint64, muts []*kvrpcpb.Mutation) error {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return fmt.Errorf("client: region %d missing for prewrite", regionID)
		}
		resp, regionErr, err := c.prewriteRegionOnce(ctx, region, primary, startVersion, ttl, muts)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return err
			}
			continue
		}
		if resp != nil && len(resp.GetErrors()) > 0 {
			return &KeyConflictError{Errors: resp.GetErrors()}
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("client: prewrite retries exhausted for region %d", regionID)
}

func (c *Client) prewriteRegionOnce(ctx context.Context, region regionSnapshot, primary []byte, startVersion, ttl uint64, muts []*kvrpcpb.Mutation) (*kvrpcpb.PrewriteResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvPrewriteRequest{
		Context: header,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations:    cloneMutations(muts),
			PrimaryLock:  append([]byte(nil), primary...),
			StartVersion: startVersion,
			LockTtl:      ttl,
		},
	}
	resp, err := cl.KvPrewrite(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) commitRegion(ctx context.Context, regionID uint64, keys [][]byte, startVersion, commitVersion uint64) error {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return fmt.Errorf("client: region %d missing for commit", regionID)
		}
		resp, regionErr, err := c.commitRegionOnce(ctx, region, keys, startVersion, commitVersion)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return err
			}
			continue
		}
		if resp != nil && resp.GetError() != nil {
			return fmt.Errorf("client: commit key error: %v", resp.GetError())
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("client: commit retries exhausted for region %d", regionID)
}

func (c *Client) commitKeysByRoute(ctx context.Context, keys [][]byte, startVersion, commitVersion uint64) error {
	pending := cloneKeys(keys)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.commitRegionOnce(ctx, group.region, group.keys, startVersion, commitVersion)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if resp != nil && resp.GetError() != nil {
				return fmt.Errorf("client: commit key error: %v", resp.GetError())
			}
			pending = removeKeySet(pending, group.keys)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return err
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("client: commit retries exhausted")
}

func (c *Client) commitRegionOnce(ctx context.Context, region regionSnapshot, keys [][]byte, startVersion, commitVersion uint64) (*kvrpcpb.CommitResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvCommitRequest{
		Context: header,
		Request: &kvrpcpb.CommitRequest{
			Keys:          cloneKeys(keys),
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
		},
	}
	resp, err := cl.KvCommit(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) batchRollbackRegion(ctx context.Context, regionID uint64, keys [][]byte, startVersion uint64) error {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return fmt.Errorf("client: region %d missing for rollback", regionID)
		}
		resp, regionErr, err := c.batchRollbackRegionOnce(ctx, region, keys, startVersion)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return err
			}
			continue
		}
		if resp != nil && resp.GetError() != nil {
			return fmt.Errorf("client: rollback key error: %v", resp.GetError())
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("client: rollback retries exhausted for region %d", regionID)
}

func (c *Client) rollbackKeysByRoute(ctx context.Context, keys [][]byte, startVersion uint64) error {
	pending := cloneKeys(keys)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.batchRollbackRegionOnce(ctx, group.region, group.keys, startVersion)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if resp != nil && resp.GetError() != nil {
				return fmt.Errorf("client: rollback key error: %v", resp.GetError())
			}
			pending = removeKeySet(pending, group.keys)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return err
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("client: rollback retries exhausted")
}

func (c *Client) batchRollbackRegionOnce(ctx context.Context, region regionSnapshot, keys [][]byte, startVersion uint64) (*kvrpcpb.BatchRollbackResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvBatchRollbackRequest{
		Context: header,
		Request: &kvrpcpb.BatchRollbackRequest{
			Keys:         cloneKeys(keys),
			StartVersion: startVersion,
		},
	}
	resp, err := cl.KvBatchRollback(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

// CheckTxnStatus inspects the primary lock for a transaction and returns the
// scheduler's decision (rollback, still alive, or already committed).
func (c *Client) CheckTxnStatus(ctx context.Context, primary []byte, lockTs, currentTs uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.routeKeyWithRetry(ctx, primary)
		if err != nil {
			return nil, err
		}
		cl, err := c.storeClient(ctx, region.leader)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		header, err := buildContext(region)
		if err != nil {
			return nil, err
		}
		req := &kvrpcpb.KvCheckTxnStatusRequest{
			Context: header,
			Request: &kvrpcpb.CheckTxnStatusRequest{
				PrimaryKey:         append([]byte(nil), primary...),
				LockTs:             lockTs,
				CurrentTs:          currentTs,
				CallerStartTs:      currentTs,
				RollbackIfNotExist: true,
				CurrentTime:        uint64(time.Now().Unix()),
			},
		}
		resp, err := cl.KvCheckTxnStatus(ctx, req)
		if err != nil {
			return nil, normalizeRPCError(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		return resp.GetResponse(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("client: check txn status retries exhausted")
}

// ResolveLocks attempts to resolve (commit or rollback) the provided keys for
// the given transaction versions. Keys are grouped by region automatically.
func (c *Client) ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	return c.resolveLocksByRoute(ctx, startVersion, commitVersion, keys)
}

func (c *Client) resolveLocksByRoute(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	pending := cloneKeys(keys)
	var resolved uint64
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return resolved, err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.resolveRegionLocksOnce(ctx, group.region, startVersion, commitVersion, group.keys)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return resolved, err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return resolved, lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if resp != nil {
				if keyErr := resp.GetError(); keyErr != nil {
					return resolved, fmt.Errorf("client: resolve lock key error: %v", keyErr)
				}
				resolved += resp.GetResolvedLocks()
			}
			pending = removeKeySet(pending, group.keys)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return resolved, err
			}
		}
	}
	if len(pending) == 0 {
		return resolved, nil
	}
	if lastErr != nil {
		return resolved, lastErr
	}
	if err := ctx.Err(); err != nil {
		return resolved, err
	}
	return resolved, fmt.Errorf("client: resolve lock retries exhausted")
}

func (c *Client) resolveRegionLocksOnce(ctx context.Context, region regionSnapshot, startVersion, commitVersion uint64, keys [][]byte) (*kvrpcpb.ResolveLockResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvResolveLockRequest{
		Context: header,
		Request: &kvrpcpb.ResolveLockRequest{
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
			Keys:          cloneKeys(keys),
		},
	}
	resp, err := cl.KvResolveLock(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func cloneMutation(mut *kvrpcpb.Mutation) *kvrpcpb.Mutation {
	if mut == nil {
		return nil
	}
	return proto.Clone(mut).(*kvrpcpb.Mutation)
}

func cloneMutations(mutations []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	out := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		out = append(out, cloneMutation(mut))
	}
	return out
}

func cloneKeys(keys [][]byte) [][]byte {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		out[i] = append([]byte(nil), key...)
	}
	return out
}

func collectKeys(muts []*kvrpcpb.Mutation) [][]byte {
	out := make([][]byte, 0, len(muts))
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		out = append(out, append([]byte(nil), mut.GetKey()...))
	}
	return out
}

func mutationHasPrimary(muts []*kvrpcpb.Mutation, primary []byte) bool {
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

func (c *Client) groupMutationsByRoute(ctx context.Context, mutations []*kvrpcpb.Mutation) (map[uint64]*mutationRouteBatch, error) {
	groups := make(map[uint64]*mutationRouteBatch)
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		region, err := c.routeKeyWithRetry(ctx, mut.GetKey())
		if err != nil {
			return nil, err
		}
		id := region.desc.RegionID
		group := groups[id]
		if group == nil {
			group = &mutationRouteBatch{region: region}
			groups[id] = group
		}
		group.mutations = append(group.mutations, mut)
	}
	return groups, nil
}

func (c *Client) groupKeysByRoute(ctx context.Context, keys [][]byte) (map[uint64]*keyRouteBatch, error) {
	groups := make(map[uint64]*keyRouteBatch)
	for _, key := range keys {
		region, err := c.routeKeyWithRetry(ctx, key)
		if err != nil {
			return nil, err
		}
		id := region.desc.RegionID
		group := groups[id]
		if group == nil {
			group = &keyRouteBatch{region: region}
			groups[id] = group
		}
		group.keys = append(group.keys, append([]byte(nil), key...))
	}
	return groups, nil
}

func removeMutationSet(pending, completed []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	done := make(map[*kvrpcpb.Mutation]struct{}, len(completed))
	for _, mut := range completed {
		done[mut] = struct{}{}
	}
	out := pending[:0]
	for _, mut := range pending {
		if _, ok := done[mut]; ok {
			continue
		}
		out = append(out, mut)
	}
	return out
}

func removeKeySet(pending, completed [][]byte) [][]byte {
	done := make(map[string]struct{}, len(completed))
	for _, key := range completed {
		done[string(key)] = struct{}{}
	}
	out := pending[:0]
	for _, key := range pending {
		if _, ok := done[string(key)]; ok {
			continue
		}
		out = append(out, key)
	}
	return out
}

func mergePrewritten(dst, src map[uint64][][]byte) {
	for regionID, keys := range src {
		dst[regionID] = append(dst[regionID], cloneKeys(keys)...)
	}
}

func flattenPrewritten(prewritten map[uint64][][]byte) [][]byte {
	var keys [][]byte
	for _, regionKeys := range prewritten {
		keys = append(keys, cloneKeys(regionKeys)...)
	}
	return keys
}
