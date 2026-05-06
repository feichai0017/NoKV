package client

import (
	"context"
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"

	"google.golang.org/protobuf/proto"
)

// Get issues a StoreKV Get RPC for the provided key/version. It retries on region errors.
func (c *Client) Get(ctx context.Context, key []byte, version uint64) (*kvrpcpb.GetResponse, error) {
	var lastErr error
	var lastKeyErr *kvrpcpb.KeyError
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
		if keyErr := resp.GetError(); keyErr != nil {
			if locked := keyErr.GetLocked(); locked != nil {
				lastKeyErr = keyErr
				resolved, err := c.resolveReadLock(ctx, locked, version)
				if err != nil && !errors.Is(err, errReadLockStillLive) {
					return nil, err
				}
				if !resolved {
					if err := c.waitRetry(ctx, attempt, retryLockResolve); err != nil {
						return nil, err
					}
				}
				continue
			}
			return nil, txnKeyError(keyErr)
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if lastKeyErr != nil {
		return nil, txnKeyError(lastKeyErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "kv get", Key: append([]byte(nil), key...)}
}

// BatchGet fetches multiple keys using the same snapshot version. Keys are
// grouped by region so that each group shares a single BatchGet round-trip
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
	var lastKeyErr *kvrpcpb.KeyError
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
				if keyErr := getResp.GetError(); keyErr != nil {
					if locked := keyErr.GetLocked(); locked != nil {
						lastKeyErr = keyErr
						resolved, err := c.resolveReadLock(ctx, locked, version)
						if err != nil && !errors.Is(err, errReadLockStillLive) {
							return nil, err
						}
						if !resolved {
							lastErr = txnKeyError(keyErr)
						}
						continue
					}
					return nil, txnKeyError(keyErr)
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
			} else if lastKeyErr != nil {
				kind = retryLockResolve
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
		if lastKeyErr != nil {
			return nil, txnKeyError(lastKeyErr)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, &RetryExhaustedError{Operation: "kv batch get"}
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
	resp, err := cl.Get(ctx, &kvrpcpb.KvGetRequest{
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
	resp, err := cl.BatchGet(ctx, &kvrpcpb.KvBatchGetRequest{Context: header, Request: request})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	out := resp.GetResponse()
	if out == nil {
		out = &kvrpcpb.BatchGetResponse{}
	}
	return out, resp.GetRegionError(), nil
}

// Scan issues a forward StoreKV Scan RPC starting at startKey, reading up to limit keys.
func (c *Client) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*kvrpcpb.KV, error) {
	if limit == 0 {
		return nil, errInvalidScanLimit
	}
	collected := make([]*kvrpcpb.KV, 0, limit)
	currentKey := append([]byte(nil), startKey...)
	remaining := limit
	lockAttempts := 0
	lastLock := ""
	var lastKeyErr *kvrpcpb.KeyError
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
		if keyErr := resp.GetError(); keyErr != nil {
			if locked := keyErr.GetLocked(); locked != nil {
				lastKeyErr = keyErr
				lockID := readLockFingerprint(locked)
				if lockID != lastLock {
					lastLock = lockID
					lockAttempts = 0
				}
				if lockAttempts >= c.retry.MaxAttempts {
					return nil, txnKeyError(lastKeyErr)
				}
				resolved, err := c.resolveReadLock(ctx, locked, version)
				if err != nil && !errors.Is(err, errReadLockStillLive) {
					return nil, err
				}
				if !resolved {
					if err := c.waitRetry(ctx, lockAttempts, retryLockResolve); err != nil {
						return nil, err
					}
				}
				lockAttempts++
				continue
			}
			return nil, txnKeyError(keyErr)
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

func readLockFingerprint(locked *kvrpcpb.Locked) string {
	return string(locked.GetKey()) + "\x00" + fmt.Sprint(locked.GetLockVersion())
}

func (c *Client) resolveReadLock(ctx context.Context, locked *kvrpcpb.Locked, readTs uint64) (bool, error) {
	if locked == nil {
		return false, nil
	}
	lockKey := locked.GetKey()
	if len(lockKey) == 0 || len(locked.GetPrimaryLock()) == 0 || locked.GetLockVersion() == 0 {
		return false, txnKeyError(&kvrpcpb.KeyError{Locked: locked})
	}
	statusResp, err := c.CheckTxnStatus(ctx, locked.GetPrimaryLock(), locked.GetLockVersion(), readTs, 0)
	if err != nil {
		return false, err
	}
	if statusResp == nil {
		return false, &ProtocolError{Operation: "resolve read lock", Detail: fmt.Sprintf("nil check txn status response for primary %q", locked.GetPrimaryLock())}
	}
	if keyErr := statusResp.GetError(); keyErr != nil {
		return false, txnKeyError(keyErr)
	}
	commitVersion := statusResp.GetCommitVersion()
	switch {
	case commitVersion > 0:
		_, err = c.ResolveLocks(ctx, locked.GetLockVersion(), commitVersion, [][]byte{lockKey})
		return err == nil, err
	case statusResp.GetAction() == kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback ||
		statusResp.GetAction() == kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback:
		_, err = c.ResolveLocks(ctx, locked.GetLockVersion(), 0, [][]byte{lockKey})
		return err == nil, err
	default:
		return false, errReadLockStillLive
	}
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
	resp, err := cl.Scan(ctx, &kvrpcpb.KvScanRequest{
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
	return c.mutateWithCommitTimestamp(ctx, primary, mutations, startVersion, lockTTL, func(context.Context) (uint64, error) {
		return commitVersion, nil
	})
}

// MutateWithCommitTimestamp runs a 2PC mutation and obtains commit_ts after all
// prewrites have reached Raft. This is the strict Percolator timestamp boundary:
// readers may push MinCommitTs while locks are live, so fsmeta uses this path to
// avoid exhausting logical-operation retries under read/write contention.
func (c *Client) MutateWithCommitTimestamp(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) error {
	if allocateCommitVersion == nil {
		return &ProtocolError{Operation: "mutate", Detail: "commit timestamp allocator required"}
	}
	return c.mutateWithCommitTimestamp(ctx, primary, mutations, startVersion, lockTTL, allocateCommitVersion)
}

func (c *Client) mutateWithCommitTimestamp(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) error {
	if len(primary) == 0 {
		return &ProtocolError{Operation: "mutate", Detail: "primary key required"}
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
		return &ProtocolError{Operation: "mutate", Detail: fmt.Sprintf("primary key %q not present in mutations", primary)}
	}
	return c.twoPhaseCommit(ctx, append([]byte(nil), primary...), cleaned, startVersion, lockTTL, allocateCommitVersion)
}

// TryAtomicMutate attempts to materialize mutations as one region-local 1PC
// Raft command. It returns handled=false when routing or local storage
// atomicity cannot be proven, allowing callers to fall back to regular 2PC.
func (c *Client) TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
	if len(primary) == 0 {
		return false, &ProtocolError{Operation: "atomic mutate", Detail: "primary key required"}
	}
	cleaned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		cleaned = append(cleaned, cloneMutation(mut))
	}
	if len(cleaned) == 0 {
		return true, nil
	}
	preds := cloneAtomicPredicates(predicates)
	if !atomicMutateHasPrimary(cleaned, preds, primary) {
		return false, &ProtocolError{Operation: "atomic mutate", Detail: fmt.Sprintf("primary key %q not present in mutations or predicates", primary)}
	}
	var lastErr error
	ambiguous := false
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		groups, err := c.groupAtomicMutateByRoute(ctx, cleaned, preds)
		if err != nil {
			return false, err
		}
		if len(groups) != 1 {
			c.atomicRouteMultiTotal.Add(1)
			if ambiguous {
				return true, &RetryExhaustedError{Operation: "atomic mutate"}
			}
			return false, nil
		}
		c.atomicRouteSingleTotal.Add(1)
		var group *mutationRouteBatch
		for _, candidate := range groups {
			group = candidate
		}
		resp, regionErr, err := c.tryAtomicMutateRegionOnce(ctx, group.region, preds, cleaned, startVersion, commitVersion)
		if err != nil {
			if isTransportUnavailable(err) {
				ambiguous = true
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return true, err
				}
				continue
			}
			return true, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
			if lastErr != nil {
				return true, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return true, err
			}
			continue
		}
		if resp.GetFallbackToTwoPhaseCommit() {
			c.atomicLocalFallbackTotal.Add(1)
			return false, nil
		}
		if err := txnKeyError(resp.GetError()); err != nil {
			return true, err
		}
		c.atomicSuccessTotal.Add(1)
		return true, nil
	}
	if lastErr != nil {
		return true, lastErr
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return true, &RetryExhaustedError{Operation: "atomic mutate"}
}

func (c *Client) tryAtomicMutateRegionOnce(ctx context.Context, region regionSnapshot, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (*kvrpcpb.TryAtomicMutateResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvTryAtomicMutateRequest{
		Context: header,
		Request: &kvrpcpb.TryAtomicMutateRequest{
			Predicates:    cloneAtomicPredicates(predicates),
			Mutations:     cloneMutations(mutations),
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
		},
	}
	resp, err := cl.TryAtomicMutate(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("atomic mutate", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("atomic mutate", region.desc.RegionID, "missing atomic mutate payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
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
	return c.twoPhaseCommit(ctx, primary, mutations, startVersion, lockTTL, func(context.Context) (uint64, error) {
		return commitVersion, nil
	})
}

func (c *Client) twoPhaseCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) error {
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
		return &ProtocolError{Operation: "two phase commit", Detail: fmt.Sprintf("primary key %q missing from mutations", primary)}
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
	commitVersion, err := allocateCommitVersion(ctx)
	if err != nil {
		if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("client: rollback after commit timestamp allocation failure: %w", rollbackErr))
		}
		return err
	}
	if commitVersion <= startVersion {
		err := &ProtocolError{Operation: "two phase commit", Detail: fmt.Sprintf("commit version %d must be greater than start version %d", commitVersion, startVersion)}
		if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("client: rollback after invalid commit timestamp: %w", rollbackErr))
		}
		return err
	}
	if err := c.commitKeysByRoute(ctx, [][]byte{append([]byte(nil), primary...)}, startVersion, commitVersion); err != nil {
		if shouldRollbackAfterPrimaryCommitFailure(err) {
			if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("client: rollback after primary commit failure: %w", rollbackErr))
			}
		}
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
				return prewritten, nokverrors.NewTxnKeyError(resp.GetErrors()...)
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
	return prewritten, &RetryExhaustedError{Operation: "prewrite"}
}

func (c *Client) rollbackPrewrites(ctx context.Context, prewritten map[uint64][][]byte, startVersion uint64) error {
	return c.rollbackKeysByRoute(ctx, flattenPrewritten(prewritten), startVersion)
}

func (c *Client) resolveCommittedSecondaries(ctx context.Context, keys [][]byte, startVersion, commitVersion uint64) error {
	_, err := c.ResolveLocks(ctx, startVersion, commitVersion, keys)
	return err
}

func shouldRollbackAfterPrimaryCommitFailure(err error) bool {
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	if !ok {
		return false
	}
	for _, keyErr := range txnErr.Errors {
		if keyErr != nil && keyErr.GetCommitTsExpired() != nil {
			return true
		}
	}
	return false
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
	resp, err := cl.Prewrite(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("prewrite", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("prewrite", region.desc.RegionID, "missing prewrite payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) commitRegion(ctx context.Context, regionID uint64, keys [][]byte, startVersion, commitVersion uint64) error {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return &ProtocolError{Operation: "commit", Detail: fmt.Sprintf("region %d missing from cache", regionID)}
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
		if err := txnKeyError(resp.GetError()); err != nil {
			return err
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return &RetryExhaustedError{Operation: "commit", RegionID: regionID}
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
			if err := txnKeyError(resp.GetError()); err != nil {
				return err
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
	return &RetryExhaustedError{Operation: "commit"}
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
	resp, err := cl.Commit(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("commit", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("commit", region.desc.RegionID, "missing commit payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
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
			if err := txnKeyError(resp.GetError()); err != nil {
				return err
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
	return &RetryExhaustedError{Operation: "rollback"}
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
	resp, err := cl.BatchRollback(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("rollback", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("rollback", region.desc.RegionID, "missing rollback payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

// CheckTxnStatus inspects the primary lock for a transaction and returns the
// scheduler's decision (rollback, still alive, or already committed).
func (c *Client) CheckTxnStatus(ctx context.Context, primary []byte, lockTs, currentTs, currentTime uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
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
				CurrentTime:        currentTime,
			},
		}
		resp, err := cl.CheckTxnStatus(ctx, req)
		if err != nil {
			return nil, normalizeRPCError(err)
		}
		if resp == nil {
			return nil, kvPayloadProtocolError("check txn status", region.desc.RegionID, "nil kv response")
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
		if resp.GetResponse() == nil {
			return nil, kvPayloadProtocolError("check txn status", region.desc.RegionID, "missing check txn status payload")
		}
		return resp.GetResponse(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "check txn status"}
}

// TxnHeartBeat extends the primary lock TTL for a live transaction.
func (c *Client) TxnHeartBeat(ctx context.Context, primary []byte, startVersion, ttlExtension uint64) (*kvrpcpb.TxnHeartBeatResponse, error) {
	return c.txnHeartBeatAt(ctx, primary, startVersion, ttlExtension, 0)
}

func (c *Client) txnHeartBeatAt(ctx context.Context, primary []byte, startVersion, ttlExtension, currentTime uint64) (*kvrpcpb.TxnHeartBeatResponse, error) {
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
		req := &kvrpcpb.KvTxnHeartBeatRequest{
			Context: header,
			Request: &kvrpcpb.TxnHeartBeatRequest{
				PrimaryKey:   append([]byte(nil), primary...),
				StartVersion: startVersion,
				TtlExtension: ttlExtension,
				CurrentTime:  currentTime,
			},
		}
		resp, err := cl.TxnHeartBeat(ctx, req)
		if err != nil {
			return nil, normalizeRPCError(err)
		}
		if resp == nil {
			return nil, kvPayloadProtocolError("txn heartbeat", region.desc.RegionID, "nil kv response")
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
		if resp.GetResponse() == nil {
			return nil, kvPayloadProtocolError("txn heartbeat", region.desc.RegionID, "missing txn heartbeat payload")
		}
		return resp.GetResponse(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "txn heartbeat"}
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
					return resolved, txnKeyError(keyErr)
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
	return resolved, &RetryExhaustedError{Operation: "resolve lock"}
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
	resp, err := cl.ResolveLock(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("resolve lock", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("resolve lock", region.desc.RegionID, "missing resolve lock payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func kvPayloadProtocolError(operation string, regionID uint64, detail string) error {
	return &ProtocolError{Operation: operation, Detail: fmt.Sprintf("region %d: %s", regionID, detail)}
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

func cloneAtomicPredicate(pred *kvrpcpb.AtomicPredicate) *kvrpcpb.AtomicPredicate {
	if pred == nil {
		return nil
	}
	return proto.Clone(pred).(*kvrpcpb.AtomicPredicate)
}

func cloneAtomicPredicates(predicates []*kvrpcpb.AtomicPredicate) []*kvrpcpb.AtomicPredicate {
	out := make([]*kvrpcpb.AtomicPredicate, 0, len(predicates))
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		out = append(out, cloneAtomicPredicate(pred))
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

func atomicMutateHasPrimary(muts []*kvrpcpb.Mutation, preds []*kvrpcpb.AtomicPredicate, primary []byte) bool {
	if mutationHasPrimary(muts, primary) {
		return true
	}
	for _, pred := range preds {
		if pred == nil {
			continue
		}
		if bytesCompare(pred.GetKey(), primary) == 0 {
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

func (c *Client) groupAtomicMutateByRoute(ctx context.Context, mutations []*kvrpcpb.Mutation, predicates []*kvrpcpb.AtomicPredicate) (map[uint64]*mutationRouteBatch, error) {
	groups := make(map[uint64]*mutationRouteBatch)
	addKey := func(key []byte) error {
		region, err := c.routeKeyWithRetry(ctx, key)
		if err != nil {
			return err
		}
		id := region.desc.RegionID
		group := groups[id]
		if group == nil {
			group = &mutationRouteBatch{region: region}
			groups[id] = group
		}
		return nil
	}
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		if err := addKey(mut.GetKey()); err != nil {
			return nil, err
		}
	}
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		if err := addKey(pred.GetKey()); err != nil {
			return nil, err
		}
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
