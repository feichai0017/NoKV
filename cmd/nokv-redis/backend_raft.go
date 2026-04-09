package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/raftstore/client"
)

type raftBackend struct {
	ctx    context.Context
	client raftClient
	ts     timestampAllocator
}

type timestampAllocator interface {
	Reserve(n uint64) (uint64, error)
}

type coordinatorTSOClient interface {
	Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
}

type raftClient interface {
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error)
	Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
	CheckTxnStatus(ctx context.Context, primary []byte, lockVersion, currentTS uint64) (*kvrpcpb.CheckTxnStatusResponse, error)
	ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error)
	Close() error
}

type coordinatorTSOAllocator struct {
	ctx     context.Context
	client  coordinatorTSOClient
	timeout time.Duration
}

func newCoordinatorTSOAllocator(ctx context.Context, client coordinatorTSOClient, timeout time.Duration) *coordinatorTSOAllocator {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &coordinatorTSOAllocator{
		ctx:     ctx,
		client:  client,
		timeout: timeout,
	}
}

func (p *coordinatorTSOAllocator) Reserve(n uint64) (uint64, error) {
	if n == 0 {
		return 0, fmt.Errorf("tso reserve: n must be >= 1")
	}
	if p == nil || p.client == nil {
		return 0, fmt.Errorf("tso reserve: allocator not initialized")
	}
	ctx, cancel := contextWithTimeout(p.ctx, p.timeout)
	defer cancel()
	resp, err := p.client.Tso(ctx, &coordpb.TsoRequest{Count: n})
	if err != nil {
		return 0, fmt.Errorf("tso reserve: rpc failed: %w", err)
	}
	if resp == nil {
		return 0, fmt.Errorf("tso reserve: empty response")
	}
	if resp.GetCount() < n {
		return 0, fmt.Errorf("tso reserve: requested %d timestamps, got %d", n, resp.GetCount())
	}
	return resp.GetTimestamp(), nil
}

func newRaftBackend(ctx context.Context, cfgPath, coordAddr, addrScope string) (*raftBackend, error) {
	cfgFile, err := config.LoadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("raft backend: read config: %w", err)
	}
	if err := cfgFile.Validate(); err != nil {
		return nil, fmt.Errorf("raft backend: config invalid: %w", err)
	}
	cfg := client.Config{
		Context: ctx,
		Retry: client.RetryPolicy{
			MaxAttempts: cfgFile.MaxRetries,
		},
	}
	for _, st := range cfgFile.Stores {
		addr := strings.TrimSpace(st.Addr)
		if strings.EqualFold(addrScope, "docker") && st.DockerAddr != "" {
			addr = strings.TrimSpace(st.DockerAddr)
		}
		cfg.Stores = append(cfg.Stores, client.StoreEndpoint{
			StoreID: st.StoreID,
			Addr:    addr,
		})
	}
	// Route source is converged to the Coordinator resolver. raft_config regions are treated
	// as bootstrap/deployment metadata and are not used as runtime routing truth.
	coordAddr = strings.TrimSpace(coordAddr)
	if coordAddr == "" {
		coordAddr = cfgFile.ResolveCoordinatorAddr(addrScope)
	}
	if coordAddr == "" {
		return nil, fmt.Errorf("raft backend: coordinator-addr is required in raft mode (flag or config.coordinator)")
	}
	dialCtx, cancel := contextWithTimeout(ctx, 3*time.Second)
	coordCli, err := coordclient.NewGRPCClient(dialCtx, coordAddr)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("raft backend: init coordinator client: %w", err)
	}
	cfg.RegionResolver = coordCli
	cl, err := client.New(cfg)
	if err != nil {
		_ = coordCli.Close()
		return nil, fmt.Errorf("raft backend: init client: %w", err)
	}

	return &raftBackend{
		ctx:    ctx,
		client: cl,
		ts:     newCoordinatorTSOAllocator(ctx, coordCli, 3*time.Second),
	}, nil
}

func decodeKey(val string) []byte {
	val = strings.TrimSpace(val)
	if val == "" || val == "-" {
		return nil
	}
	if strings.HasPrefix(val, "hex:") {
		raw, err := hex.DecodeString(val[4:])
		if err == nil {
			return raw
		}
	}
	if out, err := base64.StdEncoding.DecodeString(val); err == nil {
		return out
	}
	return []byte(val)
}

func (b *raftBackend) context() (context.Context, context.CancelFunc) {
	return contextWithTimeout(b.ctx, 3*time.Second)
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

func (b *raftBackend) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}

func (b *raftBackend) Get(key []byte) (*redisValue, error) {
	version, err := b.reserveTimestamp(1)
	if err != nil {
		return nil, err
	}
	return b.getAtVersion(key, version)
}

func (b *raftBackend) Set(args setArgs) (bool, error) {
	if len(args.Key) == 0 {
		return false, fmt.Errorf("empty key")
	}
	if args.NX || args.XX {
		// Mirror Redis semantics: the existence check must observe the latest
		// committed value before we attempt the write.
		existing, err := b.Get(args.Key)
		if err != nil {
			return false, err
		}
		if args.NX && existing.Found {
			return false, nil
		}
		if args.XX && !existing.Found {
			return false, nil
		}
	}

	valueKey := append([]byte(nil), args.Key...)
	valueCopy := append([]byte(nil), args.Value...)
	expireAt := args.ExpireAt
	if expireAt == 0 && args.TTL > 0 {
		now := time.Now()
		expireAt = uint64(now.Add(args.TTL).Unix())
		if expireAt <= uint64(now.Unix()) {
			expireAt = uint64(now.Add(time.Second).Unix())
		}
	}
	mutations := []*kvrpcpb.Mutation{{
		Op:        kvrpcpb.Mutation_Put,
		Key:       valueKey,
		Value:     valueCopy,
		ExpiresAt: expireAt,
	}}

	if err := b.mutate(valueKey, mutations...); err != nil {
		return false, err
	}
	return true, nil
}

func (b *raftBackend) Del(keys [][]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	version, err := b.reserveTimestamp(1)
	if err != nil {
		return 0, err
	}

	resps, err := b.batchGetWithRetry(keys, version)
	if err != nil {
		return 0, err
	}

	mutations := make([]*kvrpcpb.Mutation, 0, len(keys))
	var removed int64
	for _, key := range keys {
		resp := resps[string(key)]
		if resp != nil && !resp.GetNotFound() && resp.GetError() == nil {
			removed++
		}
		valueKey := append([]byte(nil), key...)
		mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: valueKey})
	}
	if len(mutations) == 0 {
		return removed, nil
	}
	if err := b.mutate(append([]byte(nil), keys[0]...), mutations...); err != nil {
		return 0, err
	}
	return removed, nil
}

func (b *raftBackend) MGet(keys [][]byte) ([]*redisValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	version, err := b.reserveTimestamp(1)
	if err != nil {
		return nil, err
	}
	request := make([][]byte, 0, len(keys))
	valueKeys := make([][]byte, len(keys))
	for i, key := range keys {
		valueKey := append([]byte(nil), key...)
		valueKeys[i] = valueKey
		request = append(request, valueKey)
	}

	resps, err := b.batchGetWithRetry(request, version)
	if err != nil {
		return nil, err
	}
	out := make([]*redisValue, len(keys))
	for i, key := range keys {
		valueResp := resps[string(valueKeys[i])]
		val, err := b.buildValueAtVersion(key, valueResp)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

func (b *raftBackend) MSet(pairs [][2][]byte) error {
	if len(pairs) == 0 {
		return nil
	}
	mutations := make([]*kvrpcpb.Mutation, 0, len(pairs))
	for _, pair := range pairs {
		if len(pair[0]) == 0 {
			return fmt.Errorf("empty key")
		}
		// MSET writes plain values and clears existing TTL by setting ExpiresAt=0.
		valueKey := append([]byte(nil), pair[0]...)
		valueCopy := append([]byte(nil), pair[1]...)
		mutations = append(mutations, &kvrpcpb.Mutation{
			Op:        kvrpcpb.Mutation_Put,
			Key:       valueKey,
			Value:     valueCopy,
			ExpiresAt: 0,
		})
	}
	return b.mutate(append([]byte(nil), pairs[0][0]...), mutations...)
}

func (b *raftBackend) Exists(keys [][]byte) (int64, error) {
	vals, err := b.MGet(keys)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, val := range vals {
		if val != nil && val.Found {
			count++
		}
	}
	return count, nil
}

func (b *raftBackend) IncrBy(key []byte, delta int64) (int64, error) {
	version, err := b.reserveTimestamp(1)
	if err != nil {
		return 0, err
	}
	val, err := b.getAtVersion(key, version)
	if err != nil {
		return 0, err
	}
	var current int64
	if val != nil && val.Found && len(val.Value) > 0 {
		current, err = strconv.ParseInt(string(val.Value), 10, 64)
		if err != nil {
			return 0, errNotInteger
		}
	}
	if delta > 0 && current > math.MaxInt64-delta {
		return 0, errOverflow
	}
	if delta < 0 && current < math.MinInt64-delta {
		return 0, errOverflow
	}
	result := current + delta
	if _, err := b.Set(setArgs{
		Key:      key,
		Value:    []byte(strconv.FormatInt(result, 10)),
		ExpireAt: val.GetExpiresAt(),
	}); err != nil {
		return 0, err
	}
	return result, nil
}

const (
	defaultLockTTL = uint64(3000)
)

func (b *raftBackend) reserveTimestamp(n uint64) (uint64, error) {
	return b.ts.Reserve(n)
}

func (b *raftBackend) getAtVersion(key []byte, version uint64) (*redisValue, error) {
	request := [][]byte{append([]byte(nil), key...)}
	resps, err := b.batchGetWithRetry(request, version)
	if err != nil {
		return nil, err
	}
	valueResp := resps[string(request[0])]
	return b.buildValueAtVersion(key, valueResp)
}

// retryWithConflictResolution executes fn with automatic retry on KeyConflictError.
// It attempts to resolve locks before retrying.
const maxRetries = 5

func (b *raftBackend) retryWithConflictResolution(fn func() error) error {
	var lastErr error
	for range maxRetries {
		err := fn()
		if err == nil {
			return nil
		}

		var conflicts *client.KeyConflictError
		if errors.As(err, &conflicts) {
			if resolveErr := b.resolveKeyConflicts(conflicts); resolveErr == nil {
				continue // Retry after resolving locks
			} else {
				lastErr = resolveErr
				break
			}
		}
		lastErr = err
		// Non-conflict error or failed to resolve, stop retrying
		break
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("raft backend: retries exhausted")
}

func translateRaftClientError(op string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case client.IsRouteUnavailable(err):
		return &temporaryBackendError{
			msg: fmt.Sprintf("TRYAGAIN route unavailable during %s", op),
			err: err,
		}
	case client.IsRegionNotFound(err):
		return fmt.Errorf("ERR region route not found during %s: %w", op, err)
	default:
		return fmt.Errorf("raft backend: %s failed: %w", op, err)
	}
}

func (b *raftBackend) batchGetWithRetry(keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error) {
	var resps map[string]*kvrpcpb.GetResponse
	err := b.retryWithConflictResolution(func() error {
		ctx, cancel := b.context()
		defer cancel()
		var err error
		resps, err = b.client.BatchGet(ctx, keys, version)
		return err
	})
	if err != nil {
		return nil, translateRaftClientError("read", err)
	}
	return resps, nil
}

// resolveLocks attempts to resolve all given locks.
// Returns true if all locks were resolved successfully.
func (b *raftBackend) resolveLocks(locks []*kvrpcpb.Locked) error {
	for _, lock := range locks {
		if err := b.resolveSingleLock(lock); err != nil {
			return err
		}
	}
	return nil
}

func (b *raftBackend) buildValueAtVersion(key []byte, valueResp *kvrpcpb.GetResponse) (*redisValue, error) {
	if valueResp == nil || valueResp.GetNotFound() {
		return &redisValue{Found: false}, nil
	}
	expiresAt := valueResp.GetExpiresAt()
	if expiresAt > 0 && expiresAt <= uint64(time.Now().Unix()) {
		if err := b.deleteKey(key); err != nil {
			return nil, err
		}
		return &redisValue{Found: false}, nil
	}
	return &redisValue{
		Value:     append([]byte(nil), valueResp.GetValue()...),
		ExpiresAt: expiresAt,
		Found:     true,
	}, nil
}

func (b *raftBackend) deleteKey(key []byte) error {
	valueKey := append([]byte(nil), key...)
	return b.mutate(valueKey,
		&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: valueKey},
	)
}

func (b *raftBackend) mutate(primary []byte, mutations ...*kvrpcpb.Mutation) error {
	if len(mutations) == 0 {
		return nil
	}
	err := b.retryWithConflictResolution(func() error {
		start, err := b.reserveTimestamp(2)
		if err != nil {
			return err
		}
		commit := start + 1
		ctx, cancel := b.context()
		defer cancel()
		return b.client.Mutate(ctx,
			append([]byte(nil), primary...),
			mutations,
			start,
			commit,
			defaultLockTTL,
		)
	})
	if err != nil {
		return translateRaftClientError("write", err)
	}
	return nil
}

func (b *raftBackend) resolveKeyConflicts(conflicts *client.KeyConflictError) error {
	if conflicts == nil || len(conflicts.Errors) == 0 {
		return fmt.Errorf("raft backend: conflict set empty")
	}
	locks := b.extractLocksFromKeyErrors(conflicts.Errors)
	if len(locks) == 0 {
		return nil // No locks to resolve
	}
	return b.resolveLocks(locks)
}

// extractLocksFromKeyErrors extracts Locked entries from KeyError slice.
func (b *raftBackend) extractLocksFromKeyErrors(keyErrors []*kvrpcpb.KeyError) []*kvrpcpb.Locked {
	var locks []*kvrpcpb.Locked
	for _, keyErr := range keyErrors {
		if keyErr == nil {
			continue
		}
		if lock := keyErr.GetLocked(); lock != nil {
			locks = append(locks, lock)
		}
	}
	return locks
}

func (b *raftBackend) resolveSingleLock(lock *kvrpcpb.Locked) error {
	if lock == nil {
		return nil
	}
	currentTs, err := b.reserveTimestamp(1)
	if err != nil {
		currentTs = uint64(time.Now().UnixNano())
	}
	ctx, cancel := b.context()
	resp, err := b.client.CheckTxnStatus(ctx, lock.GetPrimaryLock(), lock.GetLockVersion(), currentTs)
	cancel()
	if err != nil {
		return translateRaftClientError("lock status check", err)
	}
	if resp == nil {
		return fmt.Errorf("raft backend: empty lock status response")
	}
	if resp.GetCommitVersion() > 0 {
		return b.resolveLocksWithKey(lock.GetLockVersion(), resp.GetCommitVersion(), lock.GetKey())
	}
	switch resp.GetAction() {
	case kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
		kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback:
		return b.resolveLocksWithKey(lock.GetLockVersion(), 0, lock.GetKey())
	case kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction,
		kvrpcpb.CheckTxnStatusAction_CheckTxnStatusMinCommitTsPushed:
		return fmt.Errorf("raft backend: lock resolution deferred")
	default:
		return fmt.Errorf("raft backend: unsupported lock status action %s", resp.GetAction().String())
	}
}

func (b *raftBackend) resolveLocksWithKey(lockVersion, commitVersion uint64, key []byte) error {
	keys := [][]byte{append([]byte(nil), key...)}
	ctx, cancel := b.context()
	defer cancel()
	_, err := b.client.ResolveLocks(ctx, lockVersion, commitVersion, keys)
	if err != nil {
		return translateRaftClientError("lock resolve", err)
	}
	return nil
}
