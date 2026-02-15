package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/client"
)

type raftBackend struct {
	client raftClient
	ts     timestampAllocator
}

type timestampAllocator interface {
	Reserve(n uint64) (uint64, error)
}

type raftClient interface {
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*pb.GetResponse, error)
	Mutate(ctx context.Context, primary []byte, mutations []*pb.Mutation, startVersion, commitVersion, lockTTL uint64) error
	CheckTxnStatus(ctx context.Context, primary []byte, lockVersion, currentTS uint64) (*pb.CheckTxnStatusResponse, error)
	ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error)
	Close() error
}

type localOracle struct {
	last atomic.Uint64
}

func (o *localOracle) Reserve(n uint64) (uint64, error) {
	if n == 0 {
		return 0, fmt.Errorf("oracle reserve: n must be >= 1")
	}
	for {
		prev := o.last.Load()
		now := uint64(time.Now().UnixNano())
		if now <= prev {
			now = prev + n
		} else {
			now = now + (n - 1)
		}
		start := now - (n - 1)
		if o.last.CompareAndSwap(prev, now) {
			return start, nil
		}
	}
}

type httpTSO struct {
	url    string
	client *http.Client
}

func newHTTPTSO(url string) *httpTSO {
	return &httpTSO{
		url: strings.TrimRight(url, "/"),
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (t *httpTSO) Reserve(n uint64) (uint64, error) {
	if n == 0 {
		return 0, fmt.Errorf("tso reserve: n must be >= 1")
	}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/tso?batch=%d", t.url, n), nil)
	if err != nil {
		return 0, err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("tso reserve: unexpected status %s", resp.Status)
	}
	var payload struct {
		Timestamp uint64 `json:"timestamp"`
		Count     uint64 `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, fmt.Errorf("tso reserve: decode response: %w", err)
	}
	if payload.Count < n {
		return 0, fmt.Errorf("tso reserve: requested %d timestamps, got %d", n, payload.Count)
	}
	return payload.Timestamp, nil
}

func newRaftBackend(cfgPath, tsoURL, addrScope string) (*raftBackend, error) {
	cfgFile, err := config.LoadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("raft backend: read config: %w", err)
	}
	if err := cfgFile.Validate(); err != nil {
		return nil, fmt.Errorf("raft backend: config invalid: %w", err)
	}
	cfg := client.Config{
		MaxRetries: cfgFile.MaxRetries,
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
	for _, region := range cfgFile.Regions {
		meta := &pb.RegionMeta{
			Id:               region.ID,
			StartKey:         decodeKey(region.StartKey),
			EndKey:           decodeKey(region.EndKey),
			EpochVersion:     region.Epoch.Version,
			EpochConfVersion: region.Epoch.ConfVersion,
		}
		for _, peer := range region.Peers {
			meta.Peers = append(meta.Peers, &pb.RegionPeer{
				StoreId: peer.StoreID,
				PeerId:  peer.PeerID,
			})
		}
		cfg.Regions = append(cfg.Regions, client.RegionConfig{
			Meta:          meta,
			LeaderStoreID: region.LeaderStoreID,
		})
	}
	cl, err := client.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("raft backend: init client: %w", err)
	}

	tsoURL = strings.TrimSpace(tsoURL)
	if tsoURL == "" && cfgFile.TSO != nil {
		tsoURL = strings.TrimSpace(cfgFile.TSO.AdvertiseURL)
		if tsoURL == "" {
			tsoURL = strings.TrimSpace(cfgFile.TSO.ListenAddr)
			if tsoURL != "" && !strings.Contains(tsoURL, "://") {
				tsoURL = "http://" + tsoURL
			}
		}
	}

	var allocator timestampAllocator
	if tsoURL != "" {
		allocator = newHTTPTSO(tsoURL)
	} else {
		allocator = &localOracle{}
	}

	return &raftBackend{
		client: cl,
		ts:     allocator,
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
	return context.WithTimeout(context.Background(), 3*time.Second)
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
	var mutations []*pb.Mutation
	mutations = append(mutations, &pb.Mutation{
		Op:    pb.Mutation_Put,
		Key:   valueKey,
		Value: valueCopy,
	})

	metaKey := ttlMetaKey(args.Key)
	if args.ExpireAt > 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, args.ExpireAt)
		mutations = append(mutations, &pb.Mutation{
			Op:    pb.Mutation_Put,
			Key:   metaKey,
			Value: buf,
		})
	} else {
		mutations = append(mutations, &pb.Mutation{
			Op:  pb.Mutation_Delete,
			Key: metaKey,
		})
	}

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
	ctx, cancel := b.context()
	defer cancel()
	resps, err := b.client.BatchGet(ctx, keys, version)
	if err != nil {
		return 0, err
	}
	mutations := make([]*pb.Mutation, 0, len(keys)*2)
	var removed int64
	for _, key := range keys {
		resp := resps[string(key)]
		if resp != nil && !resp.GetNotFound() {
			removed++
		}
		valueKey := append([]byte(nil), key...)
		metaKey := ttlMetaKey(key)
		mutations = append(mutations,
			&pb.Mutation{Op: pb.Mutation_Delete, Key: valueKey},
			&pb.Mutation{Op: pb.Mutation_Delete, Key: metaKey},
		)
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
	ctx, cancel := b.context()
	defer cancel()
	request := make([][]byte, 0, len(keys)*2)
	valueKeys := make([][]byte, len(keys))
	metaKeys := make([][]byte, len(keys))
	for i, key := range keys {
		valueKey := append([]byte(nil), key...)
		valueKeys[i] = valueKey
		metaKey := ttlMetaKey(key)
		metaKeys[i] = metaKey
		request = append(request, valueKey, metaKey)
	}
	resps, err := b.client.BatchGet(ctx, request, version)
	if err != nil {
		return nil, err
	}
	out := make([]*redisValue, len(keys))
	for i, key := range keys {
		valueResp := resps[string(valueKeys[i])]
		ttlResp := resps[string(metaKeys[i])]
		val, err := b.buildValueAtVersion(key, valueResp, ttlResp)
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
	mutations := make([]*pb.Mutation, 0, len(pairs)*2)
	for _, pair := range pairs {
		if len(pair[0]) == 0 {
			return fmt.Errorf("empty key")
		}
		// Write value and clear TTL metadata for every key in a single mutate call.
		valueKey := append([]byte(nil), pair[0]...)
		valueCopy := append([]byte(nil), pair[1]...)
		mutations = append(mutations, &pb.Mutation{
			Op:    pb.Mutation_Put,
			Key:   valueKey,
			Value: valueCopy,
		})
		// Ensure any stale TTL metadata is cleared.
		metaKey := ttlMetaKey(pair[0])
		mutations = append(mutations, &pb.Mutation{
			Op:  pb.Mutation_Delete,
			Key: metaKey,
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

var ttlMetaPrefix = []byte("!redis:ttl!")

func (b *raftBackend) reserveTimestamp(n uint64) (uint64, error) {
	return b.ts.Reserve(n)
}

func (b *raftBackend) getAtVersion(key []byte, version uint64) (*redisValue, error) {
	ctx, cancel := b.context()
	defer cancel()
	request := [][]byte{
		append([]byte(nil), key...),
		ttlMetaKey(key),
	}
	resps, err := b.client.BatchGet(ctx, request, version)
	if err != nil {
		return nil, err
	}
	valueResp := resps[string(request[0])]
	ttlResp := resps[string(request[1])]
	return b.buildValueAtVersion(key, valueResp, ttlResp)
}

func (b *raftBackend) buildValueAtVersion(key []byte, valueResp, ttlResp *pb.GetResponse) (*redisValue, error) {
	if valueResp == nil || valueResp.GetNotFound() {
		if ttlResp != nil && !ttlResp.GetNotFound() {
			// Stale TTL metadata without a value should be cleaned up.
			if err := b.deleteKeys(key); err != nil {
				return nil, err
			}
		}
		return &redisValue{Found: false}, nil
	}
	expiresAt := decodeTTLFromResponse(ttlResp)
	if expiresAt > 0 && expiresAt <= uint64(time.Now().Unix()) {
		if err := b.deleteKeys(key); err != nil {
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

func ttlMetaKey(key []byte) []byte {
	out := make([]byte, len(ttlMetaPrefix)+len(key))
	copy(out, ttlMetaPrefix)
	copy(out[len(ttlMetaPrefix):], key)
	return out
}

func (b *raftBackend) deleteKeys(key []byte) error {
	valueKey := append([]byte(nil), key...)
	metaKey := ttlMetaKey(key)
	return b.mutate(valueKey,
		&pb.Mutation{Op: pb.Mutation_Delete, Key: valueKey},
		&pb.Mutation{Op: pb.Mutation_Delete, Key: metaKey},
	)
}

func decodeTTLFromResponse(resp *pb.GetResponse) uint64 {
	if resp == nil || resp.GetNotFound() {
		return 0
	}
	data := resp.GetValue()
	if len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

func (b *raftBackend) mutate(primary []byte, mutations ...*pb.Mutation) error {
	if len(mutations) == 0 {
		return nil
	}
	const maxRetries = 5
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		start, err := b.reserveTimestamp(2)
		if err != nil {
			return err
		}
		commit := start + 1
		ctx, cancel := b.context()
		err = b.client.Mutate(ctx,
			append([]byte(nil), primary...),
			mutations,
			start,
			commit,
			defaultLockTTL,
		)
		cancel()
		if err == nil {
			return nil
		}
		var conflicts *client.KeyConflictError
		if errors.As(err, &conflicts) {
			if b.resolveKeyConflicts(conflicts) {
				continue
			}
		}
		lastErr = err
		break
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("raft backend: mutate retries exhausted")
}

func (b *raftBackend) resolveKeyConflicts(conflicts *client.KeyConflictError) bool {
	if conflicts == nil || len(conflicts.Errors) == 0 {
		return false
	}
	for _, keyErr := range conflicts.Errors {
		if keyErr == nil {
			continue
		}
		lock := keyErr.GetLocked()
		if lock == nil {
			continue
		}
		if !b.resolveSingleLock(lock) {
			return false
		}
	}
	return true
}

func (b *raftBackend) resolveSingleLock(lock *pb.Locked) bool {
	if lock == nil {
		return true
	}
	currentTs, err := b.reserveTimestamp(1)
	if err != nil {
		currentTs = uint64(time.Now().UnixNano())
	}
	ctx, cancel := b.context()
	resp, err := b.client.CheckTxnStatus(ctx, lock.GetPrimaryLock(), lock.GetLockVersion(), currentTs)
	cancel()
	if err != nil || resp == nil {
		return false
	}
	if resp.GetCommitVersion() > 0 {
		return b.resolveLocksWithKey(lock.GetLockVersion(), resp.GetCommitVersion(), lock.GetKey())
	}
	switch resp.GetAction() {
	case pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
		pb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback:
		return b.resolveLocksWithKey(lock.GetLockVersion(), 0, lock.GetKey())
	case pb.CheckTxnStatusAction_CheckTxnStatusNoAction,
		pb.CheckTxnStatusAction_CheckTxnStatusMinCommitTsPushed:
		return false
	default:
		return false
	}
}

func (b *raftBackend) resolveLocksWithKey(lockVersion, commitVersion uint64, key []byte) bool {
	keys := [][]byte{append([]byte(nil), key...)}
	if isTTLMetaKey(key) {
		base := append([]byte(nil), key[len(ttlMetaPrefix):]...)
		if len(base) > 0 {
			keys = append(keys, base)
		}
	} else {
		keys = append(keys, ttlMetaKey(key))
	}
	ctx, cancel := b.context()
	defer cancel()
	_, err := b.client.ResolveLocks(ctx, lockVersion, commitVersion, uniqueKeys(keys))
	return err == nil
}

func isTTLMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, ttlMetaPrefix)
}

func uniqueKeys(keys [][]byte) [][]byte {
	seen := make(map[string]struct{}, len(keys))
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		str := string(key)
		if _, ok := seen[str]; ok {
			continue
		}
		seen[str] = struct{}{}
		out = append(out, key)
	}
	return out
}
