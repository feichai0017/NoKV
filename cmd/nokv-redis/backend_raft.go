package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/client"
)

type raftBackend struct {
	client *client.Client
	ts     timestampAllocator
}

type timestampAllocator interface {
	Reserve(n uint64) (uint64, error)
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
	defer resp.Body.Close()
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

func newRaftBackend(cfgPath, tsoURL string) (*raftBackend, error) {
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("raft backend: read config: %w", err)
	}
	var rawCfg raftConfigFile
	if err := json.Unmarshal(raw, &rawCfg); err != nil {
		return nil, fmt.Errorf("raft backend: parse config: %w", err)
	}
	if len(rawCfg.Stores) == 0 {
		return nil, fmt.Errorf("raft backend: at least one store required")
	}
	if len(rawCfg.Regions) == 0 {
		return nil, fmt.Errorf("raft backend: at least one region required")
	}
	cfg := client.Config{
		MaxRetries: rawCfg.MaxRetries,
	}
	for _, st := range rawCfg.Stores {
		cfg.Stores = append(cfg.Stores, client.StoreEndpoint{
			StoreID: st.StoreID,
			Addr:    st.Addr,
		})
	}
	for _, region := range rawCfg.Regions {
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
	if val == "" {
		return nil
	}
	out, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return []byte(val)
	}
	return out
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
	ctx, cancel := b.context()
	defer cancel()
	resp, err := b.client.Get(ctx, key, mathMaxUint64)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.GetNotFound() {
		return &redisValue{Found: false}, nil
	}
	// TTL metadata is stored separately. Fetch it so callers get the same behaviour
	// as the embedded backend (including auto-cleanup when the entry expired).
	expiresAt, err := b.loadTTL(key)
	if err != nil {
		return nil, err
	}
	if expiresAt > 0 && expiresAt <= uint64(time.Now().Unix()) {
		if err := b.deleteKeys(key); err != nil {
			return nil, err
		}
		return &redisValue{Found: false}, nil
	}
	return &redisValue{
		Value:     append([]byte(nil), resp.GetValue()...),
		ExpiresAt: expiresAt,
		Found:     true,
	}, nil
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
	var removed int64
	mutations := make([]*pb.Mutation, 0, len(keys)*2)
	for _, key := range keys {
		val, err := b.Get(key)
		if err != nil {
			return 0, err
		}
		// Remove both value and TTL entries in a single 2PC mutation batch so the
		// behaviour matches embedded mode.
		valueKey := append([]byte(nil), key...)
		metaKey := ttlMetaKey(key)
		mutations = append(mutations,
			&pb.Mutation{Op: pb.Mutation_Delete, Key: valueKey},
			&pb.Mutation{Op: pb.Mutation_Delete, Key: metaKey},
		)
		if val.Found {
			removed++
		}
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
	out := make([]*redisValue, len(keys))
	for i, key := range keys {
		val, err := b.Get(key)
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
	var count int64
	for _, key := range keys {
		val, err := b.Get(key)
		if err != nil {
			return count, err
		}
		if val != nil && val.Found {
			count++
		}
	}
	return count, nil
}

func (b *raftBackend) IncrBy(key []byte, delta int64) (int64, error) {
	val, err := b.Get(key)
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
	mathMaxUint64  = ^uint64(0)
)

var ttlMetaPrefix = []byte("!redis:ttl!")

func ttlMetaKey(key []byte) []byte {
	out := make([]byte, len(ttlMetaPrefix)+len(key))
	copy(out, ttlMetaPrefix)
	copy(out[len(ttlMetaPrefix):], key)
	return out
}

func (b *raftBackend) loadTTL(key []byte) (uint64, error) {
	ctx, cancel := b.context()
	defer cancel()
	resp, err := b.client.Get(ctx, ttlMetaKey(key), mathMaxUint64)
	if err != nil {
		return 0, err
	}
	if resp == nil || resp.GetNotFound() {
		return 0, nil
	}
	data := resp.GetValue()
	if len(data) != 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(data), nil
}

func (b *raftBackend) deleteKeys(key []byte) error {
	valueKey := append([]byte(nil), key...)
	metaKey := ttlMetaKey(key)
	return b.mutate(valueKey,
		&pb.Mutation{Op: pb.Mutation_Delete, Key: valueKey},
		&pb.Mutation{Op: pb.Mutation_Delete, Key: metaKey},
	)
}

func (b *raftBackend) mutate(primary []byte, mutations ...*pb.Mutation) error {
	if len(mutations) == 0 {
		return nil
	}
	start, err := b.ts.Reserve(2)
	if err != nil {
		return err
	}
	commit := start + 1
	// We rely on raftstore/client's TwoPhaseCommit to guarantee atomicity across
	// all keys in the mutation batch.
	ctx, cancel := b.context()
	defer cancel()
	return b.client.Mutate(ctx,
		append([]byte(nil), primary...),
		mutations,
		start,
		commit,
		defaultLockTTL,
	)
}

type raftConfigFile struct {
	Stores     []raftStoreConfig  `json:"stores"`
	Regions    []raftRegionConfig `json:"regions"`
	MaxRetries int                `json:"max_retries"`
}

type raftStoreConfig struct {
	StoreID uint64 `json:"store_id"`
	Addr    string `json:"addr"`
}

type raftRegionConfig struct {
	ID            uint64           `json:"id"`
	StartKey      string           `json:"start_key"`
	EndKey        string           `json:"end_key"`
	Epoch         raftRegionEpoch  `json:"epoch"`
	Peers         []raftRegionPeer `json:"peers"`
	LeaderStoreID uint64           `json:"leader_store_id"`
}

type raftRegionEpoch struct {
	Version     uint64 `json:"version"`
	ConfVersion uint64 `json:"conf_version"`
}

type raftRegionPeer struct {
	StoreID uint64 `json:"store_id"`
	PeerID  uint64 `json:"peer_id"`
}
