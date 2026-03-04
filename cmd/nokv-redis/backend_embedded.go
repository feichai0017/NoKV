package main

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

type embeddedBackend struct {
	db *NoKV.DB
	mu sync.Mutex
}

func newEmbeddedBackend(db *NoKV.DB) *embeddedBackend {
	return &embeddedBackend{db: db}
}

func (b *embeddedBackend) Close() error {
	return nil
}

func (b *embeddedBackend) Get(key []byte) (*redisValue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.getUnlocked(key)
}

func (b *embeddedBackend) getUnlocked(key []byte) (*redisValue, error) {
	entry, err := b.db.Get(key)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			// Treat missing keys the same as a Redis nil reply.
			return &redisValue{Found: false}, nil
		}
		return nil, err
	}
	if kv.IsDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
		return &redisValue{Found: false}, nil
	}
	val := append([]byte(nil), entry.Value...)
	return &redisValue{
		Value:     val,
		ExpiresAt: entry.ExpiresAt,
		Found:     true,
	}, nil
}

func (b *embeddedBackend) Set(args setArgs) (bool, error) {
	if len(args.Key) == 0 {
		return false, utils.ErrEmptyKey
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if args.NX || args.XX {
		redisVal, err := b.getUnlocked(args.Key)
		if err != nil {
			return false, err
		}
		exists := redisVal != nil && redisVal.Found
		if args.NX && exists {
			return false, errConditionNotMet
		}
		if args.XX && !exists {
			return false, errConditionNotMet
		}
	}

	value := append([]byte(nil), args.Value...)
	var err error
	if args.TTL > 0 {
		err = b.db.SetWithTTL(args.Key, value, args.TTL)
	} else if args.ExpireAt > 0 {
		err = b.db.SetWithTTL(args.Key, value, ttlFromExpireAt(args.ExpireAt))
	} else {
		err = b.db.Set(args.Key, value)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *embeddedBackend) Del(keys [][]byte) (int64, error) {
	var removed int64
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range keys {
		val, err := b.getUnlocked(key)
		if err != nil {
			return 0, err
		}
		if val != nil && val.Found {
			removed++
		}
		if err := b.db.Del(key); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

func (b *embeddedBackend) MGet(keys [][]byte) ([]*redisValue, error) {
	out := make([]*redisValue, len(keys))
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, key := range keys {
		val, err := b.getUnlocked(key)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	for i, val := range out {
		if val == nil {
			out[i] = &redisValue{Found: false}
		}
	}
	return out, nil
}

func (b *embeddedBackend) MSet(pairs [][2][]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, pair := range pairs {
		if len(pair[0]) == 0 {
			return utils.ErrEmptyKey
		}
		err := b.db.Set(pair[0], append([]byte(nil), pair[1]...))
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *embeddedBackend) Exists(keys [][]byte) (int64, error) {
	var count int64
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range keys {
		val, err := b.getUnlocked(key)
		if err != nil {
			return 0, err
		}
		if val != nil && val.Found {
			count++
		}
	}
	return count, nil
}

func (b *embeddedBackend) IncrBy(key []byte, delta int64) (int64, error) {
	var result int64
	b.mu.Lock()
	defer b.mu.Unlock()
	var (
		current  int64
		expires  uint64
		existing bool
	)

	val, err := b.getUnlocked(key)
	if err != nil {
		return 0, err
	}
	if val != nil && val.Found {
		existing = true
		expires = val.ExpiresAt
		if len(val.Value) > 0 {
			parsed, perr := strconvParseIntSafe(val.Value)
			if perr != nil {
				return 0, errNotInteger
			}
			current = parsed
		}
	}

	if delta > 0 && current > math.MaxInt64-delta {
		return 0, errOverflow
	}
	if delta < 0 && current < math.MinInt64-delta {
		return 0, errOverflow
	}

	result = current + delta
	value := []byte(strconv.FormatInt(result, 10))
	if existing {
		if expires > 0 {
			if err := b.db.SetWithTTL(key, value, ttlFromExpireAt(expires)); err != nil {
				return 0, err
			}
			return result, nil
		}
		if err := b.db.Set(key, value); err != nil {
			return 0, err
		}
		return result, nil
	}
	if err := b.db.Set(key, value); err != nil {
		return 0, err
	}
	return result, nil
}

func ttlFromExpireAt(expiresAt uint64) time.Duration {
	if expiresAt == 0 {
		return 0
	}
	return time.Until(time.Unix(int64(expiresAt), 0))
}

func strconvParseIntSafe(data []byte) (int64, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(string(data), 10, 64)
}
