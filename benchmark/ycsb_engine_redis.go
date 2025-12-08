package benchmark

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func newRedisEngine(opts ycsbEngineOptions) ycsbEngine {
	return &redisEngine{opts: opts}
}

type redisEngine struct {
	opts   ycsbEngineOptions
	client *redis.Client
	ctx    context.Context
}

func (e *redisEngine) Name() string { return "Redis" }

func (e *redisEngine) Open(clean bool) error {
	addr := e.opts.RedisAddr
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	e.ctx = context.Background()
	e.client = redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if err := e.client.Ping(e.ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	if clean {
		if err := e.client.FlushDB(e.ctx).Err(); err != nil {
			return fmt.Errorf("redis flushdb: %w", err)
		}
	}
	return nil
}

func (e *redisEngine) Close() error {
	if e.client == nil {
		return nil
	}
	return e.client.Close()
}

func (e *redisEngine) Read(key []byte, dst []byte) ([]byte, error) {
	val, err := e.client.Get(e.ctx, string(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return dst[:0], nil
	}
	if err != nil {
		return dst[:0], err
	}
	if cap(dst) < len(val) {
		dst = make([]byte, len(val))
	}
	dst = dst[:len(val)]
	copy(dst, val)
	return dst, nil
}

func (e *redisEngine) Insert(key, value []byte) error {
	return e.client.Set(e.ctx, string(key), value, 0).Err()
}

func (e *redisEngine) Update(key, value []byte) error {
	return e.client.Set(e.ctx, string(key), value, 0).Err()
}

// Scan uses SCAN to collect up to count keys. Redis SCAN is unordered; startKey
// is ignored because Redis lacks ordered range scans over arbitrary keys.
func (e *redisEngine) Scan(_ []byte, count int) (int, error) {
	if count <= 0 {
		return 0, nil
	}
	iter := e.client.Scan(e.ctx, 0, "", int64(count)).Iterator()
	read := 0
	for iter.Next(e.ctx) && read < count {
		// Fetch value to mirror other engines' scan behaviour.
		_, _ = e.client.Get(e.ctx, iter.Val()).Bytes()
		read++
	}
	if err := iter.Err(); err != nil {
		return read, err
	}
	return read, nil
}
