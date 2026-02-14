package main

import (
	"bytes"
	"errors"
	"math"
	"strconv"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

type embeddedBackend struct {
	db *NoKV.DB
}

func newEmbeddedBackend(db *NoKV.DB) *embeddedBackend {
	return &embeddedBackend{db: db}
}

func (b *embeddedBackend) Close() error {
	return nil
}

func (b *embeddedBackend) Get(key []byte) (*redisValue, error) {
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
	if args.NX || args.XX {
		// Guard the condition check and write inside a single transaction to keep the
		// Redis semantics (read + write must be atomic).
		err := b.db.Update(func(txn *NoKV.Txn) error {
			exists := false
			item, err := txn.Get(args.Key)
			switch {
			case err == nil:
				exists = true
				if kv.IsDeletedOrExpired(item.Entry().Meta, item.Entry().ExpiresAt) {
					exists = false
				}
			case errors.Is(err, utils.ErrKeyNotFound):
				exists = false
			default:
				return err
			}
			if args.NX && exists {
				return errConditionNotMet
			}
			if args.XX && !exists {
				return errConditionNotMet
			}
			e := kv.NewEntry(args.Key, append([]byte(nil), args.Value...))
			if args.ExpireAt > 0 {
				e.ExpiresAt = args.ExpireAt
			}
			return txn.SetEntry(e)
		})
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, errConditionNotMet):
			return false, errConditionNotMet
		default:
			return false, err
		}
	}

	entry := kv.NewEntry(args.Key, append([]byte(nil), args.Value...))
	if args.ExpireAt > 0 {
		entry.ExpiresAt = args.ExpireAt
	}
	if err := b.db.Update(func(txn *NoKV.Txn) error {
		return txn.SetEntry(entry)
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (b *embeddedBackend) Del(keys [][]byte) (int64, error) {
	var removed int64
	// Execute deletes for all keys inside a single transaction so the removal
	// count matches the snapshot used for the writes.
	err := b.db.Update(func(txn *NoKV.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			switch {
			case err == nil:
				entry := item.Entry()
				if kv.IsDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
					if err := txn.Delete(key); err != nil {
						return err
					}
					continue
				}
				if err := txn.Delete(key); err != nil {
					return err
				}
				removed++
			case errors.Is(err, utils.ErrKeyNotFound):
				if err := txn.Delete(key); err != nil {
					return err
				}
			default:
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

func (b *embeddedBackend) MGet(keys [][]byte) ([]*redisValue, error) {
	out := make([]*redisValue, len(keys))
	// Use a read-only transaction so all keys are observed at the same readTs.
	err := b.db.View(func(txn *NoKV.Txn) error {
		for i, key := range keys {
			item, err := txn.Get(key)
			switch {
			case err == nil:
				entry := item.Entry()
				if kv.IsDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
					out[i] = &redisValue{Found: false}
					continue
				}
				valCopy := append([]byte(nil), entry.Value...)
				out[i] = &redisValue{
					Value:     valCopy,
					ExpiresAt: entry.ExpiresAt,
					Found:     true,
				}
			case errors.Is(err, utils.ErrKeyNotFound):
				out[i] = &redisValue{Found: false}
			default:
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i, val := range out {
		if val == nil {
			out[i] = &redisValue{Found: false}
		}
	}
	return out, nil
}

func (b *embeddedBackend) MSet(pairs [][2][]byte) error {
	return b.db.Update(func(txn *NoKV.Txn) error {
		for _, pair := range pairs {
			if len(pair[0]) == 0 {
				return utils.ErrEmptyKey
			}
			entry := kv.NewEntry(pair[0], append([]byte(nil), pair[1]...))
			if err := txn.SetEntry(entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *embeddedBackend) Exists(keys [][]byte) (int64, error) {
	var count int64
	// Checking existence within a View keeps behaviour consistent with MGET and
	// avoids allocating a new iterator per key.
	err := b.db.View(func(txn *NoKV.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			switch {
			case err == nil:
				entry := item.Entry()
				if kv.IsDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
					continue
				}
				count++
			case errors.Is(err, utils.ErrKeyNotFound):
				continue
			default:
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (b *embeddedBackend) IncrBy(key []byte, delta int64) (int64, error) {
	var result int64
	err := b.db.Update(func(txn *NoKV.Txn) error {
		var (
			current  int64
			expires  uint64
			existing bool
		)

		item, err := txn.Get(key)
		switch {
		case err == nil:
			existing = true
			entry := item.Entry()
			if kv.IsDeletedOrExpired(entry.Meta, entry.ExpiresAt) {
				existing = false
			} else if len(entry.Value) > 0 {
				parsed, perr := strconvParseIntSafe(entry.Value)
				if perr != nil {
					return errNotInteger
				}
				current = parsed
			}
			expires = entry.ExpiresAt
		case errors.Is(err, utils.ErrKeyNotFound):
			existing = false
		default:
			return err
		}

		if delta > 0 && current > math.MaxInt64-delta {
			return errOverflow
		}
		if delta < 0 && current < math.MinInt64-delta {
			return errOverflow
		}

		result = current + delta
		entry := kv.NewEntry(key, []byte(strconv.FormatInt(result, 10)))
		if existing {
			entry.ExpiresAt = expires
		}
		return txn.SetEntry(entry)
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func strconvParseIntSafe(data []byte) (int64, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(string(data), 10, 64)
}
