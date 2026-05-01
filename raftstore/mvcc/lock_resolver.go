package mvcc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/engine/index"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
	txnmvcc "github.com/feichai0017/NoKV/percolator/mvcc"
	"github.com/feichai0017/NoKV/utils"
)

const defaultResolveLockBatch = 4096

// ResolveLocksOptions bounds one local expired-lock resolution pass.
type ResolveLocksOptions struct {
	// CurrentTs is the timestamp used for TTL checks. Zero disables resolution.
	CurrentTs uint64
	// BatchLocks limits how many expired locks are resolved in one
	// ApplyInternalEntries call. A non-positive value uses the default.
	BatchLocks int
}

func (o ResolveLocksOptions) batchLocks() int {
	if o.BatchLocks <= 0 {
		return defaultResolveLockBatch
	}
	return o.BatchLocks
}

// ResolveLocksStats summarizes one expired-lock resolution pass.
type ResolveLocksStats struct {
	ScannedLocks       uint64
	DeletedLockMarkers uint64
	RetainedLocks      uint64
	ExpiredLocks       uint64
	ResolvedLocks      uint64
	CommittedLocks     uint64
	RolledBackLocks    uint64
}

// ResolveExpiredLocks resolves expired Percolator locks in a local maintenance
// store. It does not propose through raft; callers must not run it against a
// live cluster-mode workdir.
func ResolveExpiredLocks(ctx context.Context, db txnstore.Store, opt ResolveLocksOptions) (ResolveLocksStats, error) {
	var stats ResolveLocksStats
	if opt.CurrentTs == 0 {
		return stats, nil
	}
	var afterUserKey []byte
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		batch, err := collectResolveLockBatch(ctx, db, opt.CurrentTs, afterUserKey, opt.batchLocks())
		if err != nil {
			return stats, err
		}
		stats.add(batch.scan)
		if len(batch.locks) > 0 {
			entries, resolved, err := buildResolveLockEntries(ctx, db, opt.CurrentTs, batch.locks)
			if err != nil {
				return stats, err
			}
			if len(entries) > 0 {
				if err := db.ApplyInternalEntries(entries); err != nil {
					releaseEntries(entries)
					return stats, err
				}
			}
			releaseEntries(entries)
			stats.add(resolved)
		}
		if batch.done {
			return stats, nil
		}
		afterUserKey = batch.lastUserKey
	}
}

type lockRecord struct {
	key  []byte
	lock txnmvcc.Lock
}

type resolveLockBatch struct {
	scan        ResolveLocksStats
	locks       []lockRecord
	lastUserKey []byte
	done        bool
}

func collectResolveLockBatch(ctx context.Context, db txnstore.Store, currentTs uint64, afterUserKey []byte, maxLocks int) (resolveLockBatch, error) {
	var batch resolveLockBatch
	if db == nil {
		return batch, fmt.Errorf("raftstore/mvcc: nil MVCC store")
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		batch.done = true
		return batch, nil
	}
	defer func() { _ = iter.Close() }()

	seekLockStart(iter, afterUserKey)
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return batch, err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok {
			return batch, fmt.Errorf("raftstore/mvcc: expected internal lock key, got %x", entry.Key)
		}
		if cf != entrykv.CFLock {
			batch.done = true
			return batch, nil
		}
		batch.lastUserKey = entrykv.SafeCopy(batch.lastUserKey, userKey)
		if entry.Meta&entrykv.BitDelete > 0 {
			batch.scan.DeletedLockMarkers++
			iter.Next()
			continue
		}
		lock, err := txnmvcc.DecodeLock(entry.Value)
		if err != nil {
			return batch, fmt.Errorf("raftstore/mvcc: decode CFLock %x: %w", userKey, err)
		}
		batch.scan.ScannedLocks++
		if !lockExpired(lock, currentTs) {
			batch.scan.RetainedLocks++
			iter.Next()
			continue
		}
		batch.scan.ExpiredLocks++
		batch.locks = append(batch.locks, lockRecord{
			key:  entrykv.SafeCopy(nil, userKey),
			lock: lock,
		})
		iter.Next()
		if len(batch.locks) >= maxLocks {
			return batch, nil
		}
	}
	batch.done = true
	return batch, nil
}

func seekLockStart(iter index.Iterator, afterUserKey []byte) {
	if len(afterUserKey) == 0 {
		iter.Seek(entrykv.InternalKey(entrykv.CFLock, nil, entrykv.MaxVersion))
		return
	}
	iter.Seek(entrykv.InternalKey(entrykv.CFLock, afterUserKey, 0))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := entrykv.SplitInternalKey(item.Entry().Key)
		if !ok || cf != entrykv.CFLock || !bytes.Equal(userKey, afterUserKey) {
			return
		}
		iter.Next()
	}
}

func buildResolveLockEntries(ctx context.Context, db txnstore.Store, currentTs uint64, locks []lockRecord) ([]*entrykv.Entry, ResolveLocksStats, error) {
	var stats ResolveLocksStats
	var entries []*entrykv.Entry
	seen := make(map[string]struct{}, len(locks))
	for _, rec := range locks {
		if err := ctx.Err(); err != nil {
			releaseEntries(entries)
			return nil, stats, err
		}
		ops, committed, rolledBack, err := resolveOneLock(ctx, db, currentTs, rec)
		if err != nil {
			releaseEntries(entries)
			return nil, stats, err
		}
		if len(ops) == 0 {
			stats.RetainedLocks++
			continue
		}
		for _, entry := range ops {
			id := string(entry.Key)
			if _, ok := seen[id]; ok {
				entry.DecrRef()
				continue
			}
			seen[id] = struct{}{}
			entries = append(entries, entry)
		}
		stats.ResolvedLocks++
		if committed {
			stats.CommittedLocks++
		}
		if rolledBack {
			stats.RolledBackLocks++
		}
	}
	return entries, stats, nil
}

func resolveOneLock(ctx context.Context, db txnstore.Store, currentTs uint64, rec lockRecord) ([]*entrykv.Entry, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, false, err
	}
	if write, commitTs, ok, err := writeByStartTs(db, rec.lock.Primary, rec.lock.Ts); err != nil {
		return nil, false, false, err
	} else if ok {
		if write.Kind == kvrpcpb.Mutation_Rollback {
			return rollbackLockEntries(rec.key, rec.lock.Ts), false, true, nil
		}
		return commitLockEntries(rec.key, rec.lock, commitTs), true, false, nil
	}

	if !bytes.Equal(rec.key, rec.lock.Primary) {
		primary, err := lockForKey(db, rec.lock.Primary)
		if err != nil {
			return nil, false, false, err
		}
		if primary != nil && primary.Ts == rec.lock.Ts && !lockExpired(*primary, currentTs) {
			return nil, false, false, nil
		}
	}
	return rollbackLockEntries(rec.key, rec.lock.Ts), false, true, nil
}

func lockForKey(db txnstore.Store, key []byte) (*txnmvcc.Lock, error) {
	entry, err := db.GetInternalEntry(entrykv.CFLock, key, entrykv.MaxVersion)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	defer entry.DecrRef()
	if entry.Meta&entrykv.BitDelete > 0 || entry.Value == nil {
		return nil, nil
	}
	lock, err := txnmvcc.DecodeLock(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("raftstore/mvcc: decode primary lock %x: %w", key, err)
	}
	return &lock, nil
}

func writeByStartTs(db txnstore.Store, key []byte, startTs uint64) (txnmvcc.Write, uint64, bool, error) {
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return txnmvcc.Write{}, 0, false, nil
	}
	defer func() { _ = iter.Close() }()

	iter.Seek(entrykv.InternalKey(entrykv.CFWrite, key, entrykv.MaxVersion))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, commitTs, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok {
			return txnmvcc.Write{}, 0, false, fmt.Errorf("raftstore/mvcc: expected internal write key, got %x", entry.Key)
		}
		if cf != entrykv.CFWrite || !bytes.Equal(userKey, key) {
			break
		}
		if entry.Meta&entrykv.BitDelete > 0 {
			iter.Next()
			continue
		}
		write, err := txnmvcc.DecodeWrite(entry.Value)
		if err != nil {
			return txnmvcc.Write{}, 0, false, fmt.Errorf("raftstore/mvcc: decode CFWrite %x@%d: %w", userKey, commitTs, err)
		}
		if write.StartTs == startTs {
			return write, commitTs, true, nil
		}
		if commitTs < startTs {
			break
		}
		iter.Next()
	}
	return txnmvcc.Write{}, 0, false, nil
}

func commitLockEntries(key []byte, lock txnmvcc.Lock, commitTs uint64) []*entrykv.Entry {
	return []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFWrite, key, commitTs, txnmvcc.EncodeWrite(txnmvcc.Write{Kind: lock.Kind, StartTs: lock.Ts}), 0, 0),
		entrykv.NewInternalEntry(entrykv.CFLock, key, entrykv.MaxVersion, nil, entrykv.BitDelete, 0),
	}
}

func rollbackLockEntries(key []byte, startTs uint64) []*entrykv.Entry {
	return []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFDefault, key, startTs, nil, entrykv.BitDelete, 0),
		entrykv.NewInternalEntry(entrykv.CFWrite, key, startTs, txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Rollback, StartTs: startTs}), 0, 0),
		entrykv.NewInternalEntry(entrykv.CFLock, key, entrykv.MaxVersion, nil, entrykv.BitDelete, 0),
	}
}

func lockExpired(lock txnmvcc.Lock, currentTs uint64) bool {
	if lock.TTL == 0 || currentTs == 0 {
		return false
	}
	return currentTs >= lock.Ts+lock.TTL
}

func (s *ResolveLocksStats) add(other ResolveLocksStats) {
	s.ScannedLocks += other.ScannedLocks
	s.DeletedLockMarkers += other.DeletedLockMarkers
	s.RetainedLocks += other.RetainedLocks
	s.ExpiredLocks += other.ExpiredLocks
	s.ResolvedLocks += other.ResolvedLocks
	s.CommittedLocks += other.CommittedLocks
	s.RolledBackLocks += other.RolledBackLocks
}
