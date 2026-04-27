// Package iterator implements the user-facing DB iterator state machine
// on top of an LSM merge iterator + value-log resolver. The root NoKV
// package keeps the (db *DB) NewIterator / NewInternalIterator wrappers
// as thin facades around iterator.New / iterator.NewInternal so callers
// continue to write `db.NewIterator(...)` while all of the actual
// state-machine code lives here, decoupled from *DB.
package iterator

import (
	"bytes"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

// Storage is the narrow LSM surface the user-facing iterator needs:
// build the merged per-shard sub-iterators, query for any active range
// tombstone, and pin a snapshot view if there is one. Implemented by
// engine/lsm.LSM.
type Storage interface {
	NewIterators(opt *index.Options) []index.Iterator
	HasAnyRangeTombstone() bool
	PinRangeTombstoneView() *lsm.RangeTombstoneView
}

// Vlog resolves a value pointer into its concrete bytes; only the Read
// method is needed here. Implemented by engine/vlog.Consumer.
type Vlog interface {
	Read(*kv.ValuePtr) ([]byte, func(), error)
}

// Deps wires the iterator into its host. Storage + Vlog + Pool are the
// only DB-side touch points the iterator needs to function.
type Deps struct {
	Storage Storage
	Vlog    Vlog
	Pool    *IteratorPool
}

// DBIterator wraps the merged LSM iterators and optionally resolves value-log pointers.
type DBIterator struct {
	iitr index.Iterator
	vlog Vlog
	pool *IteratorPool
	ctx  *IteratorContext
	rtv  *lsm.RangeTombstoneView
	// rtCheck indicates whether this iterator snapshot needs tombstone
	// coverage checks.
	rtCheck bool
	// keyOnly avoids eager value log materialisation when true.
	keyOnly bool

	lowerBound []byte
	upperBound []byte
	hasLower   bool
	hasUpper   bool
	isAsc      bool
	// seekOutOfRange marks Seek calls that intentionally invalidated the
	// iterator due to bounds checks. While set, Next must not advance from a
	// stale cursor position and resurrect validity.
	seekOutOfRange bool

	lastUserKey []byte
	pendingKey  []byte
	pendingVal  []byte
	pending     kv.Entry
	hasPending  bool
	latestKey   []byte
	latestVal   []byte
	latest      kv.Entry

	entry    kv.Entry
	item     Item
	valueBuf []byte
	valid    bool
	err      error // terminal error that stopped iteration
}

// Item is the user-facing iterator item backed by an entry and optional vlog reader.
type Item struct {
	e        *kv.Entry
	vlog     Vlog
	valueBuf []byte
}

// Entry returns the current entry view for this iterator item.
func (it *Item) Entry() *kv.Entry {
	return it.e
}

// ValueCopy returns a copy of the current value into dst (if provided).
// Mirrors Badger's semantics to aid callers expecting defensive copies.
func (it *Item) ValueCopy(dst []byte) ([]byte, error) {
	if it == nil || it.e == nil {
		return nil, utils.ErrKeyNotFound
	}
	val := it.e.Value
	if kv.IsValuePtr(it.e) {
		if it.vlog == nil {
			return nil, utils.ErrKeyNotFound
		}
		var vp kv.ValuePtr
		vp.Decode(val)
		fetched, cb, err := it.vlog.Read(&vp)
		if cb != nil {
			defer cb()
		}
		if err != nil {
			return nil, err
		}
		it.valueBuf = append(it.valueBuf[:0], fetched...)
		dst = append(dst[:0], it.valueBuf...)
		it.e.Value = it.valueBuf
		it.e.Meta &^= kv.BitValuePointer
		return dst, nil
	}
	if len(val) == 0 {
		return dst[:0], nil
	}
	dst = append(dst[:0], val...)
	return dst, nil
}

// New creates a user-facing iterator over user keys in the default column family.
func New(deps Deps, opt *index.Options) index.Iterator {
	if opt == nil {
		opt = &index.Options{}
	}
	keyOnly := opt.OnlyUseKey
	ctx := deps.Pool.Get()
	ctx.Append(deps.Storage.NewIterators(opt)...)
	itr := &DBIterator{
		vlog:       deps.Vlog,
		pool:       deps.Pool,
		ctx:        ctx,
		keyOnly:    keyOnly,
		lowerBound: opt.LowerBound,
		upperBound: opt.UpperBound,
		hasLower:   len(opt.LowerBound) > 0,
		hasUpper:   len(opt.UpperBound) > 0,
		isAsc:      opt.IsAsc,
	}
	itr.item.vlog = deps.Vlog
	itr.item.e = &itr.entry
	itr.iitr = lsm.NewMergeIterator(ctx.Iterators(), !opt.IsAsc)
	if deps.Storage != nil {
		itr.rtCheck = deps.Storage.HasAnyRangeTombstone()
	}
	if itr.rtCheck {
		itr.rtv = deps.Storage.PinRangeTombstoneView()
	}
	return itr
}

// NewInternal returns an iterator over internal keys (CF marker + user key + timestamp).
// Callers should decode kv.Entry.Key via kv.SplitInternalKey and handle ok=false.
func NewInternal(storage Storage, opt *index.Options) index.Iterator {
	if opt == nil {
		opt = &index.Options{}
	}
	iters := storage.NewIterators(opt)
	return lsm.NewMergeIterator(iters, !opt.IsAsc)
}

// Next advances to the next visible key/value pair.
func (iter *DBIterator) Next() {
	if iter == nil || iter.iitr == nil {
		return
	}
	if iter.seekOutOfRange {
		iter.valid = false
		return
	}
	if !iter.hasPending {
		iter.iitr.Next()
	}
	iter.populate()
}

// Valid reports whether the iterator currently points at a valid item.
func (iter *DBIterator) Valid() bool {
	if iter == nil {
		return false
	}
	return iter.valid
}

// Rewind positions the iterator at the first or last key based on scan direction.
func (iter *DBIterator) Rewind() {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.seekOutOfRange = false
	iter.resetIterationState()
	iter.iitr.Rewind()
	iter.populate()
}

// Seek positions the iterator at the first key >= key in default column family order.
func (iter *DBIterator) Seek(key []byte) {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.seekOutOfRange = false
	iter.resetIterationState()

	// Clamping
	if iter.isAsc {
		if iter.hasUpper && bytes.Compare(key, iter.upperBound) >= 0 {
			iter.valid = false
			iter.seekOutOfRange = true
			return
		}
		if iter.hasLower && bytes.Compare(key, iter.lowerBound) < 0 {
			key = iter.lowerBound
		}
	} else {
		if iter.hasLower && bytes.Compare(key, iter.lowerBound) < 0 {
			iter.valid = false
			iter.seekOutOfRange = true
			return
		}
		if iter.hasUpper && bytes.Compare(key, iter.upperBound) >= 0 {
			key = iter.upperBound
		}
	}

	// Convert user key to internal key for seeking. We use kv.MaxVersion
	// (the non-transactional read upper-bound sentinel) and CFDefault
	// because DBIterator currently doesn't support specifying CF.
	internalKey := kv.InternalKey(kv.CFDefault, key, kv.MaxVersion)
	iter.iitr.Seek(internalKey)
	iter.populate()
}

// Item returns the currently materialized item, or nil when iterator is invalid.
func (iter *DBIterator) Item() index.Item {
	if iter == nil || !iter.valid {
		return nil
	}
	return &iter.item
}

// Close releases underlying iterators and returns pooled iterator context.
func (iter *DBIterator) Close() error {
	if iter == nil {
		return nil
	}
	var err error
	if iter.iitr != nil {
		err = iter.iitr.Close()
		iter.iitr = nil
	}
	iter.valid = false
	iter.seekOutOfRange = false
	iter.valueBuf = iter.valueBuf[:0]
	iter.resetIterationState()
	if iter.pool != nil && iter.ctx != nil {
		iter.pool.Put(iter.ctx)
	}
	if iter.rtv != nil {
		iter.rtv.Close()
		iter.rtv = nil
	}
	iter.ctx = nil
	return err
}

// Err returns the error that stopped iteration, if any.
// Returns nil if iteration completed successfully or is still in progress.
// This method follows the pattern established by EntryIterator and RecordIterator.
func (iter *DBIterator) Err() error {
	if iter == nil {
		return nil
	}
	return iter.err
}

func (iter *DBIterator) populate() {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.valid = false
	iter.item.valueBuf = iter.item.valueBuf[:0]
	if iter.isAsc {
		iter.populateForward()
		return
	}
	iter.populateReverse()
}

func (iter *DBIterator) populateForward() {
	for iter.iitr.Valid() {
		item := iter.iitr.Item()
		if item == nil {
			iter.iitr.Next()
			continue
		}
		entry := item.Entry()
		if entry == nil {
			iter.iitr.Next()
			continue
		}
		cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			// User-facing iterator remains fail-open: skip malformed internal keys.
			iter.iitr.Next()
			continue
		}
		if cf != kv.CFDefault {
			iter.iitr.Next()
			continue
		}
		if iter.hasLower && bytes.Compare(userKey, iter.lowerBound) < 0 {
			iter.iitr.Next()
			continue
		}
		if iter.hasUpper && bytes.Compare(userKey, iter.upperBound) >= 0 {
			iter.valid = false
			return
		}
		if bytes.Equal(userKey, iter.lastUserKey) {
			iter.iitr.Next()
			continue
		}

		ok, err := iter.materializeDecoded(entry, cf, userKey, ts)
		if err != nil {
			iter.err = err
			iter.valid = false
			return
		}
		if ok {
			iter.valid = true
			iter.setLastUserKey(userKey)
			return
		}
		iter.setLastUserKey(userKey)
		iter.iitr.Next()
	}
}

func (iter *DBIterator) populateReverse() {
	for {
		entry, fromPending := iter.takeEntry()
		if entry == nil {
			if fromPending {
				continue
			}
			if iter.iitr == nil || !iter.iitr.Valid() {
				return
			}
			iter.iitr.Next()
			continue
		}
		cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			iter.advance(fromPending)
			continue
		}
		if cf != kv.CFDefault {
			iter.advance(fromPending)
			continue
		}
		if iter.hasLower && bytes.Compare(userKey, iter.lowerBound) < 0 {
			iter.valid = false
			return
		}
		if iter.hasUpper && bytes.Compare(userKey, iter.upperBound) >= 0 {
			iter.advance(fromPending)
			continue
		}

		iter.setLastUserKey(userKey)
		latest := iter.snapshotEntry(entry)
		latestCF := cf
		latestTS := ts

		iter.advance(fromPending)
		// Internal keys sort by user key, then by descending version.
		// Under reverse iteration this means we observe one user key's versions
		// from older to newer while scanning this loop. Keep overwriting `latest`
		// so the final materialized entry is the newest visible version.
		for iter.iitr.Valid() {
			item := iter.iitr.Item()
			if item == nil {
				iter.iitr.Next()
				continue
			}
			nextEntry := item.Entry()
			if nextEntry == nil {
				iter.iitr.Next()
				continue
			}
			nextCF, nextUserKey, nextTs, ok := kv.SplitInternalKey(nextEntry.Key)
			if !ok {
				iter.iitr.Next()
				continue
			}
			if nextCF != kv.CFDefault {
				iter.iitr.Next()
				continue
			}
			if !bytes.Equal(nextUserKey, iter.lastUserKey) {
				iter.stashPending(nextEntry)
				break
			}
			latest = iter.snapshotEntry(nextEntry)
			latestCF = nextCF
			latestTS = nextTs
			iter.iitr.Next()
		}

		ok, err := iter.materializeDecoded(latest, latestCF, iter.lastUserKey, latestTS)
		if err != nil {
			iter.err = err
			iter.valid = false
			return
		}
		if ok {
			iter.valid = true
			return
		}
		if !iter.iitr.Valid() && !iter.hasPending {
			return
		}
	}
}

func (iter *DBIterator) resetIterationState() {
	iter.lastUserKey = iter.lastUserKey[:0]
	iter.pendingKey = iter.pendingKey[:0]
	iter.pendingVal = iter.pendingVal[:0]
	iter.latestKey = iter.latestKey[:0]
	iter.latestVal = iter.latestVal[:0]
	iter.hasPending = false
	iter.err = nil
}

func (iter *DBIterator) setLastUserKey(key []byte) {
	iter.lastUserKey = append(iter.lastUserKey[:0], key...)
}

func (iter *DBIterator) takeEntry() (*kv.Entry, bool) {
	if iter.hasPending {
		iter.hasPending = false
		return &iter.pending, true
	}
	if iter.iitr == nil || !iter.iitr.Valid() {
		return nil, false
	}
	item := iter.iitr.Item()
	if item == nil {
		return nil, false
	}
	return item.Entry(), false
}

func (iter *DBIterator) advance(fromPending bool) {
	if fromPending {
		return
	}
	if iter.iitr != nil {
		iter.iitr.Next()
	}
}

func (iter *DBIterator) stashPending(entry *kv.Entry) {
	if iter == nil || entry == nil || iter.iitr == nil {
		return
	}
	iter.pendingKey = append(iter.pendingKey[:0], entry.Key...)
	iter.pendingVal = append(iter.pendingVal[:0], entry.Value...)
	iter.pending = kv.Entry{
		Key:       iter.pendingKey,
		Value:     iter.pendingVal,
		ExpiresAt: entry.ExpiresAt,
		Meta:      entry.Meta,
		CF:        entry.CF,
		Version:   entry.Version,
	}
	iter.hasPending = true
	iter.iitr.Next()
}

func (iter *DBIterator) snapshotEntry(entry *kv.Entry) *kv.Entry {
	if iter == nil || entry == nil {
		return nil
	}
	iter.latestKey = append(iter.latestKey[:0], entry.Key...)
	iter.latestVal = append(iter.latestVal[:0], entry.Value...)
	iter.latest = kv.Entry{
		Key:       iter.latestKey,
		Value:     iter.latestVal,
		ExpiresAt: entry.ExpiresAt,
		Meta:      entry.Meta,
		CF:        entry.CF,
		Version:   entry.Version,
	}
	return &iter.latest
}

func (iter *DBIterator) materializeDecoded(src *kv.Entry, cf kv.ColumnFamily, userKey []byte, ts uint64) (bool, error) {
	if iter == nil || src == nil {
		return false, nil
	}
	if src.IsDeletedOrExpired() {
		return false, nil
	}
	// Skip range tombstone entries themselves
	if src.IsRangeDelete() {
		return false, nil
	}
	iter.entry = kv.Entry{
		Key:          src.Key,
		Value:        src.Value,
		ExpiresAt:    src.ExpiresAt,
		CF:           src.CF,
		Meta:         src.Meta,
		Version:      src.Version,
		Offset:       src.Offset,
		Hlen:         src.Hlen,
		ValThreshold: src.ValThreshold,
	}
	// Check if this key is covered by a range tombstone.
	if iter.rtCheck && iter.rtv != nil && iter.rtv.IsKeyCovered(cf, userKey, ts) {
		return false, nil
	}
	iter.entry.Key = userKey
	iter.entry.CF = cf
	if ts != 0 {
		iter.entry.Version = ts
	}
	if kv.IsValuePtr(src) {
		if iter.keyOnly {
			// Leave pointer encoded; defer value fetch to Item.ValueCopy.
			iter.entry.Value = src.Value
			iter.item.valueBuf = iter.item.valueBuf[:0]
		} else {
			var vp kv.ValuePtr
			vp.Decode(src.Value)
			val, cb, err := iter.vlog.Read(&vp)
			if cb != nil {
				defer cb()
			}
			if err != nil {
				return false, errors.Wrapf(err, "value-log read failed for key %q", userKey)
			}
			iter.valueBuf = append(iter.valueBuf[:0], val...)
			iter.entry.Value = iter.valueBuf
			iter.entry.Meta &^= kv.BitValuePointer
			iter.item.valueBuf = iter.entry.Value
		}
	} else {
		if src.Value == nil {
			return false, nil
		}
		iter.entry.Value = src.Value
		iter.item.valueBuf = iter.entry.Value
	}
	iter.item.e = &iter.entry
	return true, nil
}
