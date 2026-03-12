package NoKV

import (
	"bytes"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
)

// DBIterator wraps the merged LSM iterators and optionally resolves value-log pointers.
type DBIterator struct {
	iitr utils.Iterator
	vlog *valueLog
	pool *iteratorPool
	ctx  *iteratorContext
	rtv  *lsm.RangeTombstoneView
	// rtCheck indicates whether this iterator snapshot needs tombstone
	// coverage checks.
	rtCheck bool
	// keyOnly avoids eager value log materialisation when true.
	keyOnly bool

	lowerBound []byte
	upperBound []byte
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
}

// Item is the user-facing iterator item backed by an entry and optional vlog reader.
type Item struct {
	e        *kv.Entry
	vlog     *valueLog
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
		fetched, cb, err := it.vlog.read(&vp)
		if cb != nil {
			defer kv.RunCallback(cb)
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

// NewIterator creates a DB-level iterator over user keys in the default column family.
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	if opt == nil {
		opt = &utils.Options{}
	}
	keyOnly := opt.OnlyUseKey
	ctx := db.iterPool.get()
	ctx.iters = append(ctx.iters, db.lsm.NewIterators(opt)...)
	itr := &DBIterator{
		vlog:       db.vlog,
		pool:       db.iterPool,
		ctx:        ctx,
		keyOnly:    keyOnly,
		lowerBound: opt.LowerBound,
		upperBound: opt.UpperBound,
		isAsc:      opt.IsAsc,
	}
	itr.item.vlog = db.vlog
	itr.item.e = &itr.entry
	itr.iitr = lsm.NewMergeIterator(ctx.iters, !opt.IsAsc)
	if db.lsm != nil {
		itr.rtCheck = db.lsm.HasAnyRangeTombstone()
	}
	if itr.rtCheck {
		itr.rtv = db.lsm.PinRangeTombstoneView()
	}
	return itr
}

// NewInternalIterator returns an iterator over internal keys (CF marker + user key + timestamp).
// Callers should decode kv.Entry.Key via kv.SplitInternalKey and handle ok=false.
func (db *DB) NewInternalIterator(opt *utils.Options) utils.Iterator {
	if opt == nil {
		opt = &utils.Options{}
	}
	iters := db.lsm.NewIterators(opt)
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
		if len(iter.upperBound) > 0 && bytes.Compare(key, iter.upperBound) >= 0 {
			iter.valid = false
			iter.seekOutOfRange = true
			return
		}
		if len(iter.lowerBound) > 0 && bytes.Compare(key, iter.lowerBound) < 0 {
			key = iter.lowerBound
		}
	} else {
		if len(iter.lowerBound) > 0 && bytes.Compare(key, iter.lowerBound) < 0 {
			iter.valid = false
			iter.seekOutOfRange = true
			return
		}
		if len(iter.upperBound) > 0 && bytes.Compare(key, iter.upperBound) >= 0 {
			key = iter.upperBound
		}
	}

	// Convert user key to internal key for seeking.
	// We use MaxUint64 as version to seek to the latest version of the key.
	// We default to CFDefault as DBIterator currently doesn't support specifying CF.
	internalKey := kv.InternalKey(kv.CFDefault, key, nonTxnMaxVersion)
	iter.iitr.Seek(internalKey)
	iter.populate()
}

// Item returns the currently materialized item, or nil when iterator is invalid.
func (iter *DBIterator) Item() utils.Item {
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
		iter.pool.put(iter.ctx)
	}
	if iter.rtv != nil {
		iter.rtv.Close()
		iter.rtv = nil
	}
	iter.ctx = nil
	return err
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
		cf, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			// User-facing iterator remains fail-open: skip malformed internal keys.
			iter.advance(fromPending)
			continue
		}
		if cf != kv.CFDefault {
			iter.advance(fromPending)
			continue
		}
		if len(iter.lastUserKey) > 0 && bytes.Equal(userKey, iter.lastUserKey) {
			iter.advance(fromPending)
			continue
		}
		if len(iter.lowerBound) > 0 && bytes.Compare(userKey, iter.lowerBound) < 0 {
			iter.setLastUserKey(userKey)
			iter.advance(fromPending)
			continue
		}
		if len(iter.upperBound) > 0 && bytes.Compare(userKey, iter.upperBound) >= 0 {
			iter.valid = false
			return
		}

		if iter.materialize(entry) {
			iter.valid = true
			iter.setLastUserKey(userKey)
			return
		}
		iter.setLastUserKey(userKey)
		iter.advance(fromPending)
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
		cf, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			iter.advance(fromPending)
			continue
		}
		if cf != kv.CFDefault {
			iter.advance(fromPending)
			continue
		}
		if len(iter.lowerBound) > 0 && bytes.Compare(userKey, iter.lowerBound) < 0 {
			iter.valid = false
			return
		}
		if len(iter.upperBound) > 0 && bytes.Compare(userKey, iter.upperBound) >= 0 {
			iter.advance(fromPending)
			continue
		}

		iter.setLastUserKey(userKey)
		latest := iter.snapshotEntry(entry)

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
			nextCF, nextUserKey, _, ok := kv.SplitInternalKey(nextEntry.Key)
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
			iter.iitr.Next()
		}

		if iter.materialize(latest) {
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

func (iter *DBIterator) materialize(src *kv.Entry) bool {
	if iter == nil || src == nil {
		return false
	}
	if src.IsDeletedOrExpired() {
		return false
	}
	// Skip range tombstone entries themselves
	if src.IsRangeDelete() {
		return false
	}
	iter.entry = *src
	cf, userKey, ts, ok := kv.SplitInternalKey(iter.entry.Key)
	if !ok {
		// User-facing iterator remains fail-open: skip malformed internal keys.
		return false
	}
	// Check if this key is covered by a range tombstone.
	if iter.rtCheck && iter.rtv != nil && iter.rtv.IsKeyCovered(cf, userKey, ts) {
		return false
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
			val, cb, err := iter.vlog.read(&vp)
			if cb != nil {
				defer kv.RunCallback(cb)
			}
			if err != nil {
				return false
			}
			iter.valueBuf = append(iter.valueBuf[:0], val...)
			iter.entry.Value = iter.valueBuf
			iter.entry.Meta &^= kv.BitValuePointer
			iter.item.valueBuf = iter.entry.Value
		}
	} else {
		if src.Value == nil {
			return false
		}
		iter.entry.Value = src.Value
		iter.item.valueBuf = iter.entry.Value
	}
	iter.item.e = &iter.entry
	return true
}
