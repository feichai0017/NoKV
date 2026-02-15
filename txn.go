package NoKV

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

type oracle struct {
	detectConflicts bool // Determines if the txns should be checked for conflicts.

	sync.Mutex // Guards committedTxns/intentTable cleanup and conflict checks.
	nextTxnTs  atomic.Uint64

	// Used to block NewTransaction, so all previous commits ars visible to a new read.
	txnMark *utils.WaterMark

	// Either of these is used to determine which versions can be permanently
	// discarded during compaction.
	readMark *utils.WaterMark // Used by DB.

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	committedTxns []committedTxn
	lastCleanupTs uint64
	intentTable   map[uint64]uint64 // key hash -> latest commit ts

	// closer is used to stop watermarks.
	closer *utils.Closer

	txnStarted   uint64
	txnCommitted uint64
	txnConflicts uint64
	txnActive    int64
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[uint64]struct{}
}

func cloneConflictKeys(src map[uint64]struct{}) map[uint64]struct{} {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[uint64]struct{}, len(src))
	for k := range src {
		dst[k] = struct{}{}
	}
	return dst
}

func newOracle(opt Options) *oracle {
	orc := &oracle{
		detectConflicts: opt.DetectConflicts,
		intentTable:     make(map[uint64]uint64),
		// We're not initializing nextTxnTs and readOnlyTs. It would be done after replay in Open.
		//
		// WaterMarks must be 64-bit aligned for atomic package, hence we must use pointers here.
		// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
		readMark: &utils.WaterMark{Name: "nokv.PendingReads"},
		txnMark:  &utils.WaterMark{Name: "nokv.TxnTimestamp"},
		closer:   utils.NewCloserInitial(2),
	}
	orc.nextTxnTs.Store(1)
	orc.readMark.Init(orc.closer)
	orc.txnMark.Init(orc.closer)
	return orc
}

func (o *oracle) initCommitState(committed uint64) {
	if o == nil || committed == 0 {
		return
	}

	o.Lock()
	if committed > o.lastCleanupTs {
		o.lastCleanupTs = committed
	}
	o.Unlock()
	if committed >= o.nextTxnTs.Load() {
		o.nextTxnTs.Store(committed + 1)
	}

	o.readMark.SetDoneUntil(committed)
	o.txnMark.SetDoneUntil(committed)
	o.txnMark.SetLastIndex(committed)
}

// Stop shuts down watermark workers used by the oracle timestamp tracker.
func (o *oracle) Stop() {
	o.closer.SignalAndWait()
}

func (o *oracle) trackTxnStart() {
	atomic.AddUint64(&o.txnStarted, 1)
	atomic.AddInt64(&o.txnActive, 1)
}

func (o *oracle) trackTxnCommit() {
	atomic.AddUint64(&o.txnCommitted, 1)
}

func (o *oracle) trackTxnConflict() {
	atomic.AddUint64(&o.txnConflicts, 1)
}

func (o *oracle) trackTxnFinish() {
	atomic.AddInt64(&o.txnActive, -1)
}

func (o *oracle) txnMetricsSnapshot() metrics.TxnMetrics {
	return metrics.TxnMetrics{
		Started:   atomic.LoadUint64(&o.txnStarted),
		Committed: atomic.LoadUint64(&o.txnCommitted),
		Conflicts: atomic.LoadUint64(&o.txnConflicts),
		Active:    atomic.LoadInt64(&o.txnActive),
	}
}

func (o *oracle) readTs() uint64 {
	readTs := o.nextTxnTs.Load() - 1
	if last := o.txnMark.LastIndex(); last < readTs {
		readTs = last
	}
	o.readMark.Begin(readTs)

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	utils.Check(o.txnMark.WaitForMark(context.Background(), readTs))
	return readTs
}

// hasConflict must be called while having a lock.
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	// Fast intent check: if any read key hash has a commit newer than readTs, short-circuit.
	if o.intentTable != nil {
		for _, ro := range txn.reads {
			if ts, ok := o.intentTable[ro]; ok && ts > txn.readTs {
				return true
			}
		}
	}
	for _, committedTxn := range o.committedTxns {
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		if committedTxn.ts <= txn.readTs {
			continue
		}

		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true
			}
		}
	}

	return false
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) {
		return 0, true
	}

	o.doneRead(txn)
	o.cleanupCommittedTransactions()

	// This is the general case, when user doesn't specify the read and commit ts.
	ts := o.nextTxnTs.Add(1) - 1

	utils.AssertTrue(ts >= o.lastCleanupTs)
	o.txnMark.Begin(ts)

	if o.detectConflicts {
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		// txn.conflictKeys is pooled; copy it so future reuse does not clear history.
		copied := cloneConflictKeys(txn.conflictKeys)
		o.committedTxns = append(o.committedTxns, committedTxn{
			ts:           ts,
			conflictKeys: copied,
		})
		if o.intentTable != nil {
			for k := range copied {
				o.intentTable[k] = ts
			}
		}
	}

	return ts, false
}

func (o *oracle) doneRead(txn *Txn) {
	if !txn.doneRead {
		txn.doneRead = true
		o.readMark.Done(txn.readTs)
	}
}

func (o *oracle) cleanupCommittedTransactions() { // Must be called under o.Lock
	if !o.detectConflicts {
		// When detectConflicts is set to false, we do not store any
		// committedTxns and so there's nothing to clean up.
		return
	}
	// Same logic as discardAtOrBelow but unlocked
	maxReadTs := o.readMark.DoneUntil()

	utils.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	if maxReadTs == o.lastCleanupTs {
		return
	}
	o.lastCleanupTs = maxReadTs

	tmp := o.committedTxns[:0]
	for _, txn := range o.committedTxns {
		if txn.ts <= maxReadTs {
			if o.intentTable != nil {
				for k := range txn.conflictKeys {
					if ts, ok := o.intentTable[k]; ok && ts == txn.ts {
						delete(o.intentTable, k)
					}
				}
			}
			continue
		}
		tmp = append(tmp, txn)
	}
	o.committedTxns = tmp
}

func (o *oracle) doneCommit(cts uint64) {
	o.txnMark.Done(cts)
}

// Txn is an optimistic transaction view with buffered writes and conflict tracking.
type Txn struct {
	readTs   uint64
	commitTs uint64
	size     int64
	count    int64
	db       *DB

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	pendingWrites map[string]*kv.Entry // cache stores any writes done by txn.

	numIterators int32
	discarded    bool
	returned     bool
	doneRead     bool
	update       bool // update is used to conditionally keep track of reads.
}

type pendingWritesIterator struct {
	entries  []*kv.Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

// Item returns the current buffered write as an iterator item.
func (pi *pendingWritesIterator) Item() utils.Item {
	return pi.entries[pi.nextIdx]
}

// Next advances to the next buffered write.
func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

// Rewind resets the buffered-write cursor to the first entry.
func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

// Seek positions the buffered-write iterator at the first entry matching key order.
func (pi *pendingWritesIterator) Seek(key []byte) {
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

// Key returns the current internal key from the buffered write iterator.
func (pi *pendingWritesIterator) Key() []byte {
	utils.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return entry.Key
}

// Value returns the current value struct from the buffered write iterator.
func (pi *pendingWritesIterator) Value() kv.ValueStruct {
	utils.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return kv.ValueStruct{
		Value:     entry.Value,
		Meta:      entry.Meta,
		ExpiresAt: entry.ExpiresAt,
		Version:   kv.ParseTs(entry.Key),
	}
}

// Valid reports whether the buffered-write cursor is within bounds.
func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

// Close is a no-op because pending writes are owned by the transaction.
func (pi *pendingWritesIterator) Close() error {
	return nil
}

func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*kv.Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		dup := *e
		dup.Key = kv.InternalKey(dup.CF, e.Key, txn.readTs)
		entries = append(entries, &dup)
	}
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}

func (txn *Txn) checkSize(e *kv.Entry) error {
	count := txn.count + 1
	// Extra bytes for the version in key.
	size := txn.size + int64(e.EstimateSize(int(txn.db.valueThreshold())+10))
	if count >= txn.db.opt.MaxBatchCount || size >= txn.db.opt.MaxBatchSize {
		return utils.ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

const maxKeySize = 65000

func (txn *Txn) modify(e *kv.Entry) error {
	switch {
	case !txn.update:
		return utils.ErrReadOnlyTxn
	case txn.discarded:
		return utils.ErrDiscardedTxn
	case len(e.Key) == 0:
		return utils.ErrEmptyKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, e.Key)
	}

	if txn.db != nil {
		if err := txn.db.maybeThrottleWrite(e.CF, e.Key); err != nil {
			return err
		}
	}

	if err := txn.checkSize(e); err != nil {
		return err
	}

	// The txn.conflictKeys is used for conflict detection. If conflict detection
	// is disabled, we don't need to store key hashes in this map.
	cfKey := kv.EncodeKeyWithCF(e.CF, e.Key)
	if txn.db.opt.DetectConflicts {
		fp := kv.MemHash(cfKey) // Avoid dealing with byte arrays.
		txn.conflictKeys[fp] = struct{}{}
	}

	txn.pendingWrites[string(cfKey)] = e
	return nil
}

// Set adds a key-value pair to the database.
// It will return ErrReadOnlyTxn if update flag was set to false when creating the transaction.
//
// The current transaction keeps a reference to the key and val byte slice
// arguments. Users must not modify key and val until the end of the transaction.
func (txn *Txn) Set(key, val []byte) error {
	return txn.SetEntry(kv.NewEntry(key, val))
}

// SetEntry takes an kv.Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
//
// The current transaction keeps a reference to the entry passed in argument.
// Users must not modify the entry until the end of the transaction.
func (txn *Txn) SetEntry(e *kv.Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
//
// This is done by adding a delete marker for the key at commit timestamp.  Any
// reads happening before this timestamp would be unaffected. Any reads after
// this commit would see the deletion.
//
// The current transaction keeps a reference to the key byte slice argument.
// Users must not modify the key until the end of the transaction.
func (txn *Txn) Delete(key []byte) error {
	e := kv.NewEntry(key, nil)
	e.Meta = kv.BitDelete
	if err := txn.modify(e); err != nil {
		e.DecrRef()
		return err
	}
	return nil
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	} else if txn.discarded {
		return nil, utils.ErrDiscardedTxn
	}

	item = new(Item)
	item.e = new(kv.Entry)
	if txn.db != nil {
		item.vlog = txn.db.vlog
	}
	if txn.update {
		cfKey := kv.EncodeKeyWithCF(kv.CFDefault, key)
		if e, has := txn.pendingWrites[string(cfKey)]; has && bytes.Equal(key, e.Key) {
			if isDeletedOrExpired(e.Meta, e.ExpiresAt) {
				return nil, utils.ErrKeyNotFound
			}
			// Fulfill from cache.
			item.e.Meta = e.Meta
			item.e.Value = kv.SafeCopy(nil, e.Value)
			item.e.Key = kv.SafeCopy(nil, key)
			item.e.CF = e.CF
			item.e.Version = txn.readTs
			item.e.ExpiresAt = e.ExpiresAt
			// We probably don't need to set db on item here.
			txn.db.recordRead(key)
			txn.db.recordCFRead(e.CF, 1)
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		txn.addReadKey(cfKey)
	}

	// Query using the transaction's readTs.
	seek := kv.InternalKey(kv.CFDefault, key, txn.readTs)
	vs, err := txn.db.loadBorrowedEntry(seek)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			return nil, err
		}
		return nil, utils.Wrapf(err, "DB::Get key: %q", key)
	}
	defer vs.DecrRef()

	if vs.Value == nil && vs.Meta == 0 {
		return nil, utils.ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, utils.ErrKeyNotFound
	}
	item.e = cloneEntry(vs, kv.CFDefault)
	txn.db.recordRead(key)
	txn.db.recordCFRead(item.e.CF, 1)
	return item, nil
}

func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := kv.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if atomic.LoadInt32(&txn.numIterators) > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true

	// Release entries in pendingWrites.
	if txn.update {
		for _, e := range txn.pendingWrites {
			e.DecrRef()
		}
		txn.clearPendingWrites()
	}

	txn.db.orc.doneRead(txn)
	txn.db.orc.trackTxnFinish()

	txn.recycle()
}

func (txn *Txn) clearPendingWrites() {
	if txn.pendingWrites == nil {
		return
	}
	for k := range txn.pendingWrites {
		delete(txn.pendingWrites, k)
	}
}

func (txn *Txn) recycle() {
	if txn.returned {
		return
	}
	txn.returned = true
	txn.db = nil
	txn.reads = txn.reads[:0]
	txn.commitTs = 0
	txn.readTs = 0
	txn.size = 0
	txn.count = 0
	txn.doneRead = false
	txn.update = false
	atomic.StoreInt32(&txn.numIterators, 0)
	txn.clearPendingWrites()
	if txn.conflictKeys != nil {
		for k := range txn.conflictKeys {
			delete(txn.conflictKeys, k)
		}
	}
}

func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	var entries []*kv.Entry
	var commitTs uint64

	var conflict bool
	commitTs, conflict = orc.newCommitTs(txn)
	if conflict {
		orc.trackTxnConflict()
		return nil, utils.ErrConflict
	}

	setVersion := func(e *kv.Entry) {
		if e.Version == 0 {
			e.Version = commitTs
		}
	}
	for _, e := range txn.pendingWrites {
		setVersion(e)
	}

	entries = make([]*kv.Entry, 0, len(txn.pendingWrites))
	processEntry := func(e *kv.Entry) {
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = kv.InternalKey(e.CF, e.Key, e.Version)
		entries = append(entries, e)
	}

	for _, e := range txn.pendingWrites {
		processEntry(e)
	}
	txn.clearPendingWrites() // Clear the map to prevent double-free in Discard.

	req, err := txn.db.sendToWriteCh(entries, true)
	if err != nil {
		orc.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		if err == nil {
			orc.trackTxnCommit()
		}
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		orc.doneCommit(commitTs)
		return err
	}
	return ret, nil
}

func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return errors.New("Trying to commit a discarded txn")
	}
	return nil
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided, will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (txn *Txn) Commit() error {
	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		return err
	}
	defer txn.Discard()

	// txn.conflictKeys can be zero if conflict detection is turned off.
	// So we should check txn.pendingWrites.
	if len(txn.pendingWrites) == 0 {
		return nil // Nothing to do, but Discard() will run via defer.
	}

	txnCb, err := txn.commitAndSend()
	if err != nil {
		return err
	}
	// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

	// Value-log errors are surfaced via req.Wait(); the value-log manager rewinds
	// partial batches before the error is returned so the LSM state remains
	// unchanged on failure.
	return txnCb()
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("txn callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for txn.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
}

// CommitWith acts like Commit, but takes a callback, which gets run via a
// goroutine to avoid blocking this function. The callback is guaranteed to run,
// so it is safe to increment sync.WaitGroup before calling CommitWith, and
// decrementing it in the callback; to block until all callbacks are run.
func (txn *Txn) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if err := txn.commitPrecheck(); err != nil {
		cb(err)
		return
	}

	if len(txn.pendingWrites) == 0 {
		txn.Discard()
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// NewTransaction creates a transaction bound to the current read timestamp.
func (db *DB) NewTransaction(update bool) *Txn {
	return db.newTransaction(update)
}

func (db *DB) newTransaction(update bool) *Txn {
	txn := &Txn{}
	txn.db = db
	txn.update = update
	txn.count = 1 // One extra entry for BitFin.
	txn.size = 0
	txn.commitTs = 0
	txn.readTs = 0
	txn.discarded = false
	txn.returned = false
	txn.doneRead = false
	txn.reads = txn.reads[:0]
	atomic.StoreInt32(&txn.numIterators, 0)
	if update {
		if db.opt.DetectConflicts {
			if txn.conflictKeys == nil {
				txn.conflictKeys = make(map[uint64]struct{})
			} else {
				for k := range txn.conflictKeys {
					delete(txn.conflictKeys, k)
				}
			}
		} else {
			txn.conflictKeys = nil
		}
		if txn.pendingWrites == nil {
			txn.pendingWrites = make(map[string]*kv.Entry)
		} else {
			for k := range txn.pendingWrites {
				delete(txn.pendingWrites, k)
			}
		}
	} else {
		txn.conflictKeys = nil
		if txn.pendingWrites != nil {
			for k := range txn.pendingWrites {
				delete(txn.pendingWrites, k)
			}
		}
	}
	db.orc.trackTxnStart()
	txn.readTs = db.orc.readTs()

	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return utils.ErrDBClosed
	}
	txn := db.NewTransaction(false)

	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
// Update cannot be used with managed transactions.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return utils.ErrDBClosed
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
