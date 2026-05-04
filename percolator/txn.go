// Package percolator implements Google-Percolator-style distributed
// MVCC two-phase commit over NoKV's key/value substrate.
//
// Protocol ops: Prewrite, Commit, Rollback, ResolveLock, CheckTxnStatus,
// TxnHeartBeat.
// Concurrency is controlled by a striped-mutex latch manager
// (percolator/latch) shared per raftstore/kv service instance.
// The timestamp oracle is provided by coordinator/tso.
//
// This package is used in distributed mode only. Embedded DB APIs
// go straight through the engine substrate without percolator's 2PC.
//
// See docs/percolator.md for the protocol walkthrough.
package percolator

import (
	"bytes"
	"time"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/percolator/mvcc"
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
)

// Prewrite applies mutation prewrites for a single region transaction.
func Prewrite(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.PrewriteRequest) []*kvrpcpb.KeyError {
	if req == nil {
		return nil
	}
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mut := range req.Mutations {
		if mut != nil && len(mut.Key) > 0 {
			keys = append(keys, mut.Key)
		}
	}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	var errs []*kvrpcpb.KeyError
	ops := make([]versionedOp, 0, len(req.Mutations)*3)
	for _, mut := range req.Mutations {
		if mut == nil {
			continue
		}
		planned, err := planPrewriteMutation(reader, req, mut)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		ops = append(ops, planned...)
	}
	if len(errs) > 0 {
		return errs
	}
	// Prewrite is the lock admission boundary for a region request. Validation
	// happens for every mutation before storage is touched, so semantic key
	// errors never hide partial locks. Storage failures are retryable: replaying
	// the same start_ts observes its own locks and finishes the missing keys.
	if err := applyVersionedOps(db, ops...); err != nil {
		return []*kvrpcpb.KeyError{keyErrorRetryable(err)}
	}
	return nil
}

func planPrewriteMutation(reader *Reader, req *kvrpcpb.PrewriteRequest, mut *kvrpcpb.Mutation) ([]versionedOp, *kvrpcpb.KeyError) {
	key := mut.GetKey()
	if len(key) == 0 {
		return nil, keyErrorAbort(errEmptyMutationKey)
	}
	lock, err := reader.GetLock(key)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if lock != nil && lock.Ts != req.StartVersion {
		return nil, keyErrorLocked(key, lock)
	}
	if write, commitTs, err := reader.MostRecentWrite(key); err != nil {
		return nil, keyErrorRetryable(err)
	} else if write != nil && commitTs >= req.StartVersion {
		return nil, keyErrorWriteConflict(key, req.PrimaryLock, commitTs, write.StartTs, req.StartVersion)
	}
	if mut.GetAssertionNotExist() {
		exists, err := keyExistsAt(reader, key, req.StartVersion)
		if err != nil {
			return nil, keyErrorRetryable(err)
		}
		if exists {
			return nil, keyErrorAlreadyExists(key)
		}
	}
	ops := make([]versionedOp, 0, 3)
	switch mut.Op {
	case kvrpcpb.Mutation_Put:
		ops = append(ops,
			versionedOp{cf: kv.CFDefault, key: key, version: req.StartVersion, meta: kv.BitDelete},
			versionedOp{cf: kv.CFDefault, key: key, version: req.StartVersion, value: mut.Value, expires: mut.GetExpiresAt()},
		)
	case kvrpcpb.Mutation_Delete, kvrpcpb.Mutation_Lock:
		ops = append(ops,
			versionedOp{cf: kv.CFDefault, key: key, version: req.StartVersion, meta: kv.BitDelete},
		)
	default:
		return nil, keyErrorAbortf(errUnsupportedMutationOp, "%v", mut.Op)
	}
	newLock := mvcc.Lock{
		Primary:     kv.SafeCopy(nil, req.PrimaryLock),
		Ts:          req.StartVersion,
		StartTime:   currentPhysicalTimeMillis(),
		TTL:         req.LockTtl,
		Kind:        mut.Op,
		MinCommitTs: req.MinCommitTs,
	}
	encoded := mvcc.EncodeLock(newLock)
	ops = append(ops, versionedOp{cf: kv.CFLock, key: key, version: lockColumnTs, value: encoded})
	return ops, nil
}

// validateCommitVersion rejects commits that would violate MVCC ordering.
func validateCommitVersion(StartVersion uint64, CommitVersion uint64) *kvrpcpb.KeyError {
	if CommitVersion <= StartVersion {
		return keyErrorAbort(errCommitVersionNotAfterStart)
	}
	return nil
}

// Commit finalises earlier prewrites by removing locks and writing commit
// records. A non-nil KeyError is returned when commit should abort.
func Commit(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.CommitRequest) *kvrpcpb.KeyError {
	if req == nil {
		return nil
	}
	if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
		return err
	}
	guard := latches.Acquire(req.Keys)
	defer guard.Release()

	reader := NewReader(db)
	ops := make([]versionedOp, 0, len(req.Keys)*2)
	for _, key := range req.Keys {
		planned, err := planCommitKey(reader, key, req.StartVersion, req.CommitVersion)
		if err != nil {
			return err
		}
		ops = append(ops, planned...)
	}
	// Commit remains Percolator-idempotent: already committed keys produce no
	// write op, while residual locks are cleaned with the commit records for the
	// same key. The DB may fan multi-key requests out by LSM shard affinity.
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

// ApplyFSMetaCreate atomically materializes a filesystem create operation that
// is already ordered by one region's Raft log. Unlike Prewrite/Commit, it never
// exposes locks: every mutation becomes visible at commitVersion in one local
// storage batch, or none of them do.
func ApplyFSMetaCreate(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.FSMetaCreateRequest) *kvrpcpb.KeyError {
	if req == nil {
		return nil
	}
	if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
		return err
	}
	mutations := req.GetMutations()
	if len(mutations) != 2 || mutations[0] == nil || mutations[1] == nil {
		return keyErrorAbort(errInvalidFSMetaCreate)
	}
	keys := make([][]byte, 0, len(mutations))
	for _, mut := range mutations {
		if mut != nil && len(mut.Key) > 0 {
			keys = append(keys, mut.Key)
		}
	}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	if applied, err := fsmetaCreateAlreadyApplied(db, reader, req); err != nil {
		return keyErrorRetryable(err)
	} else if applied {
		return nil
	}

	primary := mutations[0].GetKey()
	ops := make([]versionedOp, 0, len(mutations)*3)
	for i, mut := range mutations {
		if mut == nil {
			continue
		}
		if err := validateFSMetaCreateMutation(i, mut); err != nil {
			return err
		}
		key := mut.GetKey()
		lock, err := reader.GetLock(key)
		if err != nil {
			return keyErrorRetryable(err)
		}
		if lock != nil {
			return keyErrorLocked(key, lock)
		}
		if write, commitTs, err := reader.MostRecentWrite(key); err != nil {
			return keyErrorRetryable(err)
		} else if write != nil && commitTs >= req.StartVersion {
			return keyErrorWriteConflict(key, primary, commitTs, write.StartTs, req.StartVersion)
		}
		if mut.GetAssertionNotExist() {
			exists, err := keyExistsAt(reader, key, req.StartVersion)
			if err != nil {
				return keyErrorRetryable(err)
			}
			if exists {
				return keyErrorAlreadyExists(key)
			}
		}
		ops = append(ops, committedMutationOps(mut, req.StartVersion, req.CommitVersion)...)
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func validateFSMetaCreateMutation(index int, mut *kvrpcpb.Mutation) *kvrpcpb.KeyError {
	if len(mut.GetKey()) == 0 {
		return keyErrorAbort(errEmptyMutationKey)
	}
	switch mut.GetOp() {
	case kvrpcpb.Mutation_Put, kvrpcpb.Mutation_Delete:
	default:
		return keyErrorAbortf(errUnsupportedMutationOp, "%v", mut.GetOp())
	}
	if index < 2 && (mut.GetOp() != kvrpcpb.Mutation_Put || !mut.GetAssertionNotExist()) {
		return keyErrorAbort(errInvalidFSMetaCreate)
	}
	return nil
}

func fsmetaCreateAlreadyApplied(db txnstore.Store, reader *Reader, req *kvrpcpb.FSMetaCreateRequest) (bool, error) {
	anyPresent := false
	allPresent := true
	for _, mut := range req.GetMutations() {
		if mut == nil {
			continue
		}
		write, commitTs, err := reader.GetWriteByStartTs(mut.GetKey(), req.StartVersion)
		if err != nil {
			return false, err
		}
		if write == nil {
			allPresent = false
			continue
		}
		anyPresent = true
		if commitTs != req.CommitVersion || write.Kind != mut.GetOp() {
			return false, nil
		}
		if mut.GetOp() == kvrpcpb.Mutation_Put {
			matches, err := defaultRecordMatches(db, mut, req.StartVersion)
			if err != nil || !matches {
				return false, err
			}
		}
	}
	return anyPresent && allPresent, nil
}

func defaultRecordMatches(db txnstore.Store, mut *kvrpcpb.Mutation, startVersion uint64) (bool, error) {
	entry, err := db.GetInternalEntry(kv.CFDefault, mut.GetKey(), startVersion)
	if err != nil {
		return false, err
	}
	defer entry.DecrRef()
	if entry.Meta&kv.BitDelete > 0 {
		return false, nil
	}
	return bytes.Equal(entry.Value, mut.GetValue()) && entry.ExpiresAt == mut.GetExpiresAt(), nil
}

func committedMutationOps(mut *kvrpcpb.Mutation, startVersion, commitVersion uint64) []versionedOp {
	write := mvcc.EncodeWrite(mvcc.Write{Kind: mut.GetOp(), StartTs: startVersion})
	switch mut.GetOp() {
	case kvrpcpb.Mutation_Put:
		return []versionedOp{
			{cf: kv.CFDefault, key: mut.GetKey(), version: startVersion, meta: kv.BitDelete},
			{cf: kv.CFDefault, key: mut.GetKey(), version: startVersion, value: mut.GetValue(), expires: mut.GetExpiresAt()},
			{cf: kv.CFWrite, key: mut.GetKey(), version: commitVersion, value: write},
		}
	case kvrpcpb.Mutation_Delete:
		return []versionedOp{
			{cf: kv.CFDefault, key: mut.GetKey(), version: startVersion, meta: kv.BitDelete},
			{cf: kv.CFWrite, key: mut.GetKey(), version: commitVersion, value: write},
		}
	default:
		return nil
	}
}

// BatchRollback rolls back the provided keys for the given start version.
func BatchRollback(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.KeyError {
	if req == nil {
		return nil
	}
	guard := latches.Acquire(req.Keys)
	defer guard.Release()
	reader := NewReader(db)
	ops := make([]versionedOp, 0, len(req.Keys)*3)
	for _, key := range req.Keys {
		planned, err := planRollbackKey(reader, key, req.StartVersion)
		if err != nil {
			return err
		}
		ops = append(ops, planned...)
	}
	// Rollback markers and lock tombstones for one key must be planned together;
	// the DB preserves per-key shard affinity when it persists the internal
	// entries, and retries keep rollback idempotent.
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

// ResolveLock resolves locks for the given transaction. commitVersion == 0
// performs a rollback; otherwise the keys are committed.
func ResolveLock(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.ResolveLockRequest) (uint64, *kvrpcpb.KeyError) {
	if req == nil {
		return 0, nil
	}
	if req.CommitVersion != 0 {
		if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
			return 0, err
		}
	}
	guard := latches.Acquire(req.Keys)
	defer guard.Release()

	reader := NewReader(db)
	var resolved uint64
	ops := make([]versionedOp, 0, len(req.Keys)*3)
	seen := make(map[string]struct{}, len(req.Keys))
	for _, key := range req.Keys {
		if len(key) == 0 {
			continue
		}
		keyID := string(key)
		if _, ok := seen[keyID]; ok {
			continue
		}
		seen[keyID] = struct{}{}
		lock, err := reader.GetLock(key)
		if err != nil {
			return resolved, keyErrorRetryable(err)
		}
		if lock == nil || lock.Ts != req.StartVersion {
			continue
		}
		if req.CommitVersion == 0 {
			planned, err := planRollbackKey(reader, key, req.StartVersion)
			if err != nil {
				return resolved, err
			}
			ops = append(ops, planned...)
		} else {
			planned, err := planCommitKeyWithLock(reader, key, lock, req.CommitVersion)
			if err != nil {
				return resolved, err
			}
			ops = append(ops, planned...)
		}
		resolved++
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return 0, keyErrorRetryable(err)
	}
	return resolved, nil
}

// CheckTxnStatus inspects the primary lock state and optionally rolls back
// expired transactions.
func CheckTxnStatus(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.CheckTxnStatusRequest) *kvrpcpb.CheckTxnStatusResponse {
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	if req == nil {
		return resp
	}
	keys := [][]byte{req.PrimaryKey}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	lock, err := reader.GetLock(req.PrimaryKey)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if lock != nil {
		if lock.Ts != req.LockTs {
			resp.Error = keyErrorLocked(req.PrimaryKey, lock)
			return resp
		}
		if isLockExpired(lock, req.CurrentTime) {
			if err := rollbackKey(db, reader, req.PrimaryKey, req.LockTs); err != nil {
				resp.Error = err
				return resp
			}
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback
			return resp
		}
		if req.CallerStartTs > 0 && lock.MinCommitTs < req.CallerStartTs+1 {
			lock.MinCommitTs = req.CallerStartTs + 1
			if err := applyVersionedOps(db, versionedOp{
				cf:      kv.CFLock,
				key:     req.PrimaryKey,
				version: lockColumnTs,
				value:   mvcc.EncodeLock(*lock),
			}); err != nil {
				resp.Error = keyErrorRetryable(err)
				return resp
			}
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusMinCommitTsPushed
		}
		resp.LockTtl = lock.TTL
		return resp
	}

	write, commitTs, err := reader.GetWriteByStartTs(req.PrimaryKey, req.LockTs)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if write != nil {
		if write.Kind == kvrpcpb.Mutation_Rollback {
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback
			return resp
		}
		resp.CommitVersion = commitTs
		return resp
	}

	if req.RollbackIfNotExist {
		if err := rollbackKey(db, reader, req.PrimaryKey, req.LockTs); err != nil {
			resp.Error = err
		} else {
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback
		}
	}
	return resp
}

// TxnHeartBeat extends the primary lock TTL for a live transaction. It never
// resurrects an expired or already-resolved primary lock.
func TxnHeartBeat(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.TxnHeartBeatRequest) *kvrpcpb.TxnHeartBeatResponse {
	resp := &kvrpcpb.TxnHeartBeatResponse{}
	if req == nil {
		return resp
	}
	if len(req.PrimaryKey) == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatPrimaryRequired)
		return resp
	}
	if req.StartVersion == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatStartRequired)
		return resp
	}
	if req.TtlExtension == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatTTLRequired)
		return resp
	}
	if req.CurrentTime == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatTimeRequired)
		return resp
	}

	guard := latches.Acquire([][]byte{req.PrimaryKey})
	defer guard.Release()

	reader := NewReader(db)
	lock, err := reader.GetLock(req.PrimaryKey)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if lock != nil {
		if lock.Ts != req.StartVersion {
			resp.Error = keyErrorLocked(req.PrimaryKey, lock)
			return resp
		}
		if !bytes.Equal(lock.Primary, req.PrimaryKey) {
			resp.Error = keyErrorAbort(errTxnHeartbeatPrimaryMismatch)
			return resp
		}
		if isLockExpired(lock, req.CurrentTime) {
			if err := rollbackKey(db, reader, req.PrimaryKey, req.StartVersion); err != nil {
				resp.Error = err
				return resp
			}
			resp.Action = kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExpireRollback
			return resp
		}
		desiredTTL := req.TtlExtension
		if req.CurrentTime > lock.StartTime {
			desiredTTL = req.CurrentTime - lock.StartTime + req.TtlExtension
		}
		if desiredTTL > lock.TTL {
			lock.TTL = desiredTTL
			if err := applyVersionedOps(db, versionedOp{
				cf:      kv.CFLock,
				key:     req.PrimaryKey,
				version: lockColumnTs,
				value:   mvcc.EncodeLock(*lock),
			}); err != nil {
				resp.Error = keyErrorRetryable(err)
				return resp
			}
			resp.Action = kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended
		}
		resp.LockTtl = lock.TTL
		resp.LockExpireTime = lockExpireTime(lock)
		return resp
	}

	write, commitTs, err := reader.GetWriteByStartTs(req.PrimaryKey, req.StartVersion)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if write != nil && write.Kind != kvrpcpb.Mutation_Rollback {
		resp.CommitVersion = commitTs
		return resp
	}
	if err := rollbackKey(db, reader, req.PrimaryKey, req.StartVersion); err != nil {
		resp.Error = err
		return resp
	}
	resp.Action = kvrpcpb.TxnHeartBeatAction_TxnHeartBeatLockNotExistRollback
	return resp
}

func keyExistsAt(reader *Reader, key []byte, readTs uint64) (bool, error) {
	write, _, err := reader.getWriteForRead(key, readTs)
	if err != nil {
		return false, err
	}
	if write == nil {
		return false, nil
	}
	switch write.Kind {
	case kvrpcpb.Mutation_Delete, kvrpcpb.Mutation_Rollback:
		return false, nil
	default:
		return true, nil
	}
}

func planCommitKey(reader *Reader, key []byte, startVersion, commitVersion uint64) ([]versionedOp, *kvrpcpb.KeyError) {
	if len(key) == 0 {
		return nil, keyErrorAbort(errEmptyCommitKey)
	}
	lock, err := reader.GetLock(key)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if lock == nil {
		write, _, err := reader.GetWriteByStartTs(key, startVersion)
		if err != nil {
			return nil, keyErrorRetryable(err)
		}
		if write != nil {
			if write.Kind == kvrpcpb.Mutation_Rollback {
				return nil, keyErrorAbort(errTxnAlreadyRolledBack)
			}
			return nil, nil
		}
		return nil, keyErrorAbort(errLockNotFound)
	}
	if lock.Ts != startVersion {
		return nil, keyErrorLocked(key, lock)
	}
	return planCommitKeyWithLock(reader, key, lock, commitVersion)
}

func planCommitKeyWithLock(reader *Reader, key []byte, lock *mvcc.Lock, commitVersion uint64) ([]versionedOp, *kvrpcpb.KeyError) {
	write, _, err := reader.GetWriteByStartTs(key, lock.Ts)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if write != nil {
		if write.Kind == kvrpcpb.Mutation_Rollback {
			return nil, keyErrorAbort(errTxnAlreadyRolledBack)
		}
		return []versionedOp{{
			cf:      kv.CFLock,
			key:     key,
			version: lockColumnTs,
			meta:    kv.BitDelete,
		}}, nil
	}

	if lock.MinCommitTs > commitVersion {
		return nil, keyErrorCommitTsExpired(key, commitVersion, lock.MinCommitTs)
	}

	entry := mvcc.EncodeWrite(mvcc.Write{Kind: lock.Kind, StartTs: lock.Ts})
	return []versionedOp{
		{cf: kv.CFWrite, key: key, version: commitVersion, value: entry},
		{cf: kv.CFLock, key: key, version: lockColumnTs, meta: kv.BitDelete},
	}, nil
}

func commitKey(db txnstore.Store, reader *Reader, key []byte, lock *mvcc.Lock, commitVersion uint64) *kvrpcpb.KeyError {
	ops, keyErr := planCommitKeyWithLock(reader, key, lock, commitVersion)
	if keyErr != nil {
		return keyErr
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func rollbackKey(db txnstore.Store, reader *Reader, key []byte, startTs uint64) *kvrpcpb.KeyError {
	ops, keyErr := planRollbackKey(reader, key, startTs)
	if keyErr != nil {
		return keyErr
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func planRollbackKey(reader *Reader, key []byte, startTs uint64) ([]versionedOp, *kvrpcpb.KeyError) {
	if len(key) == 0 {
		return nil, keyErrorAbort(errEmptyRollbackKey)
	}
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if write != nil {
		return nil, nil
	}

	lock, err := reader.GetLock(key)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}

	rollback := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Rollback, StartTs: startTs})
	ops := []versionedOp{
		{cf: kv.CFDefault, key: key, version: startTs, meta: kv.BitDelete},
		{cf: kv.CFWrite, key: key, version: startTs, value: rollback},
	}
	if lock != nil && lock.Ts == startTs {
		ops = append(ops, versionedOp{cf: kv.CFLock, key: key, version: lockColumnTs, meta: kv.BitDelete})
	}
	return ops, nil
}

type versionedOp struct {
	cf      kv.ColumnFamily
	key     []byte
	version uint64
	value   []byte
	meta    byte
	expires uint64
}

func applyVersionedOps(db txnstore.Store, ops ...versionedOp) error {
	if len(ops) == 0 {
		return nil
	}
	entries := make([]*kv.Entry, 0, len(ops))
	for _, op := range ops {
		entry := kv.NewInternalEntry(op.cf, op.key, op.version, op.value, op.meta, op.expires)
		entries = append(entries, entry)
	}
	// NoKV's DB regroups these internal entries by commit-pipeline shard before
	// they reach the sharded LSM. Percolator batches at the protocol phase
	// boundary; storage keeps the per-key placement invariant.
	err := db.ApplyInternalEntries(entries)
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
	return err
}

func currentPhysicalTimeMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

func isLockExpired(lock *mvcc.Lock, currentTime uint64) bool {
	if lock == nil {
		return false
	}
	if lock.TTL == 0 || lock.StartTime == 0 || currentTime == 0 {
		return false
	}
	return currentTime >= lock.StartTime && currentTime-lock.StartTime >= lock.TTL
}

func lockExpireTime(lock *mvcc.Lock) uint64 {
	if lock == nil || lock.StartTime == 0 || lock.TTL == 0 {
		return 0
	}
	if ^uint64(0)-lock.StartTime < lock.TTL {
		return ^uint64(0)
	}
	return lock.StartTime + lock.TTL
}
