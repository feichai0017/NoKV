package percolator

import (
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator/latch"
)

// Prewrite applies mutation prewrites for a single region transaction.
func Prewrite(db NoKV.MVCCStore, latches *latch.Manager, req *pb.PrewriteRequest) []*pb.KeyError {
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
	var errs []*pb.KeyError
	for _, mut := range req.Mutations {
		if mut == nil {
			continue
		}
		if err := prewriteMutation(db, reader, req, mut); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func prewriteMutation(db NoKV.MVCCStore, reader *Reader, req *pb.PrewriteRequest, mut *pb.Mutation) *pb.KeyError {
	key := mut.GetKey()
	if len(key) == 0 {
		return keyErrorAbort("empty key in mutation")
	}
	lock, err := reader.GetLock(key)
	if err != nil {
		return keyErrorRetryable(err)
	}
	if lock != nil && lock.Ts != req.StartVersion {
		return keyErrorLocked(key, lock)
	}
	if write, commitTs, err := reader.MostRecentWrite(key); err != nil {
		return keyErrorRetryable(err)
	} else if write != nil && commitTs >= req.StartVersion {
		return keyErrorWriteConflict(key, req.PrimaryLock, commitTs, write.StartTs, req.StartVersion)
	}
	ops := make([]versionedOp, 0, 3)
	switch mut.Op {
	case pb.Mutation_Put:
		ops = append(ops,
			versionedOp{cf: kv.CFDefault, key: key, version: req.StartVersion, meta: kv.BitDelete},
			versionedOp{cf: kv.CFDefault, key: key, version: req.StartVersion, value: mut.Value, expires: mut.GetExpiresAt()},
		)
	case pb.Mutation_Delete, pb.Mutation_Lock:
		ops = append(ops,
			versionedOp{cf: kv.CFDefault, key: key, version: req.StartVersion, meta: kv.BitDelete},
		)
	default:
		return keyErrorAbort(fmt.Sprintf("unsupported mutation op %v", mut.Op))
	}
	newLock := Lock{
		Primary:     kv.SafeCopy(nil, req.PrimaryLock),
		Ts:          req.StartVersion,
		TTL:         req.LockTtl,
		Kind:        mut.Op,
		MinCommitTs: req.MinCommitTs,
	}
	encoded := EncodeLock(newLock)
	ops = append(ops, versionedOp{cf: kv.CFLock, key: key, version: lockColumnTs, value: encoded})
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func validateCommitVersion(StartVersion uint64, CommitVersion uint64) *pb.KeyError {
	if CommitVersion < StartVersion {
		return keyErrorAbort("commit version is earlier than start version")
	}
	return nil
}

// Commit finalises earlier prewrites by removing locks and writing commit
// records. A non-nil KeyError is returned when commit should abort.
func Commit(db NoKV.MVCCStore, latches *latch.Manager, req *pb.CommitRequest) *pb.KeyError {
	if req == nil {
		return nil
	}
	if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
		return err
	}
	guard := latches.Acquire(req.Keys)
	defer guard.Release()

	reader := NewReader(db)
	for _, key := range req.Keys {
		if len(key) == 0 {
			return keyErrorAbort("empty key in commit")
		}
		lock, err := reader.GetLock(key)
		if err != nil {
			return keyErrorRetryable(err)
		}
		if lock == nil {
			write, _, err := reader.GetWriteByStartTs(key, req.StartVersion)
			if err != nil {
				return keyErrorRetryable(err)
			}
			if write != nil {
				if write.Kind == pb.Mutation_Rollback {
					return keyErrorAbort("transaction already rolled back")
				}
				continue
			}
			return keyErrorAbort("lock not found")
		}
		if lock.Ts != req.StartVersion {
			return keyErrorLocked(key, lock)
		}
		if err := commitKey(db, reader, key, lock, req.CommitVersion); err != nil {
			return err
		}
	}
	return nil
}

// BatchRollback rolls back the provided keys for the given start version.
func BatchRollback(db NoKV.MVCCStore, latches *latch.Manager, req *pb.BatchRollbackRequest) *pb.KeyError {
	if req == nil {
		return nil
	}
	guard := latches.Acquire(req.Keys)
	defer guard.Release()
	reader := NewReader(db)
	for _, key := range req.Keys {
		if len(key) == 0 {
			return keyErrorAbort("empty key in rollback")
		}
		if err := rollbackKey(db, reader, key, req.StartVersion); err != nil {
			return err
		}
	}
	return nil
}

// ResolveLock resolves locks for the given transaction. commitVersion == 0
// performs a rollback; otherwise the keys are committed.
func ResolveLock(db NoKV.MVCCStore, latches *latch.Manager, req *pb.ResolveLockRequest) (uint64, *pb.KeyError) {
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
	for _, key := range req.Keys {
		if len(key) == 0 {
			continue
		}
		lock, err := reader.GetLock(key)
		if err != nil {
			return resolved, keyErrorRetryable(err)
		}
		if lock == nil || lock.Ts != req.StartVersion {
			continue
		}
		if req.CommitVersion == 0 {
			if err := rollbackKey(db, reader, key, req.StartVersion); err != nil {
				return resolved, err
			}
		} else {
			if err := commitKey(db, reader, key, lock, req.CommitVersion); err != nil {
				return resolved, err
			}
		}
		resolved++
	}
	return resolved, nil
}

// CheckTxnStatus inspects the primary lock state and optionally rolls back
// expired transactions.
func CheckTxnStatus(db NoKV.MVCCStore, latches *latch.Manager, req *pb.CheckTxnStatusRequest) *pb.CheckTxnStatusResponse {
	resp := &pb.CheckTxnStatusResponse{}
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
		if isLockExpired(lock, req.CurrentTs) {
			if err := rollbackKey(db, reader, req.PrimaryKey, req.LockTs); err != nil {
				resp.Error = err
				return resp
			}
			resp.Action = pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback
			return resp
		}
		if req.CallerStartTs > 0 && lock.MinCommitTs < req.CallerStartTs+1 {
			lock.MinCommitTs = req.CallerStartTs + 1
			if err := applyVersionedOps(db, versionedOp{
				cf:      kv.CFLock,
				key:     req.PrimaryKey,
				version: lockColumnTs,
				value:   EncodeLock(*lock),
			}); err != nil {
				resp.Error = keyErrorRetryable(err)
				return resp
			}
			resp.Action = pb.CheckTxnStatusAction_CheckTxnStatusMinCommitTsPushed
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
		if write.Kind == pb.Mutation_Rollback {
			resp.Action = pb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback
			return resp
		}
		resp.CommitVersion = commitTs
		return resp
	}

	if req.RollbackIfNotExist {
		if err := rollbackKey(db, reader, req.PrimaryKey, req.LockTs); err != nil {
			resp.Error = err
		} else {
			resp.Action = pb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback
		}
	}
	return resp
}

// keyErrorLocked builds a KeyError for a locked key.
func keyErrorLocked(key []byte, lock *Lock) *pb.KeyError {
	return &pb.KeyError{
		Locked: &pb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, key),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		},
	}
}

func keyErrorWriteConflict(key, primary []byte, conflictTs, startTs, currentTs uint64) *pb.KeyError {
	return &pb.KeyError{
		WriteConflict: &pb.WriteConflict{
			Key:        kv.SafeCopy(nil, key),
			Primary:    kv.SafeCopy(nil, primary),
			ConflictTs: conflictTs,
			StartTs:    startTs,
			CommitTs:   currentTs,
		},
	}
}

func keyErrorRetryable(err error) *pb.KeyError {
	return &pb.KeyError{Retryable: err.Error()}
}

func keyErrorAbort(msg string) *pb.KeyError {
	return &pb.KeyError{Abort: msg}
}

func keyErrorCommitTsExpired(key []byte, commitTs, minCommitTs uint64) *pb.KeyError {
	return &pb.KeyError{
		CommitTsExpired: &pb.CommitTsExpired{
			Key:         kv.SafeCopy(nil, key),
			CommitTs:    commitTs,
			MinCommitTs: minCommitTs,
		},
	}
}

func commitKey(db NoKV.MVCCStore, reader *Reader, key []byte, lock *Lock, commitVersion uint64) *pb.KeyError {
	if lock.MinCommitTs > commitVersion {
		return keyErrorCommitTsExpired(key, commitVersion, lock.MinCommitTs)
	}
	write, commitTs, err := reader.GetWriteByStartTs(key, lock.Ts)
	if err != nil {
		return keyErrorRetryable(err)
	}
	if write != nil {
		if write.Kind == pb.Mutation_Rollback {
			return keyErrorAbort("transaction already rolled back")
		}
		if commitTs != commitVersion {
			// Already committed with a different commit version; treat as success.
			if err := applyVersionedOps(db, versionedOp{
				cf:      kv.CFLock,
				key:     key,
				version: lockColumnTs,
				meta:    kv.BitDelete,
			}); err != nil {
				return keyErrorRetryable(err)
			}
			return nil
		}
		return nil
	}

	entry := EncodeWrite(Write{Kind: lock.Kind, StartTs: lock.Ts})
	if err := applyVersionedOps(db,
		versionedOp{cf: kv.CFWrite, key: key, version: commitVersion, value: entry},
		versionedOp{cf: kv.CFLock, key: key, version: lockColumnTs, meta: kv.BitDelete},
	); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func rollbackKey(db NoKV.MVCCStore, reader *Reader, key []byte, startTs uint64) *pb.KeyError {
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	if err != nil {
		return keyErrorRetryable(err)
	}
	if write != nil {
		return nil
	}

	lock, err := reader.GetLock(key)
	if err != nil {
		return keyErrorRetryable(err)
	}

	rollback := EncodeWrite(Write{Kind: pb.Mutation_Rollback, StartTs: startTs})
	ops := []versionedOp{
		{cf: kv.CFDefault, key: key, version: startTs, meta: kv.BitDelete},
		{cf: kv.CFWrite, key: key, version: startTs, value: rollback},
	}
	if lock != nil && lock.Ts == startTs {
		ops = append(ops, versionedOp{cf: kv.CFLock, key: key, version: lockColumnTs, meta: kv.BitDelete})
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

type versionedOp struct {
	cf      kv.ColumnFamily
	key     []byte
	version uint64
	value   []byte
	meta    byte
	expires uint64
}

func applyVersionedOps(db NoKV.MVCCStore, ops ...versionedOp) error {
	if len(ops) == 0 {
		return nil
	}
	entries := make([]*kv.Entry, 0, len(ops))
	for _, op := range ops {
		entry := kv.NewInternalEntry(op.cf, op.key, op.version, op.value, op.meta, op.expires)
		entries = append(entries, entry)
	}
	err := db.ApplyInternalEntries(entries)
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
	return err
}

func isLockExpired(lock *Lock, currentTs uint64) bool {
	if lock == nil {
		return false
	}
	if lock.TTL == 0 {
		return false
	}
	return currentTs >= lock.Ts+lock.TTL
}
