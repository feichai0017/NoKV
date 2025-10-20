package mvcc

import (
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/mvcc/latch"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
)

// Prewrite applies mutation prewrites for a single region transaction.
func Prewrite(db *NoKV.DB, latches *latch.Manager, req *pb.PrewriteRequest) []*pb.KeyError {
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

func prewriteMutation(db *NoKV.DB, reader *Reader, req *pb.PrewriteRequest, mut *pb.Mutation) *pb.KeyError {
	key := mut.GetKey()
	if len(key) == 0 {
		return keyErrorAbort(fmt.Sprintf("empty key in mutation"))
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
	switch mut.Op {
	case pb.Mutation_Put:
		if err := db.DeleteVersionedEntry(utils.CFDefault, key, req.StartVersion); err != nil && err != utils.ErrKeyNotFound {
			return keyErrorRetryable(err)
		}
		if err := db.SetVersionedEntry(utils.CFDefault, key, req.StartVersion, mut.Value, 0); err != nil {
			return keyErrorRetryable(err)
		}
	case pb.Mutation_Delete, pb.Mutation_Lock:
		if err := db.DeleteVersionedEntry(utils.CFDefault, key, req.StartVersion); err != nil && err != utils.ErrKeyNotFound {
			return keyErrorRetryable(err)
		}
	default:
		return keyErrorAbort(fmt.Sprintf("unsupported mutation op %v", mut.Op))
	}
	newLock := Lock{
		Primary:     utils.SafeCopy(nil, req.PrimaryLock),
		Ts:          req.StartVersion,
		TTL:         req.LockTtl,
		Kind:        mut.Op,
		MinCommitTs: req.MinCommitTs,
	}
	encoded := EncodeLock(newLock)
	if err := db.SetVersionedEntry(utils.CFLock, key, lockColumnTs, encoded, 0); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

// Commit finalises earlier prewrites by removing locks and writing commit
// records. A non-nil KeyError is returned when commit should abort.
func Commit(db *NoKV.DB, latches *latch.Manager, req *pb.CommitRequest) *pb.KeyError {
	if req == nil {
		return nil
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
func BatchRollback(db *NoKV.DB, latches *latch.Manager, req *pb.BatchRollbackRequest) *pb.KeyError {
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
func ResolveLock(db *NoKV.DB, latches *latch.Manager, req *pb.ResolveLockRequest) (uint64, *pb.KeyError) {
	if req == nil {
		return 0, nil
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
func CheckTxnStatus(db *NoKV.DB, latches *latch.Manager, req *pb.CheckTxnStatusRequest) *pb.CheckTxnStatusResponse {
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
			if err := db.SetVersionedEntry(utils.CFLock, req.PrimaryKey, lockColumnTs, EncodeLock(*lock), 0); err != nil {
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

func keyErrorLocked(key []byte, lock *Lock) *pb.KeyError {
	return &pb.KeyError{
		Locked: &pb.Locked{
			PrimaryLock: lock.Primary,
			Key:         utils.SafeCopy(nil, key),
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
			Key:        utils.SafeCopy(nil, key),
			Primary:    utils.SafeCopy(nil, primary),
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

func commitKey(db *NoKV.DB, reader *Reader, key []byte, lock *Lock, commitVersion uint64) *pb.KeyError {
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
			if err := db.DeleteVersionedEntry(utils.CFLock, key, lockColumnTs); err != nil && err != utils.ErrKeyNotFound {
				return keyErrorRetryable(err)
			}
			return nil
		}
		return nil
	}

	entry := EncodeWrite(Write{Kind: lock.Kind, StartTs: lock.Ts})
	if err := db.SetVersionedEntry(utils.CFWrite, key, commitVersion, entry, 0); err != nil {
		return keyErrorRetryable(err)
	}
	if err := db.DeleteVersionedEntry(utils.CFLock, key, lockColumnTs); err != nil && err != utils.ErrKeyNotFound {
		return keyErrorRetryable(err)
	}
	return nil
}

func rollbackKey(db *NoKV.DB, reader *Reader, key []byte, startTs uint64) *pb.KeyError {
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	if err != nil {
		return keyErrorRetryable(err)
	}
	if write != nil {
		if write.Kind == pb.Mutation_Rollback {
			return nil
		}
		return nil
	}
	if err := db.DeleteVersionedEntry(utils.CFLock, key, lockColumnTs); err != nil && err != utils.ErrKeyNotFound {
		return keyErrorRetryable(err)
	}
	if err := db.DeleteVersionedEntry(utils.CFDefault, key, startTs); err != nil && err != utils.ErrKeyNotFound {
		return keyErrorRetryable(err)
	}
	rollback := EncodeWrite(Write{Kind: pb.Mutation_Rollback, StartTs: startTs})
	if err := db.SetVersionedEntry(utils.CFWrite, key, startTs, rollback, 0); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
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
