package percolator

import (
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator/mvcc"
)

var (
	errEmptyMutationKey            = errors.New("percolator: empty key in mutation")
	errEmptyCommitKey              = errors.New("percolator: empty key in commit")
	errEmptyRollbackKey            = errors.New("percolator: empty key in rollback")
	errUnsupportedMutationOp       = errors.New("percolator: unsupported mutation op")
	errCommitVersionNotAfterStart  = errors.New("percolator: commit version must be greater than start version")
	errTxnAlreadyRolledBack        = errors.New("percolator: transaction already rolled back")
	errLockNotFound                = errors.New("percolator: lock not found")
	errTxnHeartbeatPrimaryRequired = errors.New("percolator: heartbeat primary key is required")
	errTxnHeartbeatStartRequired   = errors.New("percolator: heartbeat start version is required")
	errTxnHeartbeatTTLRequired     = errors.New("percolator: heartbeat ttl extension is required")
	errTxnHeartbeatTimeRequired    = errors.New("percolator: heartbeat current time is required")
	errTxnHeartbeatPrimaryMismatch = errors.New("percolator: heartbeat primary key does not match lock primary")
)

func keyErrorLocked(key []byte, lock *mvcc.Lock) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, key),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		},
	}
}

func keyErrorWriteConflict(key, primary []byte, conflictTs, startTs, currentTs uint64) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{
		WriteConflict: &kvrpcpb.WriteConflict{
			Key:        kv.SafeCopy(nil, key),
			Primary:    kv.SafeCopy(nil, primary),
			ConflictTs: conflictTs,
			StartTs:    startTs,
			CommitTs:   currentTs,
		},
	}
}

func keyErrorRetryable(err error) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Retryable: err.Error()}
}

func keyErrorAlreadyExists(key []byte) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{AlreadyExists: &kvrpcpb.KeyAlreadyExists{Key: kv.SafeCopy(nil, key)}}
}

func keyErrorAbort(err error) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Abort: err.Error()}
}

func keyErrorAbortf(cause error, format string, args ...any) *kvrpcpb.KeyError {
	return keyErrorAbort(fmt.Errorf("%w: %s", cause, fmt.Sprintf(format, args...)))
}

func keyErrorCommitTsExpired(key []byte, commitTs, minCommitTs uint64) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{
		CommitTsExpired: &kvrpcpb.CommitTsExpired{
			Key:         kv.SafeCopy(nil, key),
			CommitTs:    commitTs,
			MinCommitTs: minCommitTs,
		},
	}
}
