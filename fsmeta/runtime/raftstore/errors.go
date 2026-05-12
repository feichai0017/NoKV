package raftstore

import (
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	errCoordinatorAddrRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: coordinator addr is required")
	errSessionCleanupLimitExceeded = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: session cleanup limit exceeds maximum")
	errLockTTLInvalid              = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: lock ttl must be non-negative")
	errPerasCommitterInvalid       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: peras committer config is invalid")
	errPerasCommitterClosed        = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/runtime/raftstore: peras committer is closed")
	errMountCacheNotConfigured     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: mount cache is not configured")
	errRootPublisherNotConfigured  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: root publisher is not configured")
	errStoreListerRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: store lister is required")
	errWatchRouterRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: router is required")
	errKVClientRequired            = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: raftstore kv client required")
	errTSOClientRequired           = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: tso client required")
	errIDAllocatorClientRequired   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: id allocator client required")
	errTimestampCountRequired      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: timestamp count must be > 0")
	errInodeAllocBatchRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: inode allocation batch must be > 0")
	errRootEventNotAccepted        = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: root event was not accepted")
	errNilTSOResponse              = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: nil tso response")
	errZeroTSOTimestamp            = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: zero tso timestamp")
	errNilAllocIDResponse          = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: nil alloc id response")
	errEmptyAllocIDResponse        = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: empty alloc id response")
	errNoUsableInodeID             = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: no usable inode id in allocation batch")
)

func errTSOCountMismatch(got, requested uint64) error {
	return nokverrors.New(
		nokverrors.KindProtocolViolation,
		fmt.Sprintf("fsmeta/runtime/raftstore: tso count=%d requested=%d", got, requested),
	)
}

func runnerKeyError(op string, keyErr *kvrpcpb.KeyError) error {
	if keyErr == nil {
		return nil
	}
	return fmt.Errorf("fsmeta/runtime/raftstore: %s: %w", op, nokverrors.NewTxnKeyError(keyErr))
}

func (c *RemotePerasCommitter) recordErrorf(format string, args ...any) error {
	return c.recordError(fmt.Errorf(format, args...))
}

func isPerasAdmissionTerminalError(err error) bool {
	return errors.Is(err, fsmeta.ErrExists) ||
		errors.Is(err, fsmeta.ErrNotFound) ||
		errors.Is(err, fsmeta.ErrInvalidRequest) ||
		errors.Is(err, fsmeta.ErrInvalidValue)
}

func (c *RemotePerasCommitter) recordError(err error) error {
	if c == nil || err == nil {
		return err
	}
	c.errorTotal.Add(1)
	c.statsMu.Lock()
	c.lastError = err.Error()
	c.statsMu.Unlock()
	return err
}
