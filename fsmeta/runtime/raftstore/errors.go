package raftstore

import (
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	errCoordinatorAddrRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: coordinator addr is required")
	errSessionCleanupLimitExceeded = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: session cleanup limit exceeds maximum")
	errMountCacheNotConfigured     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: mount cache is not configured")
	errRootPublisherNotConfigured  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: root publisher is not configured")
	errStoreListerRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: store lister is required")
	errWatchRouterRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: router is required")
	errKVClientRequired            = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: raftstore kv client required")
	errTSOClientRequired           = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: tso client required")
	errTimestampCountRequired      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/raftstore: timestamp count must be > 0")
	errRootEventNotAccepted        = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: root event was not accepted")
	errNilTSOResponse              = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: nil tso response")
	errZeroTSOTimestamp            = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/raftstore: zero tso timestamp")
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
