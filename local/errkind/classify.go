// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package errkind

import (
	stderrors "errors"

	enginekv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/lsm/flush"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/utils"
)

// Classify maps local DB and embedded-engine errors to the stable
// cross-boundary error taxonomy. Engine and utils packages intentionally keep
// their local sentinels and do not import the root errors package; public DB,
// runtime, or RPC boundaries should classify them here before deciding retry,
// abort, or corruption handling. Package-private control-flow sentinels stay
// Unknown and must not escape as public API decisions.
func Classify(err error) nokverrors.Kind {
	if err == nil {
		return nokverrors.KindUnknown
	}
	if kind := nokverrors.KindOf(err); kind != nokverrors.KindUnknown {
		return kind
	}
	switch {
	case stderrors.Is(err, utils.ErrKeyNotFound):
		return nokverrors.KindNotFound
	case stderrors.Is(err, utils.ErrEmptyKey),
		stderrors.Is(err, utils.ErrNilValue),
		stderrors.Is(err, utils.ErrInvalidRequest),
		stderrors.Is(err, lsm.ErrLSMNilOptions),
		stderrors.Is(err, lsm.ErrLSMNilWALManager),
		stderrors.Is(err, lsm.ErrLSMNilClonedOptions),
		stderrors.Is(err, vfs.ErrRenameNoReplaceUnsupported):
		return nokverrors.KindInvalidArgument
	case stderrors.Is(err, utils.ErrTxnTooBig):
		return nokverrors.KindResourceExhausted
	case stderrors.Is(err, utils.ErrBlockedWrites),
		stderrors.Is(err, utils.ErrHotKeyWriteThrottle),
		stderrors.Is(err, lsm.ErrFillTables),
		stderrors.Is(err, wal.ErrSegmentRetained),
		stderrors.Is(err, wal.ErrWALBackpressure):
		return nokverrors.KindRetryable
	case stderrors.Is(err, utils.ErrDBClosed),
		stderrors.Is(err, flush.ErrClosed),
		stderrors.Is(err, lsm.ErrLSMClosed):
		return nokverrors.KindAborted
	case stderrors.Is(err, enginekv.ErrChecksumMismatch),
		stderrors.Is(err, enginekv.ErrBadChecksum),
		stderrors.Is(err, enginekv.ErrPartialEntry),
		stderrors.Is(err, wal.ErrPartialRecord),
		stderrors.Is(err, wal.ErrEmptyRecord):
		return nokverrors.KindCorruption
	case stderrors.Is(err, lsm.ErrMemtableNotInitialized),
		stderrors.Is(err, flush.ErrNil),
		stderrors.Is(err, lsm.ErrFlushNilMemtable),
		stderrors.Is(err, lsm.ErrLSMNil):
		return nokverrors.KindProtocolViolation
	default:
		return nokverrors.KindUnknown
	}
}
