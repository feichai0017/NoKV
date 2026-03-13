package utils

import (
	"github.com/pkg/errors"
)

// ErrKeyNotFound indicates a missing key.
var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate = errors.New("Do truncate")
	ErrStop     = errors.New("Stop")

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig      = errors.New("Txn is too big to fit into one request")
	ErrDeleteVlogFile = errors.New("Delete vlog file")

	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
	// ErrNilValue is returned when a write API receives a nil value payload.
	ErrNilValue = errors.New("Value cannot be nil")

	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New(
		"Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")

	// ErrBlockedWrites is returned if the user called DropAll. During the process of dropping all
	// data
	ErrBlockedWrites = errors.New("Writes are blocked, possibly due to DropAll or Close")

	// ErrDBClosed is returned when a get operation is performed after closing the DB.
	ErrDBClosed = errors.New("DB Closed")

	// ErrHotKeyWriteThrottle indicates that a key exceeded the configured write hot-key limit.
	ErrHotKeyWriteThrottle = errors.New("hot key write throttled")
)

// Panic panics when err is non-nil.
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// CondPanic panics with err when condition is true.
//
// Usage guidance:
//   - Prefer this helper when the error object already exists (for example, an
//     `err` returned from another call).
//   - If the panic message needs dynamic formatting (fmt.Errorf / string
//     concatenation), prefer CondPanicFunc to avoid constructing that error on
//     the non-panic path.
//
// Note:
//   - Passing err=nil is valid and mirrors Panic(nil) behavior (i.e. no panic).
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}

// CondPanicFunc panics when condition is true, creating the error lazily via errFn.
//
// This is the preferred helper for hot paths where panic diagnostics require
// dynamic formatting. errFn is only invoked when condition is true, so normal
// execution avoids fmt/error allocations.
//
// Usage guidance:
//   - Use CondPanic when you already have an error value.
//   - Use CondPanicFunc when the error would otherwise be built eagerly (for
//     example fmt.Errorf with runtime values).
func CondPanicFunc(condition bool, errFn func() error) {
	if condition {
		Panic(errFn())
	}
}
