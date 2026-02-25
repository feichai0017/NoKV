package utils

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

// ErrKeyNotFound indicates a missing key.
var (
	// ErrValueLogSize is returned when opt.ValueLogFileSize option is not within the valid
	// range.
	ErrValueLogSize = errors.New("Invalid ValueLogFileSize, must be in range [1MB, 2GB)")

	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrReWriteFailure reWrite failure
	ErrReWriteFailure = errors.New("reWrite failure")
	// ErrBadMagic bad magic
	ErrBadMagic = errors.New("bad magic")
	// ErrBadChecksum bad check sum
	ErrBadChecksum = errors.New("bad check sum")
	// ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate = errors.New("Do truncate")
	ErrStop     = errors.New("Stop")

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig      = errors.New("Txn is too big to fit into one request")
	ErrDeleteVlogFile = errors.New("Delete vlog file")

	// ErrConflict is returned when a transaction conflicts with another transaction. This can
	// happen if the read rows had been updated concurrently by another transaction.
	ErrConflict = errors.New("Transaction Conflict. Please retry")

	// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
	ErrReadOnlyTxn = errors.New("No sets or deletes are allowed in a read-only transaction")

	// ErrDiscardedTxn is returned if a previously discarded transaction is re-used.
	ErrDiscardedTxn = errors.New("This transaction has been discarded. Create a new one")

	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")

	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New(
		"Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")

	// ErrManagedTxn is returned if the user tries to use an API which isn't
	// allowed due to external management of transactions, when using ManagedDB.
	ErrManagedTxn = errors.New(
		"Invalid API request. Not allowed to perform this action using ManagedDB")

	// ErrTruncateNeeded is returned when the value log gets corrupt, and requires truncation of
	// corrupt data to allow to run properly.
	ErrTruncateNeeded = errors.New(
		"Log truncate required to run DB. This might result in data loss")

	// ErrBlockedWrites is returned if the user called DropAll. During the process of dropping all
	// data
	ErrBlockedWrites = errors.New("Writes are blocked, possibly due to DropAll or Close")

	// ErrDBClosed is returned when a get operation is performed after closing the DB.
	ErrDBClosed = errors.New("DB Closed")

	ErrFillTables = errors.New("fill tables")

	// ErrHotKeyWriteThrottle indicates that a key exceeded the configured write hot-key limit.
	ErrHotKeyWriteThrottle = errors.New("hot key write throttled")

	// ErrMissingManifestOrWAL indicates WAL and manifest must be provided together for raftstore durability.
	ErrMissingManifestOrWAL = errors.New("raftstore: WAL and manifest must both be provided")

	// ErrPartialRecord indicates that a WAL record ended prematurely (typically due to EOF/corruption).
	ErrPartialRecord = errors.New("wal: partial record")
	// ErrEmptyRecord indicates that a WAL record header advertised zero payload length.
	ErrEmptyRecord = errors.New("wal: empty record")
)

// Panic panics when err is non-nil.
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// Panic2 _
func Panic2(_ any, err error) {
	Panic(err)
}

// Err err
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

// WrapErr logs and returns err with caller location metadata.
func WrapErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}

// WarpErr is kept for backward compatibility.
// Deprecated: use WrapErr.
func WarpErr(format string, err error) error {
	return WrapErr(format, err)
}
func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		file = strings.TrimPrefix(file, gopath)
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// CondPanic e
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}

// CondPanicFunc defers error construction until the condition is true, avoiding
// allocations on the hot path.
func CondPanicFunc(condition bool, errFn func() error) {
	if condition {
		Panic(errFn())
	}
}

func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", Wrap(err, ""))
	}
}

var debugMode = false

func Wrap(err error, msg string) error {
	if !debugMode {
		if err == nil {
			return nil
		}
		return fmt.Errorf("%s err: %+v", msg, err)
	}
	return errors.Wrap(err, msg)
}

// Wrapf is Wrap with extra info.
func Wrapf(err error, format string, args ...any) error {
	if !debugMode {
		if err == nil {
			return nil
		}
		return fmt.Errorf(format+" error: %+v", append(args, err)...)
	}
	return errors.Wrapf(err, format, args...)
}
