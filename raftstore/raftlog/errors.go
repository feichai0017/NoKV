package raftlog

import (
	"errors"
	"fmt"
)

var (
	// errStopPointerValidation stops WAL pointer validation once the current pointer was confirmed.
	errStopPointerValidation     = errors.New("raftstore: stop pointer validation")
	errStorageDirRequired        = errors.New("raftstore: storage dir required")
	errWALStorageRequiresGroupID = errors.New("raftstore: wal storage requires group id")
	errWALStorageRequiresManager = errors.New("raftstore: wal storage requires WAL manager")
)

func errDecodeSnapshot(err error) error  { return fmt.Errorf("raftstore: decode snapshot: %w", err) }
func errReadLogLength(err error) error   { return fmt.Errorf("raftstore: read log length: %w", err) }
func errReadLogPayload(err error) error  { return fmt.Errorf("raftstore: read log payload: %w", err) }
func errDecodeEntry(err error) error     { return fmt.Errorf("raftstore: decode entry: %w", err) }
func errDecodeHardState(err error) error { return fmt.Errorf("raftstore: decode hard state: %w", err) }

func errExpectedSingleEntryRecord(n int) error {
	return fmt.Errorf("raftstore: expected single entry record, got %d", n)
}

func errExpectedSingleSnapshotRecord(n int) error {
	return fmt.Errorf("raftstore: expected single snapshot record, got %d", n)
}

func errExpectedSingleHardStateRecord(n int) error {
	return fmt.Errorf("raftstore: expected single hard state record, got %d", n)
}
