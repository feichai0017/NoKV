package failpoints

import (
	"errors"
	"sync/atomic"
)

// Mode configures rooted control-plane failure injection hooks. Modes can be
// ORed together to simulate multiple failures in one test.
type Mode uint32

const (
	// None disables all rooted control-plane failpoints.
	None Mode = 0
	// BeforeApplyTenure aborts a rooted coordinator lease mutation
	// before it enters the replicated metadata-root state machine.
	BeforeApplyTenure Mode = 1 << iota
	// BeforeApplyHandover aborts a rooted coordinator handover mutation
	// before it enters the replicated metadata-root state machine.
	BeforeApplyHandover
	// BeforeTenureStorageRead aborts the coordinator's storage-backed
	// succession gate before it reloads a rooted snapshot.
	BeforeTenureStorageRead
	// AfterAppendCommittedBeforeCheckpoint aborts one rooted append after the
	// replicated log commit is observed but before the checkpoint is advanced.
	AfterAppendCommittedBeforeCheckpoint
)

var currentMode atomic.Uint32

var (
	ErrBeforeApplyTenure                    = errors.New("meta/root failpoint: before apply coordinator lease")
	ErrBeforeApplyHandover                  = errors.New("meta/root failpoint: before apply coordinator handover")
	ErrBeforeTenureStorageRead              = errors.New("meta/root failpoint: before coordinator lease storage read")
	ErrAfterAppendCommittedBeforeCheckpoint = errors.New("meta/root failpoint: after append committed before checkpoint")
)

// Set configures the active rooted control-plane failpoint mode. Passing None
// clears all previously configured failpoints.
func Set(mode Mode) {
	currentMode.Store(uint32(mode))
}

// Current returns the active rooted control-plane failpoint mode.
func Current() Mode {
	return Mode(currentMode.Load())
}

func enabled(mode Mode) bool {
	return Current()&mode != 0
}

// InjectBeforeApplyTenure returns the configured injected failure for
// rooted lease apply operations.
func InjectBeforeApplyTenure() error {
	if enabled(BeforeApplyTenure) {
		return ErrBeforeApplyTenure
	}
	return nil
}

// InjectBeforeApplyHandover returns the configured injected failure
// for rooted handover apply operations.
func InjectBeforeApplyHandover() error {
	if enabled(BeforeApplyHandover) {
		return ErrBeforeApplyHandover
	}
	return nil
}

// InjectBeforeTenureStorageRead returns the configured injected
// failure for storage-backed coordinator lease view refreshes.
func InjectBeforeTenureStorageRead() error {
	if enabled(BeforeTenureStorageRead) {
		return ErrBeforeTenureStorageRead
	}
	return nil
}

// InjectAfterAppendCommittedBeforeCheckpoint returns the configured injected
// failure after committed rooted events reach the durable log but before the
// compact checkpoint advances.
func InjectAfterAppendCommittedBeforeCheckpoint() error {
	if enabled(AfterAppendCommittedBeforeCheckpoint) {
		return ErrAfterAppendCommittedBeforeCheckpoint
	}
	return nil
}
