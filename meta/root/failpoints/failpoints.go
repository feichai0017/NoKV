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
	// BeforeApplyCoordinatorLease aborts a rooted coordinator lease mutation
	// before it enters the replicated metadata-root state machine.
	BeforeApplyCoordinatorLease Mode = 1 << iota
	// BeforeApplyCoordinatorClosure aborts a rooted coordinator closure mutation
	// before it enters the replicated metadata-root state machine.
	BeforeApplyCoordinatorClosure
	// BeforeCoordinatorLeaseStorageRead aborts the coordinator's storage-backed
	// preAction gate before it reloads a rooted snapshot.
	BeforeCoordinatorLeaseStorageRead
	// AfterAppendCommittedBeforeCheckpoint aborts one rooted append after the
	// replicated log commit is observed but before the checkpoint is advanced.
	AfterAppendCommittedBeforeCheckpoint
)

var currentMode atomic.Uint32

var (
	ErrBeforeApplyCoordinatorLease          = errors.New("meta/root failpoint: before apply coordinator lease")
	ErrBeforeApplyCoordinatorClosure        = errors.New("meta/root failpoint: before apply coordinator closure")
	ErrBeforeCoordinatorLeaseStorageRead    = errors.New("meta/root failpoint: before coordinator lease storage read")
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

// InjectBeforeApplyCoordinatorLease returns the configured injected failure for
// rooted lease apply operations.
func InjectBeforeApplyCoordinatorLease() error {
	if enabled(BeforeApplyCoordinatorLease) {
		return ErrBeforeApplyCoordinatorLease
	}
	return nil
}

// InjectBeforeApplyCoordinatorClosure returns the configured injected failure
// for rooted closure apply operations.
func InjectBeforeApplyCoordinatorClosure() error {
	if enabled(BeforeApplyCoordinatorClosure) {
		return ErrBeforeApplyCoordinatorClosure
	}
	return nil
}

// InjectBeforeCoordinatorLeaseStorageRead returns the configured injected
// failure for storage-backed coordinator lease view refreshes.
func InjectBeforeCoordinatorLeaseStorageRead() error {
	if enabled(BeforeCoordinatorLeaseStorageRead) {
		return ErrBeforeCoordinatorLeaseStorageRead
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
