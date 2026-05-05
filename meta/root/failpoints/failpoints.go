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
	// BeforeApplyGrantIssue aborts a rooted coordinator grant mutation
	// before it enters the replicated metadata-root state machine.
	BeforeApplyGrantIssue Mode = 1 << iota
	// BeforeApplyGrantRetirement aborts a rooted coordinator grant retirement mutation
	// before it enters the replicated metadata-root state machine.
	BeforeApplyGrantRetirement
	// BeforeGrantStorageRead aborts the coordinator's storage-backed
	// eunomia gate before it reloads a rooted snapshot.
	BeforeGrantStorageRead
	// AfterAppendCommittedBeforeCheckpoint aborts one rooted append after the
	// replicated log commit is observed but before the checkpoint is advanced.
	AfterAppendCommittedBeforeCheckpoint
)

var currentMode atomic.Uint32

var (
	ErrBeforeApplyGrantIssue                = errors.New("meta/root failpoint: before apply coordinator grant")
	ErrBeforeApplyGrantRetirement           = errors.New("meta/root failpoint: before apply coordinator grant retirement")
	ErrBeforeGrantStorageRead               = errors.New("meta/root failpoint: before coordinator grant storage read")
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

// InjectBeforeApplyGrantIssue returns the configured injected failure for
// rooted grant issue apply operations.
func InjectBeforeApplyGrantIssue() error {
	if enabled(BeforeApplyGrantIssue) {
		return ErrBeforeApplyGrantIssue
	}
	return nil
}

// InjectBeforeApplyGrantRetirement returns the configured injected failure
// for rooted grant retirement apply operations.
func InjectBeforeApplyGrantRetirement() error {
	if enabled(BeforeApplyGrantRetirement) {
		return ErrBeforeApplyGrantRetirement
	}
	return nil
}

// InjectBeforeGrantStorageRead returns the configured injected
// failure for storage-backed coordinator grant view refreshes.
func InjectBeforeGrantStorageRead() error {
	if enabled(BeforeGrantStorageRead) {
		return ErrBeforeGrantStorageRead
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
