package failpoints

import "sync/atomic"

// Mode configures different failure injection hooks for raftstore runtime
// processing. Modes can be ORed together to simulate multiple failure points.
type Mode uint32

const (
	// None disables failure injection.
	None Mode = 0
	// BeforeStorage injects an error before Ready data is written to persistent
	// storage, matching earlier unit test behaviour.
	BeforeStorage Mode = 1 << iota
	// SkipLocalMeta simulates a crash after WAL append but before local raft WAL
	// pointers advance, allowing recovery tests to replay from WAL.
	SkipLocalMeta
	// AfterSnapshotApplyBeforePublish simulates a crash after a snapshot has
	// been applied to local durable state but before the store publishes the new
	// peer/runtime into its router and region catalog.
	AfterSnapshotApplyBeforePublish
	// AfterInitModePreparing simulates a crash after migration init has written
	// MODE=preparing but before any local catalog or snapshot state is published.
	AfterInitModePreparing
	// AfterInitCatalogPersist simulates a crash after migration init has written
	// local region catalog state but before seed snapshot export and seeded mode.
	AfterInitCatalogPersist
	// AfterInitSeedSnapshot simulates a crash after the seed logical snapshot has
	// been exported but before raft seed state and MODE=seeded are finalized.
	AfterInitSeedSnapshot
)

var currentMode atomic.Uint32

// Set configures the active Ready failure injection mode. Passing None clears
// any previously configured failpoint.
func Set(mode Mode) {
	currentMode.Store(uint32(mode))
}

// Mode returns the currently active failure mode.
func Current() Mode {
	return Mode(currentMode.Load())
}

// ShouldFailBeforeStorage reports whether Ready processing should abort before
// persisting data.
func ShouldFailBeforeStorage() bool {
	return Current()&BeforeStorage != 0
}

// ShouldSkipLocalMetaUpdate reports whether local raft WAL pointer updates
// should be skipped even though WAL data was appended.
func ShouldSkipLocalMetaUpdate() bool {
	return Current()&SkipLocalMeta != 0
}

// ShouldFailAfterSnapshotApplyBeforePublish reports whether store-level
// snapshot install should fail after local apply but before peer publication.
func ShouldFailAfterSnapshotApplyBeforePublish() bool {
	return Current()&AfterSnapshotApplyBeforePublish != 0
}

// ShouldFailAfterInitModePreparing reports whether migration init should stop
// immediately after persisting MODE=preparing.
func ShouldFailAfterInitModePreparing() bool {
	return Current()&AfterInitModePreparing != 0
}

// ShouldFailAfterInitCatalogPersist reports whether migration init should stop
// immediately after persisting local region catalog state.
func ShouldFailAfterInitCatalogPersist() bool {
	return Current()&AfterInitCatalogPersist != 0
}

// ShouldFailAfterInitSeedSnapshot reports whether migration init should stop
// after exporting the seed logical snapshot but before seeded finalization.
func ShouldFailAfterInitSeedSnapshot() bool {
	return Current()&AfterInitSeedSnapshot != 0
}
