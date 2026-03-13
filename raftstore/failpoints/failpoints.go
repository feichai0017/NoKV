package failpoints

import "sync/atomic"

// Mode configures different failure injection hooks for raft Ready processing.
// Modes can be ORed together to simulate multiple failure points.
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
