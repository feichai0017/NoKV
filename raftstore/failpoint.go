package raftstore

import "sync/atomic"

// ReadyFailpointMode configures different failure injection hooks for raft Ready
// processing. Modes can be ORed together to simulate multiple failure points.
type ReadyFailpointMode uint32

const (
	// ReadyFailpointNone disables failure injection.
	ReadyFailpointNone ReadyFailpointMode = 0
	// ReadyFailpointBeforeStorage injects an error before Ready data is written
	// to persistent storage, matching earlier unit test behaviour.
	ReadyFailpointBeforeStorage ReadyFailpointMode = 1 << iota
	// ReadyFailpointSkipManifest simulates a crash after WAL append but before
	// manifest pointers advance, allowing recovery tests to replay from WAL.
	ReadyFailpointSkipManifest
)

var readyFailpoint atomic.Uint32

// SetReadyFailpoint configures the active Ready failure injection mode. Passing
// ReadyFailpointNone clears any previously configured failpoint.
func SetReadyFailpoint(mode ReadyFailpointMode) {
	readyFailpoint.Store(uint32(mode))
}

func readyFailpointMode() ReadyFailpointMode {
	return ReadyFailpointMode(readyFailpoint.Load())
}

func shouldFailBeforeStorage() bool {
	return readyFailpointMode()&ReadyFailpointBeforeStorage != 0
}

func shouldSkipManifestUpdate() bool {
	return readyFailpointMode()&ReadyFailpointSkipManifest != 0
}

// ShouldInjectFailure reports whether the Ready pipeline should abort before
// writing to storage. Exposed for legacy callers and tests.
func ShouldInjectFailure() bool {
	return shouldFailBeforeStorage()
}
