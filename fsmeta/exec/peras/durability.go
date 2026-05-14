// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

type SegmentPersistenceLevel uint8

const (
	// Witnessed is deliberately not a public flush level. A witness quorum is
	// recovery evidence; user durable metadata starts at raftstore install.
	//
	// SegmentPersistenceDurable waits until the segment is witnessed and
	// installed in raftstore catalog storage. Root publish may lag.
	SegmentPersistenceDurable SegmentPersistenceLevel = iota + 1
	// SegmentPersistencePublished waits until the installed segment is also
	// published through the rooted authority frontier.
	SegmentPersistencePublished
)

// NormalizeSegmentPersistence preserves the user-facing default: callers that
// ask for durable persistence wait for witness quorum plus raftstore install.
func NormalizeSegmentPersistence(level SegmentPersistenceLevel) SegmentPersistenceLevel {
	if level == 0 {
		return SegmentPersistenceDurable
	}
	return level
}

// Valid reports whether a flush request names a supported persistence boundary.
func (level SegmentPersistenceLevel) Valid() bool {
	switch NormalizeSegmentPersistence(level) {
	case SegmentPersistenceDurable, SegmentPersistencePublished:
		return true
	default:
		return false
	}
}

// RequiresPublish reports whether a flush must publish the rooted frontier
// before returning to the caller.
func (level SegmentPersistenceLevel) RequiresPublish() bool {
	return NormalizeSegmentPersistence(level) == SegmentPersistencePublished
}
