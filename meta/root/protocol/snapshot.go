// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package protocol

// SnapshotEvidenceRef is rooted snapshot evidence that must be retained for a
// visible fsmeta snapshot frontier.
type SnapshotEvidenceRef struct {
	EpochID       uint64
	EvidenceRoot  [32]byte
	PayloadDigest [32]byte
}

func (r SnapshotEvidenceRef) Valid() bool {
	var zero [32]byte
	return r.EpochID != 0 && r.EvidenceRoot != zero && r.PayloadDigest != zero
}

func CloneSnapshotEvidenceRefs(refs []SnapshotEvidenceRef) []SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]SnapshotEvidenceRef, len(refs))
	copy(out, refs)
	return out
}
