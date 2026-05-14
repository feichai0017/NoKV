// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import kvrpcpb "github.com/feichai0017/NoKV/pb/kv"

// EffectiveSafePoint clamps a requested GC safe point by active retention
// floors. A zero requested safe point disables MVCC GC. Zero floors are ignored.
func EffectiveSafePoint(requested uint64, floors ...uint64) uint64 {
	if requested == 0 {
		return 0
	}
	effective := requested
	for _, floor := range floors {
		if floor != 0 && floor < effective {
			effective = floor
		}
	}
	return effective
}

// GCWriteVersion is one CFWrite version for one user key.
type GCWriteVersion struct {
	CommitTs uint64
	Write    Write
}

// GCWriteDecision describes whether one CFWrite version must be retained at a
// safe point.
type GCWriteDecision struct {
	CommitTs             uint64
	Write                Write
	Keep                 bool
	Anchor               bool
	RetainDefaultStartTs uint64
}

// PlanWriteGC applies Percolator's MVCC anchor rule to one user key's CFWrite
// versions. It is order-preserving: decisions are returned in the same order as
// versions.
func PlanWriteGC(versions []GCWriteVersion, safePoint uint64) []GCWriteDecision {
	return AppendWriteGCDecisions(nil, versions, safePoint)
}

// AppendWriteGCDecisions appends GC decisions to dst. Callers that scan many
// keys can reuse dst to avoid one short-lived allocation per user key.
func AppendWriteGCDecisions(dst []GCWriteDecision, versions []GCWriteVersion, safePoint uint64) []GCWriteDecision {
	anchor := -1
	if safePoint != 0 {
		latest := -1
		var latestTs uint64
		for i, version := range versions {
			if version.CommitTs >= safePoint || version.Write.Kind == kvrpcpb.Mutation_Rollback {
				continue
			}
			if version.CommitTs > latestTs {
				latest = i
				latestTs = version.CommitTs
			}
		}
		if latest >= 0 && versions[latest].Write.Kind != kvrpcpb.Mutation_Delete {
			anchor = latest
		}
	}

	base := len(dst)
	for range versions {
		dst = append(dst, GCWriteDecision{})
	}
	for i, version := range versions {
		keep := safePoint == 0 || version.CommitTs >= safePoint || i == anchor
		decision := GCWriteDecision{
			CommitTs: version.CommitTs,
			Write:    version.Write,
			Keep:     keep,
			Anchor:   i == anchor,
		}
		if keep && WriteNeedsDefaultRecord(version.Write) {
			decision.RetainDefaultStartTs = version.Write.StartTs
		}
		dst[base+i] = decision
	}
	return dst
}

// WriteNeedsDefaultRecord reports whether a write record refers to CFDefault.
func WriteNeedsDefaultRecord(write Write) bool {
	if len(write.ShortValue) > 0 {
		return false
	}
	switch write.Kind {
	case kvrpcpb.Mutation_Delete, kvrpcpb.Mutation_Rollback:
		return false
	default:
		return true
	}
}
