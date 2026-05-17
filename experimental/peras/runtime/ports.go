// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

type KV struct {
	Key   []byte
	Value []byte
}

type GrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (rootproto.VisibleAuthorityGrant, bool, error)
}

type SealPublisher interface {
	PublishSegmentSeal(context.Context, rootproto.VisibleAuthorityGrant, fsperas.PerasSegment, [32]byte, InstallCursor) error
}

type SealProvider interface {
	ListVisibleAuthoritySeals(context.Context, compile.AuthorityScope) ([]rootproto.VisibleAuthoritySeal, error)
}

type SegmentInstallRequest struct {
	Scope           compile.AuthorityScope
	Segment         fsperas.PerasSegment
	Payload         []byte
	PayloadDigest   [32]byte
	Install         compile.InstallPlan
	MaterializeMVCC bool
}

type SegmentInstaller interface {
	InstallSegment(context.Context, SegmentInstallRequest) (InstallCursor, error)
}

// SegmentPayloadRequirement lets an install layer declare whether it consumes
// the encoded segment payload. Unknown installers are treated as requiring the
// payload so existing distributed/catalog paths keep their old behavior.
type SegmentPayloadRequirement interface {
	NeedsSegmentPayload() bool
}

// SegmentMaterializationRequirement lets an install layer declare that flushed
// segments are installed directly into the base MVCC view. This makes local vs.
// distributed runtime shape a property of the composed install layers instead
// of a parallel Config flag.
type SegmentMaterializationRequirement interface {
	MaterializesSegments() bool
}

// SegmentFinalizeRequest carries the installed segment and cursor into the
// post-publish runtime finalize stage.
type SegmentFinalizeRequest struct {
	Scope           compile.AuthorityScope
	Plan            fsperas.ReplayPlan
	Segment         fsperas.PerasSegment
	InstallCursor   InstallCursor
	MaterializeMVCC bool
}

// SegmentFinalizer updates runtime read/dedup state after segment durability
// and publish requirements have been satisfied.
type SegmentFinalizer interface {
	FinalizeSegment(context.Context, SegmentFinalizeRequest) error
}

type SegmentCatalogScanner interface {
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
}

func SegmentInstallRoutingKeys(segment fsperas.PerasSegment, materialize bool) ([][]byte, error) {
	if materialize {
		key, err := segment.FirstKey()
		if err != nil {
			return nil, err
		}
		return [][]byte{key}, nil
	}
	return fsperas.PerasSegmentCatalogObjectKeys(segment)
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
