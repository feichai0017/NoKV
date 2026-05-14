// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

type KV struct {
	Key   []byte
	Value []byte
}

type GrantProvider interface {
	HolderID() string
	Acquire(context.Context, compile.AuthorityScope) (rootproto.PerasAuthorityGrant, bool, error)
}

type SealPublisher interface {
	PublishSegmentSeal(context.Context, rootproto.PerasAuthorityGrant, fsperas.PerasSegment, [32]byte, InstallCursor) error
}

type SealProvider interface {
	ListPerasAuthoritySeals(context.Context, compile.AuthorityScope) ([]rootproto.PerasAuthoritySeal, error)
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
