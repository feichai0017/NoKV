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
	Acquire(context.Context, compile.AuthorityScope) (AuthorityGrant, bool, error)
}

type SealPublisher interface {
	PublishSegmentSeal(context.Context, AuthorityGrant, fsperas.PerasSegment, [32]byte, InstallCursor) error
}

type SealProvider interface {
	ListPerasAuthoritySeals(context.Context, compile.AuthorityScope) ([]rootproto.PerasAuthoritySeal, error)
}

type SegmentInstallRequest struct {
	Scope           compile.AuthorityScope
	Segment         fsperas.PerasSegment
	Payload         []byte
	PayloadDigest   [32]byte
	MaterializeMVCC bool
}

type SegmentInstaller interface {
	InstallSegment(context.Context, SegmentInstallRequest) (InstallCursor, error)
}

type SegmentCatalogScanner interface {
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
}
