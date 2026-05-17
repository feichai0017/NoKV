// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const (
	localPerasRegionID uint64 = 1
	localPerasTerm     uint64 = 1
)

type localPerasAuthority struct {
	holderID string
	grant    rootproto.PerasAuthorityGrant
	now      func() time.Time
}

func newLocalPerasAuthority(holderID string, mount fsmetaexec.MountAdmission, now func() time.Time) *localPerasAuthority {
	if now == nil {
		now = time.Now
	}
	return &localPerasAuthority{
		holderID: holderID,
		grant: rootproto.PerasAuthorityGrant{
			GrantID:          "local/" + string(mount.MountID),
			EpochID:          1,
			HolderID:         holderID,
			Scope:            localPerasRootScope(mount),
			ExpiresUnixNano:  now().Add(24 * time.Hour).UnixNano(),
			RootClusterEpoch: 1,
			IssuedRootToken: rootproto.AuthorityRootToken{
				Term:     localPerasTerm,
				Index:    1,
				Revision: 1,
			},
		},
		now: now,
	}
}

func localPerasRootScope(mount fsmetaexec.MountAdmission) rootproto.PerasAuthorityScope {
	identity := mount.Identity()
	return rootproto.PerasAuthorityScope{
		MountID:    string(identity.MountID),
		MountKeyID: uint64(identity.MountKeyID),
	}
}

func (a *localPerasAuthority) HolderID() string {
	if a == nil {
		return ""
	}
	return a.holderID
}

func (a *localPerasAuthority) Scope() compile.AuthorityScope {
	if a == nil {
		return compile.AuthorityScope{}
	}
	return runtimeperas.ScopeFromGrant(a.grant)
}

func (a *localPerasAuthority) Acquire(_ context.Context, scope compile.AuthorityScope) (rootproto.PerasAuthorityGrant, bool, error) {
	if a == nil {
		return rootproto.PerasAuthorityGrant{}, false, runtimeperas.ErrRuntimeInvalid
	}
	grant := rootproto.ClonePerasAuthorityGrant(a.grant)
	if a.now != nil {
		grant.ExpiresUnixNano = a.now().Add(24 * time.Hour).UnixNano()
	}
	if !grant.Valid() {
		return rootproto.PerasAuthorityGrant{}, false, runtimeperas.ErrRuntimeInvalid
	}
	if scope.Mount != "" && string(scope.Mount) != a.grant.Scope.MountID {
		return rootproto.PerasAuthorityGrant{}, false, nil
	}
	if scope.MountKeyID != 0 && uint64(scope.MountKeyID) != a.grant.Scope.MountKeyID {
		return rootproto.PerasAuthorityGrant{}, false, nil
	}
	return grant, true, nil
}

func (a *localPerasAuthority) AcquirePerasAuthority(ctx context.Context, scope compile.AuthorityScope) (bool, error) {
	_, owned, err := a.Acquire(ctx, scope)
	return owned, err
}

func (a *localPerasAuthority) RetirePerasAuthority(context.Context, ...compile.AuthorityScope) error {
	if a == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	return nil
}

func appendLocalPerasMutation(mutations []*kvrpcpb.Mutation, mutation *kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	if mutation == nil || len(mutation.GetKey()) == 0 {
		return mutations
	}
	for _, existing := range mutations {
		if existing != nil && bytes.Equal(existing.GetKey(), mutation.GetKey()) {
			existing.Op = mutation.GetOp()
			existing.Key = cloneBytes(mutation.GetKey())
			existing.Value = cloneBytes(mutation.GetValue())
			existing.AssertionNotExist = mutation.GetAssertionNotExist()
			existing.ExpiresAt = mutation.GetExpiresAt()
			return mutations
		}
	}
	return append(mutations, mutation)
}
