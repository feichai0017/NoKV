// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"fmt"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
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

type localPerasSegmentInstaller struct {
	runner *Runner
}

func (i localPerasSegmentInstaller) InstallSegment(ctx context.Context, req runtimeperas.SegmentInstallRequest) (runtimeperas.InstallCursor, error) {
	if i.runner == nil {
		return runtimeperas.InstallCursor{}, runtimeperas.ErrRuntimeInvalid
	}
	if _, err := fsperas.VerifyPerasSegmentPayload(req.Payload, req.Segment.Root, req.PayloadDigest); err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	startVersion, err := i.runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	commitVersion := startVersion + 1
	primary, mutations, err := localPerasInstallMutations(req, commitVersion)
	if err != nil {
		return runtimeperas.InstallCursor{}, err
	}
	if _, err := i.runner.MutateAtCommit(ctx, primary, mutations, startVersion, commitVersion, 0); err != nil {
		return runtimeperas.InstallCursor{}, fmt.Errorf("local peras install materialize=%t mutations=%d: %w", req.MaterializeMVCC, len(mutations), err)
	}
	return runtimeperas.InstallCursor{
		RegionID:       localPerasRegionID,
		Term:           localPerasTerm,
		Index:          commitVersion,
		InstallVersion: commitVersion,
	}, nil
}

func localPerasInstallMutations(req runtimeperas.SegmentInstallRequest, installVersion uint64) ([]byte, []*kvrpcpb.Mutation, error) {
	plan := req.Install
	if len(plan.CanonicalObjectKey) == 0 {
		var err error
		plan, err = fsperas.PerasSegmentInstallPlan(req.Segment, req.MaterializeMVCC)
		if err != nil {
			return nil, nil, err
		}
	}
	canonical := cloneBytes(plan.CanonicalObjectKey)
	if len(canonical) == 0 {
		return nil, nil, runtimeperas.ErrRuntimeInvalid
	}
	catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(req.Segment, installVersion, req.Payload, req.PayloadDigest)
	if err != nil {
		return nil, nil, err
	}
	mutations := []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   canonical,
		Value: catalogValue,
	}}
	indexKeys, err := fsperas.PerasSegmentCatalogIndexKeys(req.Segment)
	if err != nil {
		return nil, nil, err
	}
	for _, key := range indexKeys {
		indexValue, err := fsperas.EncodePerasSegmentCatalogIndexRecordFields(req.Segment.EpochID, installVersion, req.Segment.Root, req.PayloadDigest, uint64(len(req.Payload)), canonical)
		if err != nil {
			return nil, nil, err
		}
		mutations = appendLocalPerasMutation(mutations, &kvrpcpb.Mutation{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: indexValue,
		})
	}
	if req.MaterializeMVCC {
		for _, entry := range req.Segment.EntriesView() {
			mutation := &kvrpcpb.Mutation{Key: cloneBytes(entry.Key)}
			if entry.Delete {
				mutation.Op = kvrpcpb.Mutation_Delete
			} else {
				mutation.Op = kvrpcpb.Mutation_Put
				mutation.Value = cloneBytes(entry.Value)
			}
			mutations = appendLocalPerasMutation(mutations, mutation)
		}
	}
	return canonical, mutations, nil
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

type localPerasCatalogScanner struct {
	runner *Runner
}

func (s localPerasCatalogScanner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]runtimeperas.KV, error) {
	if s.runner == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	if version == 0 {
		var err error
		version, err = s.runner.ReserveTimestamp(ctx, 1)
		if err != nil {
			return nil, err
		}
	}
	rows, err := s.runner.Scan(ctx, startKey, limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]runtimeperas.KV, 0, len(rows))
	for _, row := range rows {
		out = append(out, runtimeperas.KV{
			Key:   cloneBytes(row.Key),
			Value: cloneBytes(row.Value),
		})
	}
	return out, nil
}
