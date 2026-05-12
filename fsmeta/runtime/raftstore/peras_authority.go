package raftstore

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

const defaultPerasAuthorityTTL = 5 * time.Minute

type perasAuthorityClient interface {
	ApplyPerasAuthority(context.Context, *coordpb.ApplyPerasAuthorityRequest) (*coordpb.ApplyPerasAuthorityResponse, error)
}

// PerasAuthorityManager is the fsmeta-side holder adapter for Peras
// authority. It talks only to coordinator; meta/root remains hidden behind the
// coordinator service boundary.
type PerasAuthorityManager struct {
	coord    perasAuthorityClient
	table    *perasauth.ActiveAuthorities
	holderID string
	ttl      time.Duration
	now      func() time.Time
}

func NewPerasAuthorityManager(coord perasAuthorityClient, table *perasauth.ActiveAuthorities, holderID string, ttl time.Duration, now func() time.Time) (*PerasAuthorityManager, error) {
	holderID = strings.TrimSpace(holderID)
	if coord == nil {
		return nil, errPerasAuthorityClientRequired
	}
	if table == nil {
		return nil, errPerasAuthorityTableRequired
	}
	if holderID == "" {
		return nil, errPerasAuthorityHolderRequired
	}
	if ttl < 0 {
		return nil, errPerasAuthorityTTLInvalid
	}
	if ttl == 0 {
		ttl = defaultPerasAuthorityTTL
	}
	if now == nil {
		now = time.Now
	}
	return &PerasAuthorityManager{
		coord:    coord,
		table:    table,
		holderID: holderID,
		ttl:      ttl,
		now:      now,
	}, nil
}

func (m *PerasAuthorityManager) HolderID() string {
	if m == nil {
		return ""
	}
	return m.holderID
}

func (m *PerasAuthorityManager) AcquirePerasAuthority(ctx context.Context, scope compile.AuthorityScope) (bool, error) {
	_, owned, err := m.Acquire(ctx, scope)
	return owned, err
}

func (m *PerasAuthorityManager) Acquire(ctx context.Context, scope compile.AuthorityScope) (perasauth.AuthorityGrant, bool, error) {
	if m == nil {
		return perasauth.AuthorityGrant{}, false, errPerasAuthorityClientRequired
	}
	now := m.now()
	if grant, ok, err := m.table.Find(scope, now); err != nil {
		return perasauth.AuthorityGrant{}, false, err
	} else if ok && grant.HolderID == m.holderID {
		return grant, true, nil
	}

	cmd := rootproto.PerasAuthorityCommand{
		Kind:            rootproto.PerasAuthorityActAcquire,
		HolderID:        m.holderID,
		Scope:           perasAuthorityAcquireScope(scope),
		NowUnixNano:     now.UnixNano(),
		ExpiresUnixNano: now.Add(m.ttl).UnixNano(),
	}
	resp, err := m.coord.ApplyPerasAuthority(ctx, &coordpb.ApplyPerasAuthorityRequest{
		Command: metawire.RootPerasAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return perasauth.AuthorityGrant{}, false, err
	}
	if err := m.installResponse(resp); err != nil {
		return perasauth.AuthorityGrant{}, false, err
	}
	switch resp.GetStatus() {
	case metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED:
		grant := metawire.RootPerasAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			return perasauth.AuthorityGrant{}, false, errPerasAuthorityInvalidResponse
		}
		return grant, true, nil
	case metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD:
		grant, _, err := m.table.Find(scope, now)
		return grant, false, err
	case metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED:
		return perasauth.AuthorityGrant{}, false, errPerasAuthorityInvalidResponse
	default:
		return perasauth.AuthorityGrant{}, false, errPerasAuthorityInvalidResponse
	}
}

func perasAuthorityAcquireScope(scope compile.AuthorityScope) rootproto.PerasAuthorityScope {
	rootScope := perasauth.AuthorityScopeFromDelta(scope)
	// Segment install is bucket-local, but the holder-visible transition is
	// authority-local and may span buckets before seal. Keep the rooted grant
	// mount-wide for this holder epoch; region locality is enforced later by
	// splitting the sealed segment into bucket-local install records.
	rootScope.Buckets = nil
	rootScope.Parents = nil
	rootScope.Inodes = nil
	return rootScope
}

func (m *PerasAuthorityManager) Retire(ctx context.Context, grant perasauth.AuthorityGrant) error {
	if m == nil {
		return errPerasAuthorityClientRequired
	}
	if !grant.Valid() {
		return errPerasAuthorityInvalidResponse
	}
	if strings.TrimSpace(grant.HolderID) != m.holderID {
		return errPerasAuthorityNotHeld
	}
	now := m.now()
	cmd := rootproto.PerasAuthorityCommand{
		Kind:        rootproto.PerasAuthorityActRetire,
		HolderID:    m.holderID,
		GrantID:     grant.GrantID,
		NowUnixNano: now.UnixNano(),
	}
	resp, err := m.coord.ApplyPerasAuthority(ctx, &coordpb.ApplyPerasAuthorityRequest{
		Command: metawire.RootPerasAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return err
	}
	if err := m.installResponse(resp); err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD {
		return errPerasAuthorityNotHeld
	}
	if resp.GetStatus() != metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED {
		return errPerasAuthorityInvalidResponse
	}
	return nil
}

func (m *PerasAuthorityManager) SealPerasSegment(ctx context.Context, grant perasauth.AuthorityGrant, segment fsperas.PerasSegment, payloadDigest [32]byte, cursor PerasInstallCursor) error {
	if m == nil {
		return errPerasAuthorityClientRequired
	}
	if !grant.Valid() {
		return errPerasAuthorityInvalidResponse
	}
	if !cursor.Valid() {
		return errPerasAuthorityInvalidResponse
	}
	if strings.TrimSpace(grant.HolderID) != m.holderID {
		return errPerasAuthorityNotHeld
	}
	stats := segment.Stats()
	now := m.now()
	cmd := rootproto.PerasAuthorityCommand{
		Kind:                 rootproto.PerasAuthorityActSeal,
		HolderID:             m.holderID,
		GrantID:              grant.GrantID,
		NowUnixNano:          now.UnixNano(),
		SegmentRoot:          segment.Root,
		SegmentPayloadDigest: payloadDigest,
		OperationCount:       stats.OperationCount,
		EntryCount:           stats.EntryCount,
		InstallRegionID:      cursor.RegionID,
		InstallTerm:          cursor.Term,
		InstallIndex:         cursor.Index,
		InstallVersion:       cursor.InstallVersion,
	}
	resp, err := m.coord.ApplyPerasAuthority(ctx, &coordpb.ApplyPerasAuthorityRequest{
		Command: metawire.RootPerasAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return err
	}
	if err := m.installResponse(resp); err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD {
		return errPerasAuthorityNotHeld
	}
	if resp.GetStatus() != metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_SEALED {
		return errPerasAuthorityInvalidResponse
	}
	return nil
}

func (m *PerasAuthorityManager) RetirePerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if m == nil {
		return errPerasAuthorityClientRequired
	}
	grants := m.ownedGrantsForScopes(scopes...)
	for _, grant := range grants {
		if err := m.Retire(ctx, grant); err != nil {
			return err
		}
	}
	return nil
}

func (m *PerasAuthorityManager) ownedGrantsForScopes(scopes ...compile.AuthorityScope) []perasauth.AuthorityGrant {
	if m == nil || m.table == nil || strings.TrimSpace(m.holderID) == "" {
		return nil
	}
	now := m.now()
	snapshot := m.table.Snapshot()
	out := make([]perasauth.AuthorityGrant, 0, len(snapshot))
	for _, grant := range snapshot {
		if grant.HolderID != m.holderID || !grant.ActiveAt(now.UnixNano()) {
			continue
		}
		if len(scopes) == 0 {
			out = append(out, grant)
			continue
		}
		for _, scope := range scopes {
			if perasAuthorityScopeEmpty(scope) || perasGrantMatchesRetireScope(grant, scope, now) {
				out = append(out, grant)
				break
			}
		}
	}
	return out
}

func perasGrantMatchesRetireScope(grant perasauth.AuthorityGrant, scope compile.AuthorityScope, now time.Time) bool {
	if !grant.Valid() || !grant.ActiveAt(now.UnixNano()) {
		return false
	}
	if scope.MountKeyID == 0 || grant.Scope.MountKeyID != uint64(scope.MountKeyID) {
		return false
	}
	if len(scope.Buckets) == 0 && len(scope.Parents) == 0 && len(scope.Inodes) == 0 {
		return true
	}
	return perasauth.GrantCoversDelta(grant, scope, now)
}

func perasAuthorityScopeEmpty(scope compile.AuthorityScope) bool {
	return scope.Mount == "" || scope.MountKeyID == 0
}

func (m *PerasAuthorityManager) installResponse(resp *coordpb.ApplyPerasAuthorityResponse) error {
	if m == nil || m.table == nil {
		return errPerasAuthorityTableRequired
	}
	if resp == nil {
		return errPerasAuthorityInvalidResponse
	}
	grants, err := parsePerasAuthorityGrants(resp.GetActiveGrants())
	if err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED {
		grant := metawire.RootPerasAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			return errPerasAuthorityInvalidResponse
		}
		grants = appendPerasGrantIfMissing(grants, grant)
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD && len(grants) == 0 {
		return errPerasAuthorityInvalidResponse
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_UNSPECIFIED {
		return errPerasAuthorityInvalidResponse
	}
	return m.table.Replace(grants)
}

func parsePerasAuthorityGrants(in []*metapb.RootPerasAuthorityGrant) ([]perasauth.AuthorityGrant, error) {
	out := make([]perasauth.AuthorityGrant, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, pbGrant := range in {
		grant := metawire.RootPerasAuthorityGrantFromProto(pbGrant)
		if !grant.Valid() {
			return nil, errPerasAuthorityInvalidResponse
		}
		if _, ok := seen[grant.GrantID]; ok {
			return nil, errPerasAuthorityInvalidResponse
		}
		seen[grant.GrantID] = struct{}{}
		out = append(out, grant)
	}
	return out, nil
}

func appendPerasGrantIfMissing(grants []perasauth.AuthorityGrant, grant perasauth.AuthorityGrant) []perasauth.AuthorityGrant {
	for _, current := range grants {
		if current.GrantID == grant.GrantID {
			return grants
		}
	}
	return append(grants, grant)
}

func IsPerasAuthorityNotHeld(err error) bool {
	return errors.Is(err, errPerasAuthorityNotHeld)
}
