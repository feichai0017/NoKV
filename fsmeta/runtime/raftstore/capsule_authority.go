package raftstore

import (
	"context"
	"errors"
	"strings"
	"time"

	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

const defaultCapsuleAuthorityTTL = 30 * time.Second

type capsuleAuthorityClient interface {
	ApplyCapsuleAuthority(context.Context, *coordpb.ApplyCapsuleAuthorityRequest) (*coordpb.ApplyCapsuleAuthorityResponse, error)
}

// CapsuleAuthorityManager is the fsmeta-side holder adapter for Capsule
// authority. It talks only to coordinator; meta/root remains hidden behind the
// coordinator service boundary.
type CapsuleAuthorityManager struct {
	coord    capsuleAuthorityClient
	table    *fscapsule.ActiveAuthorities
	holderID string
	ttl      time.Duration
	now      func() time.Time
}

func NewCapsuleAuthorityManager(coord capsuleAuthorityClient, table *fscapsule.ActiveAuthorities, holderID string, ttl time.Duration, now func() time.Time) (*CapsuleAuthorityManager, error) {
	holderID = strings.TrimSpace(holderID)
	if coord == nil {
		return nil, errCapsuleAuthorityClientRequired
	}
	if table == nil {
		return nil, errCapsuleAuthorityTableRequired
	}
	if holderID == "" {
		return nil, errCapsuleAuthorityHolderRequired
	}
	if ttl < 0 {
		return nil, errCapsuleAuthorityTTLInvalid
	}
	if ttl == 0 {
		ttl = defaultCapsuleAuthorityTTL
	}
	if now == nil {
		now = time.Now
	}
	return &CapsuleAuthorityManager{
		coord:    coord,
		table:    table,
		holderID: holderID,
		ttl:      ttl,
		now:      now,
	}, nil
}

func (m *CapsuleAuthorityManager) HolderID() string {
	if m == nil {
		return ""
	}
	return m.holderID
}

func (m *CapsuleAuthorityManager) Acquire(ctx context.Context, scope compile.AuthorityScope) (fscapsule.AuthorityGrant, bool, error) {
	if m == nil {
		return fscapsule.AuthorityGrant{}, false, errCapsuleAuthorityClientRequired
	}
	now := m.now()
	if grant, ok, err := m.table.Find(scope, now); err != nil {
		return fscapsule.AuthorityGrant{}, false, err
	} else if ok && grant.HolderID == m.holderID {
		return grant, true, nil
	}

	cmd := rootproto.CapsuleAuthorityCommand{
		Kind:            rootproto.CapsuleAuthorityActAcquire,
		HolderID:        m.holderID,
		Scope:           fscapsule.AuthorityScopeFromDelta(scope),
		NowUnixNano:     now.UnixNano(),
		ExpiresUnixNano: now.Add(m.ttl).UnixNano(),
	}
	resp, err := m.coord.ApplyCapsuleAuthority(ctx, &coordpb.ApplyCapsuleAuthorityRequest{
		Command: metawire.RootCapsuleAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return fscapsule.AuthorityGrant{}, false, err
	}
	if err := m.installResponse(resp); err != nil {
		return fscapsule.AuthorityGrant{}, false, err
	}
	switch resp.GetStatus() {
	case metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED:
		grant := metawire.RootCapsuleAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			return fscapsule.AuthorityGrant{}, false, errCapsuleAuthorityInvalidResponse
		}
		return grant, true, nil
	case metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD:
		grant, _, err := m.table.Find(scope, now)
		return grant, false, err
	case metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_RETIRED:
		return fscapsule.AuthorityGrant{}, false, errCapsuleAuthorityInvalidResponse
	default:
		return fscapsule.AuthorityGrant{}, false, errCapsuleAuthorityInvalidResponse
	}
}

func (m *CapsuleAuthorityManager) Retire(ctx context.Context, grant fscapsule.AuthorityGrant) error {
	if m == nil {
		return errCapsuleAuthorityClientRequired
	}
	if !grant.Valid() {
		return errCapsuleAuthorityInvalidResponse
	}
	if strings.TrimSpace(grant.HolderID) != m.holderID {
		return errCapsuleAuthorityNotHeld
	}
	now := m.now()
	cmd := rootproto.CapsuleAuthorityCommand{
		Kind:        rootproto.CapsuleAuthorityActRetire,
		HolderID:    m.holderID,
		GrantID:     grant.GrantID,
		NowUnixNano: now.UnixNano(),
	}
	resp, err := m.coord.ApplyCapsuleAuthority(ctx, &coordpb.ApplyCapsuleAuthorityRequest{
		Command: metawire.RootCapsuleAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return err
	}
	if err := m.installResponse(resp); err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD {
		return errCapsuleAuthorityNotHeld
	}
	if resp.GetStatus() != metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_RETIRED {
		return errCapsuleAuthorityInvalidResponse
	}
	return nil
}

func (m *CapsuleAuthorityManager) installResponse(resp *coordpb.ApplyCapsuleAuthorityResponse) error {
	if m == nil || m.table == nil {
		return errCapsuleAuthorityTableRequired
	}
	if resp == nil {
		return errCapsuleAuthorityInvalidResponse
	}
	grants, err := parseCapsuleAuthorityGrants(resp.GetActiveGrants())
	if err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED {
		grant := metawire.RootCapsuleAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			return errCapsuleAuthorityInvalidResponse
		}
		grants = appendCapsuleGrantIfMissing(grants, grant)
	}
	if resp.GetStatus() == metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD && len(grants) == 0 {
		return errCapsuleAuthorityInvalidResponse
	}
	if resp.GetStatus() == metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_UNSPECIFIED {
		return errCapsuleAuthorityInvalidResponse
	}
	return m.table.Replace(grants)
}

func parseCapsuleAuthorityGrants(in []*metapb.RootCapsuleAuthorityGrant) ([]fscapsule.AuthorityGrant, error) {
	out := make([]fscapsule.AuthorityGrant, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, pbGrant := range in {
		grant := metawire.RootCapsuleAuthorityGrantFromProto(pbGrant)
		if !grant.Valid() {
			return nil, errCapsuleAuthorityInvalidResponse
		}
		if _, ok := seen[grant.GrantID]; ok {
			return nil, errCapsuleAuthorityInvalidResponse
		}
		seen[grant.GrantID] = struct{}{}
		out = append(out, grant)
	}
	return out, nil
}

func appendCapsuleGrantIfMissing(grants []fscapsule.AuthorityGrant, grant fscapsule.AuthorityGrant) []fscapsule.AuthorityGrant {
	for _, current := range grants {
		if current.GrantID == grant.GrantID {
			return grants
		}
	}
	return append(grants, grant)
}

func IsCapsuleAuthorityNotHeld(err error) bool {
	return errors.Is(err, errCapsuleAuthorityNotHeld)
}
