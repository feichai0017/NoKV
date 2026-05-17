// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

const defaultGrantTTL = 5 * time.Minute

type Client interface {
	ApplyVisibleAuthority(context.Context, *coordpb.ApplyVisibleAuthorityRequest) (*coordpb.ApplyVisibleAuthorityResponse, error)
	ListVisibleAuthoritySeals(context.Context, *coordpb.ListVisibleAuthoritySealsRequest) (*coordpb.ListVisibleAuthoritySealsResponse, error)
}

type InstallCursor struct {
	RegionID       uint64
	Term           uint64
	Index          uint64
	InstallVersion uint64
}

func (c InstallCursor) Valid() bool {
	return c.RegionID != 0 && c.Term != 0 && c.Index != 0 && c.InstallVersion != 0
}

// AuthorityManager is the fsmeta holder adapter for root-issued Peras authority.
// It talks only to coordinator; meta/root remains hidden behind the coordinator
// service boundary.
type AuthorityManager struct {
	coord    Client
	table    *ActiveAuthorities
	holderID string
	ttl      time.Duration
	now      func() time.Time

	acquireMu sync.Mutex
	acquires  map[string]*acquireCall
}

type acquireCall struct {
	done  chan struct{}
	grant rootproto.VisibleAuthorityGrant
	owned bool
	err   error
}

func NewAuthorityManager(coord Client, table *ActiveAuthorities, holderID string, ttl time.Duration, now func() time.Time) (*AuthorityManager, error) {
	holderID = strings.TrimSpace(holderID)
	if coord == nil {
		return nil, ErrClientRequired
	}
	if table == nil {
		return nil, ErrTableRequired
	}
	if holderID == "" {
		return nil, ErrHolderRequired
	}
	if ttl < 0 {
		return nil, ErrTTLInvalid
	}
	if ttl == 0 {
		ttl = defaultGrantTTL
	}
	if now == nil {
		now = time.Now
	}
	return &AuthorityManager{
		coord:    coord,
		table:    table,
		holderID: holderID,
		ttl:      ttl,
		now:      now,
	}, nil
}

func (m *AuthorityManager) HolderID() string {
	if m == nil {
		return ""
	}
	return m.holderID
}

func (m *AuthorityManager) AcquireVisibleAuthority(ctx context.Context, scope compile.AuthorityScope) (bool, error) {
	_, owned, err := m.Acquire(ctx, scope)
	return owned, err
}

func (m *AuthorityManager) Acquire(ctx context.Context, scope compile.AuthorityScope) (rootproto.VisibleAuthorityGrant, bool, error) {
	if m == nil {
		return rootproto.VisibleAuthorityGrant{}, false, ErrClientRequired
	}
	now := m.now()
	if grant, ok, err := m.table.Find(scope, now); err != nil {
		return rootproto.VisibleAuthorityGrant{}, false, err
	} else if ok && grant.HolderID == m.holderID {
		return grant, true, nil
	}

	rootScope := acquireScope(scope)
	key := acquireKey(rootScope)
	if call, leader := m.beginAcquire(key); !leader {
		select {
		case <-call.done:
			return call.grant, call.owned, call.err
		case <-ctx.Done():
			return rootproto.VisibleAuthorityGrant{}, false, ctx.Err()
		}
	}

	var grant rootproto.VisibleAuthorityGrant
	var owned bool
	cmd := rootproto.VisibleAuthorityCommand{
		Kind:            rootproto.VisibleAuthorityActAcquire,
		HolderID:        m.holderID,
		Scope:           rootScope,
		NowUnixNano:     now.UnixNano(),
		ExpiresUnixNano: now.Add(m.ttl).UnixNano(),
	}
	resp, err := m.coord.ApplyVisibleAuthority(ctx, &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(cmd),
	})
	if err != nil {
		m.finishAcquire(key, rootproto.VisibleAuthorityGrant{}, false, err)
		return rootproto.VisibleAuthorityGrant{}, false, err
	}
	if err := m.installResponse(resp); err != nil {
		m.finishAcquire(key, rootproto.VisibleAuthorityGrant{}, false, err)
		return rootproto.VisibleAuthorityGrant{}, false, err
	}
	switch resp.GetStatus() {
	case metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_GRANTED:
		grant = metawire.RootVisibleAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			m.finishAcquire(key, rootproto.VisibleAuthorityGrant{}, false, ErrInvalidResponse)
			return rootproto.VisibleAuthorityGrant{}, false, ErrInvalidResponse
		}
		owned = true
	case metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD:
		grant, _, err = m.table.Find(scope, now)
		owned = false
	case metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_RETIRED:
		err = ErrInvalidResponse
	default:
		err = ErrInvalidResponse
	}
	m.finishAcquire(key, grant, owned, err)
	return grant, owned, err
}

func (m *AuthorityManager) beginAcquire(key string) (*acquireCall, bool) {
	m.acquireMu.Lock()
	defer m.acquireMu.Unlock()
	if m.acquires == nil {
		m.acquires = make(map[string]*acquireCall)
	}
	if call := m.acquires[key]; call != nil {
		return call, false
	}
	call := &acquireCall{done: make(chan struct{})}
	m.acquires[key] = call
	return call, true
}

func (m *AuthorityManager) finishAcquire(key string, grant rootproto.VisibleAuthorityGrant, owned bool, err error) {
	m.acquireMu.Lock()
	call := m.acquires[key]
	if call != nil {
		call.grant = grant
		call.owned = owned
		call.err = err
		delete(m.acquires, key)
		close(call.done)
	}
	m.acquireMu.Unlock()
}

func acquireKey(scope rootproto.VisibleAuthorityScope) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(scope.MountKeyID, 10))
	for _, bucket := range scope.Buckets {
		b.WriteByte('/')
		b.WriteString(strconv.FormatUint(uint64(bucket), 10))
	}
	return b.String()
}

func acquireScope(scope compile.AuthorityScope) rootproto.VisibleAuthorityScope {
	rootScope := AuthorityScopeFromDelta(scope)
	// Root v1 proves exclusion, not workload intent. Request the mount-local
	// bucket set as one rooted capability so bursty namespace creation pays one
	// authority round instead of one round per affinity bucket. Do not pin parent
	// or freshly allocated inode IDs into the rooted grant: parent-only grants
	// with wildcard inodes still overlap at root, while per-inode grants make
	// ordinary create bursts conflict with their own predecessor grant.
	rootScope.Buckets = nil
	rootScope.Parents = nil
	rootScope.Inodes = nil
	return rootScope
}

func (m *AuthorityManager) Retire(ctx context.Context, grant rootproto.VisibleAuthorityGrant) error {
	if m == nil {
		return ErrClientRequired
	}
	if !grant.Valid() {
		return ErrInvalidResponse
	}
	if strings.TrimSpace(grant.HolderID) != m.holderID {
		return ErrNotHeld
	}
	now := m.now()
	cmd := rootproto.VisibleAuthorityCommand{
		Kind:        rootproto.VisibleAuthorityActRetire,
		HolderID:    m.holderID,
		GrantID:     grant.GrantID,
		NowUnixNano: now.UnixNano(),
	}
	resp, err := m.coord.ApplyVisibleAuthority(ctx, &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return err
	}
	if err := m.installResponse(resp); err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD {
		return ErrNotHeld
	}
	if resp.GetStatus() != metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_RETIRED {
		return ErrInvalidResponse
	}
	return nil
}

func (m *AuthorityManager) PublishSegmentSeal(ctx context.Context, grant rootproto.VisibleAuthorityGrant, segment fsperas.PerasSegment, payloadDigest [32]byte, cursor InstallCursor) error {
	if m == nil {
		return ErrClientRequired
	}
	if !grant.Valid() {
		return ErrInvalidResponse
	}
	if !cursor.Valid() {
		return ErrInvalidResponse
	}
	if strings.TrimSpace(grant.HolderID) != m.holderID {
		return ErrNotHeld
	}
	stats := segment.Stats()
	now := m.now()
	cmd := rootproto.VisibleAuthorityCommand{
		Kind:                 rootproto.VisibleAuthorityActSeal,
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
	resp, err := m.coord.ApplyVisibleAuthority(ctx, &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(cmd),
	})
	if err != nil {
		return err
	}
	if err := m.installResponse(resp); err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD {
		return ErrNotHeld
	}
	if resp.GetStatus() != metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_SEALED {
		return ErrInvalidResponse
	}
	return nil
}

func (m *AuthorityManager) ListVisibleAuthoritySeals(ctx context.Context, scope compile.AuthorityScope) ([]rootproto.VisibleAuthoritySeal, error) {
	if m == nil {
		return nil, ErrClientRequired
	}
	resp, err := m.coord.ListVisibleAuthoritySeals(ctx, &coordpb.ListVisibleAuthoritySealsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]rootproto.VisibleAuthoritySeal, 0, len(resp.GetSeals()))
	for _, pbSeal := range resp.GetSeals() {
		seal := metawire.RootVisibleAuthoritySealFromProto(pbSeal)
		if !seal.Valid() {
			return nil, ErrInvalidResponse
		}
		if !sealCoversScope(seal, scope) {
			continue
		}
		out = append(out, rootproto.CloneVisibleAuthoritySeal(seal))
	}
	return out, nil
}

func sealCoversScope(seal rootproto.VisibleAuthoritySeal, scope compile.AuthorityScope) bool {
	if !seal.Valid() {
		return false
	}
	if ScopeEmpty(scope) {
		return true
	}
	grant := rootproto.VisibleAuthorityGrant{
		GrantID:         seal.GrantID,
		EpochID:         seal.EpochID,
		HolderID:        seal.HolderID,
		Scope:           seal.Scope,
		ExpiresUnixNano: 1,
	}
	return grant.Covers(AuthorityScopeFromDelta(scope), 0)
}

func (m *AuthorityManager) RetireVisibleAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
	if m == nil {
		return ErrClientRequired
	}
	grants := m.ownedGrantsForScopes(scopes...)
	for _, grant := range grants {
		if err := m.Retire(ctx, grant); err != nil {
			return err
		}
	}
	return nil
}

func (m *AuthorityManager) ownedGrantsForScopes(scopes ...compile.AuthorityScope) []rootproto.VisibleAuthorityGrant {
	if m == nil || m.table == nil || strings.TrimSpace(m.holderID) == "" {
		return nil
	}
	now := m.now()
	snapshot := m.table.Snapshot()
	out := make([]rootproto.VisibleAuthorityGrant, 0, len(snapshot))
	for _, grant := range snapshot {
		if grant.HolderID != m.holderID || !grant.ActiveAt(now.UnixNano()) {
			continue
		}
		if len(scopes) == 0 {
			out = append(out, grant)
			continue
		}
		for _, scope := range scopes {
			if ScopeEmpty(scope) || grantMatchesRetireScope(grant, scope, now) {
				out = append(out, grant)
				break
			}
		}
	}
	return out
}

func grantMatchesRetireScope(grant rootproto.VisibleAuthorityGrant, scope compile.AuthorityScope, now time.Time) bool {
	if !grant.Valid() || !grant.ActiveAt(now.UnixNano()) {
		return false
	}
	if scope.MountKeyID == 0 || grant.Scope.MountKeyID != uint64(scope.MountKeyID) {
		return false
	}
	if len(scope.Buckets) == 0 && len(scope.Parents) == 0 && len(scope.Inodes) == 0 {
		return true
	}
	return GrantCoversDelta(grant, scope, now)
}

func (m *AuthorityManager) installResponse(resp *coordpb.ApplyVisibleAuthorityResponse) error {
	if m == nil || m.table == nil {
		return ErrTableRequired
	}
	if resp == nil {
		return ErrInvalidResponse
	}
	grants, err := parseVisibleAuthorityGrants(resp.GetActiveGrants())
	if err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_GRANTED {
		grant := metawire.RootVisibleAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			return ErrInvalidResponse
		}
		grants = appendVisibleGrantIfMissing(grants, grant)
	}
	if resp.GetStatus() == metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD && len(grants) == 0 {
		return ErrInvalidResponse
	}
	if resp.GetStatus() == metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_UNSPECIFIED {
		return ErrInvalidResponse
	}
	return m.table.Replace(grants)
}

func parseVisibleAuthorityGrants(in []*metapb.RootVisibleAuthorityGrant) ([]rootproto.VisibleAuthorityGrant, error) {
	out := make([]rootproto.VisibleAuthorityGrant, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, pbGrant := range in {
		grant := metawire.RootVisibleAuthorityGrantFromProto(pbGrant)
		if !grant.Valid() {
			return nil, ErrInvalidResponse
		}
		if _, ok := seen[grant.GrantID]; ok {
			return nil, ErrInvalidResponse
		}
		seen[grant.GrantID] = struct{}{}
		out = append(out, grant)
	}
	return out, nil
}

func appendVisibleGrantIfMissing(grants []rootproto.VisibleAuthorityGrant, grant rootproto.VisibleAuthorityGrant) []rootproto.VisibleAuthorityGrant {
	for _, current := range grants {
		if current.GrantID == grant.GrantID {
			return grants
		}
	}
	return append(grants, grant)
}
