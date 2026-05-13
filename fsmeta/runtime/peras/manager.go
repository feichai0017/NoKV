package peras

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

const defaultGrantTTL = 5 * time.Minute

type Client interface {
	ApplyPerasAuthority(context.Context, *coordpb.ApplyPerasAuthorityRequest) (*coordpb.ApplyPerasAuthorityResponse, error)
	ListPerasAuthoritySeals(context.Context, *coordpb.ListPerasAuthoritySealsRequest) (*coordpb.ListPerasAuthoritySealsResponse, error)
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
	grant AuthorityGrant
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

func (m *AuthorityManager) AcquirePerasAuthority(ctx context.Context, scope compile.AuthorityScope) (bool, error) {
	_, owned, err := m.Acquire(ctx, scope)
	return owned, err
}

func (m *AuthorityManager) Acquire(ctx context.Context, scope compile.AuthorityScope) (AuthorityGrant, bool, error) {
	if m == nil {
		return AuthorityGrant{}, false, ErrClientRequired
	}
	now := m.now()
	if grant, ok, err := m.table.Find(scope, now); err != nil {
		return AuthorityGrant{}, false, err
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
			return AuthorityGrant{}, false, ctx.Err()
		}
	}

	var grant AuthorityGrant
	var owned bool
	cmd := rootproto.PerasAuthorityCommand{
		Kind:            rootproto.PerasAuthorityActAcquire,
		HolderID:        m.holderID,
		Scope:           rootScope,
		NowUnixNano:     now.UnixNano(),
		ExpiresUnixNano: now.Add(m.ttl).UnixNano(),
	}
	resp, err := m.coord.ApplyPerasAuthority(ctx, &coordpb.ApplyPerasAuthorityRequest{
		Command: metawire.RootPerasAuthorityCommandToProto(cmd),
	})
	if err != nil {
		m.finishAcquire(key, AuthorityGrant{}, false, err)
		return AuthorityGrant{}, false, err
	}
	if err := m.installResponse(resp); err != nil {
		m.finishAcquire(key, AuthorityGrant{}, false, err)
		return AuthorityGrant{}, false, err
	}
	switch resp.GetStatus() {
	case metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED:
		grant = metawire.RootPerasAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			m.finishAcquire(key, AuthorityGrant{}, false, ErrInvalidResponse)
			return AuthorityGrant{}, false, ErrInvalidResponse
		}
		owned = true
	case metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD:
		grant, _, err = m.table.Find(scope, now)
		owned = false
	case metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED:
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

func (m *AuthorityManager) finishAcquire(key string, grant AuthorityGrant, owned bool, err error) {
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

func acquireKey(scope rootproto.PerasAuthorityScope) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(scope.MountKeyID, 10))
	for _, bucket := range scope.Buckets {
		b.WriteByte('/')
		b.WriteString(strconv.FormatUint(uint64(bucket), 10))
	}
	return b.String()
}

func acquireScope(scope compile.AuthorityScope) rootproto.PerasAuthorityScope {
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

func (m *AuthorityManager) Retire(ctx context.Context, grant AuthorityGrant) error {
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
		return ErrNotHeld
	}
	if resp.GetStatus() != metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED {
		return ErrInvalidResponse
	}
	return nil
}

func (m *AuthorityManager) PublishSegmentSeal(ctx context.Context, grant AuthorityGrant, segment fsperas.PerasSegment, payloadDigest [32]byte, cursor InstallCursor) error {
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
		return ErrNotHeld
	}
	if resp.GetStatus() != metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_SEALED {
		return ErrInvalidResponse
	}
	return nil
}

func (m *AuthorityManager) ListPerasAuthoritySeals(ctx context.Context, scope compile.AuthorityScope) ([]rootproto.PerasAuthoritySeal, error) {
	if m == nil {
		return nil, ErrClientRequired
	}
	resp, err := m.coord.ListPerasAuthoritySeals(ctx, &coordpb.ListPerasAuthoritySealsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]rootproto.PerasAuthoritySeal, 0, len(resp.GetSeals()))
	for _, pbSeal := range resp.GetSeals() {
		seal := metawire.RootPerasAuthoritySealFromProto(pbSeal)
		if !seal.Valid() {
			return nil, ErrInvalidResponse
		}
		if !sealCoversScope(seal, scope) {
			continue
		}
		out = append(out, rootproto.ClonePerasAuthoritySeal(seal))
	}
	return out, nil
}

func sealCoversScope(seal rootproto.PerasAuthoritySeal, scope compile.AuthorityScope) bool {
	if !seal.Valid() {
		return false
	}
	if ScopeEmpty(scope) {
		return true
	}
	grant := rootproto.PerasAuthorityGrant{
		GrantID:         seal.GrantID,
		EpochID:         seal.EpochID,
		HolderID:        seal.HolderID,
		Scope:           seal.Scope,
		ExpiresUnixNano: 1,
	}
	return grant.Covers(AuthorityScopeFromDelta(scope), 0)
}

func (m *AuthorityManager) RetirePerasAuthority(ctx context.Context, scopes ...compile.AuthorityScope) error {
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

func (m *AuthorityManager) ownedGrantsForScopes(scopes ...compile.AuthorityScope) []AuthorityGrant {
	if m == nil || m.table == nil || strings.TrimSpace(m.holderID) == "" {
		return nil
	}
	now := m.now()
	snapshot := m.table.Snapshot()
	out := make([]AuthorityGrant, 0, len(snapshot))
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

func grantMatchesRetireScope(grant AuthorityGrant, scope compile.AuthorityScope, now time.Time) bool {
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

func (m *AuthorityManager) installResponse(resp *coordpb.ApplyPerasAuthorityResponse) error {
	if m == nil || m.table == nil {
		return ErrTableRequired
	}
	if resp == nil {
		return ErrInvalidResponse
	}
	grants, err := parsePerasAuthorityGrants(resp.GetActiveGrants())
	if err != nil {
		return err
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED {
		grant := metawire.RootPerasAuthorityGrantFromProto(resp.GetGrant())
		if !grant.Valid() {
			return ErrInvalidResponse
		}
		grants = appendPerasGrantIfMissing(grants, grant)
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD && len(grants) == 0 {
		return ErrInvalidResponse
	}
	if resp.GetStatus() == metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_UNSPECIFIED {
		return ErrInvalidResponse
	}
	return m.table.Replace(grants)
}

func parsePerasAuthorityGrants(in []*metapb.RootPerasAuthorityGrant) ([]AuthorityGrant, error) {
	out := make([]AuthorityGrant, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, pbGrant := range in {
		grant := metawire.RootPerasAuthorityGrantFromProto(pbGrant)
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

func appendPerasGrantIfMissing(grants []AuthorityGrant, grant AuthorityGrant) []AuthorityGrant {
	for _, current := range grants {
		if current.GrantID == grant.GrantID {
			return grants
		}
	}
	return append(grants, grant)
}
