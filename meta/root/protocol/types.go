package protocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"strings"
)

// Cursor identifies one committed position in the metadata-root log.
type Cursor struct {
	Term  uint64
	Index uint64
}

type DutyID string

const (
	DutyAllocID         DutyID = "alloc_id"
	DutyTSO             DutyID = "tso"
	DutyRegionLookup    DutyID = "region_lookup"
	DutyLeaseStart      DutyID = "lease_start"
	DutyFSMetaNamespace DutyID = "fsmeta_namespace"
	DutyQuotaFence      DutyID = "quota_fence"
)

type DutyScopeKind uint8

const (
	DutyScopeUnspecified DutyScopeKind = iota
	DutyScopeGlobal
	DutyScopeMount
	DutyScopeSubtree
	DutyScopeRegionRange
)

type DutyScope struct {
	Kind        DutyScopeKind
	MountID     string
	SubtreeRoot uint64
	StartKey    []byte
	EndKey      []byte
}

type DutyBoundKind uint8

const (
	DutyBoundUnspecified DutyBoundKind = iota
	DutyBoundMonotone
	DutyBoundVersion
	DutyBoundBudget
	DutyBoundEpoch
)

type DutyBound struct {
	Kind                      DutyBoundKind
	MonotoneUpper             uint64
	VersionRootToken          AuthorityRootToken
	DescriptorRevisionCeiling uint64
	MaxRootLag                uint64
	Budget                    uint64
	Epoch                     uint64
}

type DutyGrant struct {
	DutyID DutyID
	Scope  DutyScope
	Bound  DutyBound
}

type AuthorityGrant struct {
	GrantID                string
	HolderID               string
	Era                    uint64
	ExpiresUnixNano        int64
	IssuedAt               Cursor
	IssuedRootToken        AuthorityRootToken
	Duties                 []DutyGrant
	PredecessorRetirements []GrantRetirement
}

type GrantRetirementMode uint8

const (
	GrantRetirementUnspecified GrantRetirementMode = iota
	GrantRetirementSealedExact
	GrantRetirementExpiredBound
)

type GrantRetirement struct {
	GrantID            string
	HolderID           string
	Era                uint64
	Mode               GrantRetirementMode
	Bounds             []DutyGrant
	RetiredAt          Cursor
	InheritedByGrantID string
}

type GrantInheritance struct {
	PredecessorGrantID string
	SuccessorGrantID   string
	InheritedAt        Cursor
}

type GrantCertificate struct {
	Grant       AuthorityGrant
	SignerKeyID string
	Signature   []byte
}

type AuthorityUsage struct {
	DutyID DutyID
	Scope  DutyScope
	Usage  DutyBound
}

type AuthorityEvidence struct {
	Certificate             GrantCertificate
	Usage                   AuthorityUsage
	ObservedRetirements     []GrantRetirement
	ObservedRetiredEraFloor uint64
}

type GrantAct uint8

const (
	GrantActUnknown GrantAct = iota
	GrantActIssue
	GrantActSeal
	GrantActRetireExpired
	GrantActInherit
)

type GrantCommand struct {
	Kind                GrantAct
	HolderID            string
	GrantID             string
	ExpiresUnixNano     int64
	NowUnixNano         int64
	RequestedDuties     []DutyGrant
	ExactUsages         []AuthorityUsage
	PredecessorGrantIDs []string
}

const GrantSignerKeyID = "nokv-eunomia-root-ed25519-v1"

var rootGrantSigningKey = func() ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("nokv:eunomia:root-issued-bounded-authority-grant:v1"))
	return ed25519.NewKeyFromSeed(seed[:])
}()

func SignGrantBytes(payload []byte) []byte {
	return ed25519.Sign(rootGrantSigningKey, payload)
}

func VerifyGrantBytes(payload, signature []byte) bool {
	publicKey := rootGrantSigningKey.Public().(ed25519.PublicKey)
	return ed25519.Verify(publicKey, payload, signature)
}

func NewGlobalMonotoneDuty(duty DutyID, upper uint64) DutyGrant {
	return DutyGrant{
		DutyID: duty,
		Scope:  DutyScope{Kind: DutyScopeGlobal},
		Bound:  DutyBound{Kind: DutyBoundMonotone, MonotoneUpper: upper},
	}
}

func NewGlobalVersionDuty(duty DutyID, token AuthorityRootToken, descriptorRevisionCeiling, maxRootLag uint64) DutyGrant {
	return DutyGrant{
		DutyID: duty,
		Scope:  DutyScope{Kind: DutyScopeGlobal},
		Bound: DutyBound{
			Kind:                      DutyBoundVersion,
			VersionRootToken:          token,
			DescriptorRevisionCeiling: descriptorRevisionCeiling,
			MaxRootLag:                maxRootLag,
		},
	}
}

func (g AuthorityGrant) Present() bool {
	return strings.TrimSpace(g.GrantID) != "" && strings.TrimSpace(g.HolderID) != "" && g.Era != 0
}

func (g AuthorityGrant) ActiveAt(nowUnixNano int64) bool {
	return g.Present() && g.ExpiresUnixNano > nowUnixNano
}

func (g AuthorityGrant) Duty(duty DutyID) (DutyGrant, bool) {
	for _, entry := range g.Duties {
		if entry.DutyID == duty {
			return entry, true
		}
	}
	return DutyGrant{}, false
}

func (r GrantRetirement) Present() bool {
	return strings.TrimSpace(r.GrantID) != "" && r.Era != 0 && r.Mode != GrantRetirementUnspecified
}

func DutyName(duty DutyID) string {
	if strings.TrimSpace(string(duty)) == "" {
		return "unspecified"
	}
	return string(duty)
}

type AuthorityRootToken struct {
	Term     uint64
	Index    uint64
	Revision uint64
}

const (
	AuthorityEraAttached   uint64 = 0
	AuthorityEraSuppressed uint64 = ^uint64(0)
)
