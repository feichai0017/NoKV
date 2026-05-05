package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
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
	ServedUnixNano          int64
}

type AuthorityVerifierKey struct {
	ClusterID string
	DutyID    DutyID
	Scope     DutyScope
}

type AuthorityVerifierState struct {
	Key                   AuthorityVerifierKey
	MaxSeenEra            uint64
	RetiredEraFloor       uint64
	MaxRootToken          AuthorityRootToken
	MaxDescriptorRevision uint64
	MaxFrontier           DutyBound
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

const (
	GrantSignerKeyID              = "nokv-eunomia-root-ed25519-v1"
	GrantSigningPrivateKeyEnv     = "NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY"
	GrantVerificationPublicKeyEnv = "NOKV_EUNOMIA_GRANT_VERIFY_PUBLIC_KEY"
	GrantAllowEphemeralKeysEnv    = "NOKV_EUNOMIA_GRANT_ALLOW_EPHEMERAL_KEYS"
)

type grantKeyMaterial struct {
	private ed25519.PrivateKey
	public  ed25519.PublicKey
}

var rootGrantKeys = loadGrantKeyMaterial()

func SignGrantBytes(payload []byte) []byte {
	if len(rootGrantKeys.private) != ed25519.PrivateKeySize {
		return nil
	}
	return ed25519.Sign(rootGrantKeys.private, payload)
}

func VerifyGrantBytes(payload, signature []byte) bool {
	if len(rootGrantKeys.public) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(rootGrantKeys.public, payload, signature)
}

func loadGrantKeyMaterial() grantKeyMaterial {
	private := parseEd25519PrivateKeyEnv(GrantSigningPrivateKeyEnv)
	public := parseEd25519PublicKeyEnv(GrantVerificationPublicKeyEnv)
	if private == nil && public == nil {
		if !allowEphemeralGrantKeys() {
			panic("meta/root/protocol: " + GrantSigningPrivateKeyEnv +
				" and/or " + GrantVerificationPublicKeyEnv +
				" must be provided; set " + GrantAllowEphemeralKeysEnv +
				"=1 only for local dev/test")
		}
		generatedPublic, generatedPrivate, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			panic("meta/root/protocol: generate local grant signing key: " + err.Error())
		}
		return grantKeyMaterial{private: generatedPrivate, public: generatedPublic}
	}
	if public == nil && private != nil {
		public = private.Public().(ed25519.PublicKey)
	}
	return grantKeyMaterial{private: private, public: public}
}

func allowEphemeralGrantKeys() bool {
	if strings.TrimSpace(os.Getenv(GrantAllowEphemeralKeysEnv)) != "" {
		return true
	}
	return len(os.Args) > 0 && strings.HasSuffix(os.Args[0], ".test")
}

func parseEd25519PrivateKeyEnv(name string) ed25519.PrivateKey {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	decoded := mustDecodeGrantKeyEnv(name, raw)
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded)
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(decoded)
	default:
		panic(name + " must be base64 Ed25519 seed or private key")
	}
}

func parseEd25519PublicKeyEnv(name string) ed25519.PublicKey {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	decoded := mustDecodeGrantKeyEnv(name, raw)
	if len(decoded) != ed25519.PublicKeySize {
		panic(name + " must be base64 Ed25519 public key")
	}
	return ed25519.PublicKey(decoded)
}

func mustDecodeGrantKeyEnv(name, raw string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		panic(name + " must be base64: " + err.Error())
	}
	return decoded
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

func (g AuthorityGrant) DutyFor(duty DutyID, scope DutyScope) (DutyGrant, bool) {
	for _, entry := range g.Duties {
		if entry.DutyID == duty && ScopeEqual(entry.Scope, scope) {
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
