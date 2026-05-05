package server

import (
	"reflect"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authorityProof struct {
	Grant                   rootproto.AuthorityGrant
	ObservedRetiredEraFloor uint64
	Evidence                *metapb.RootAuthorityEvidence
}

func (a dutyAdmission) authorityEvidence(usage rootproto.DutyBound) (authorityProof, error) {
	if !a.grant.Present() {
		return authorityProof{}, nil
	}
	if !grantCertificateMatches(a.certificate, a.grant) {
		return authorityProof{}, status.Error(codes.Internal, "root-issued grant certificate is missing or stale")
	}
	if a.grant.ExpiresUnixNano <= a.servedUnixNano {
		return authorityProof{}, status.Error(codes.FailedPrecondition, "admitted grant expired before evidence generation")
	}
	scope := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	dutyGrant, ok := a.grant.DutyFor(a.duty, scope)
	if !ok || !rootproto.DutyBoundCovers(dutyGrant.Bound, usage) {
		return authorityProof{}, status.Errorf(codes.FailedPrecondition, "admitted grant does not cover duty=%s grant_id=%s era=%d", rootproto.DutyName(a.duty), a.grant.GrantID, a.grant.Era)
	}
	evidence := metawire.RootAuthorityEvidenceToProto(rootproto.AuthorityEvidence{
		Certificate: a.certificate,
		Usage: rootproto.AuthorityUsage{
			DutyID: a.duty,
			Scope:  scope,
			Usage:  usage,
		},
		ObservedRetirements:     a.retirements,
		ObservedRetiredEraFloor: a.retiredEraFloor,
		ServedUnixNano:          a.servedUnixNano,
	})
	return authorityProof{
		Grant:                   a.grant,
		ObservedRetiredEraFloor: a.retiredEraFloor,
		Evidence:                evidence,
	}, nil
}

func grantCertificateMatches(cert rootproto.GrantCertificate, grant rootproto.AuthorityGrant) bool {
	return grantCertificatePresent(cert) &&
		grant.Present() &&
		reflect.DeepEqual(cert.Grant, grant)
}

func grantCertificateCoversGrant(cert rootproto.GrantCertificate, grant rootproto.AuthorityGrant) bool {
	if !grantCertificatePresent(cert) || !grant.Present() {
		return false
	}
	certGrant := cert.Grant
	return certGrant.GrantID == grant.GrantID &&
		certGrant.HolderID == grant.HolderID &&
		certGrant.Era == grant.Era &&
		certGrant.ExpiresUnixNano == grant.ExpiresUnixNano &&
		reflect.DeepEqual(certGrant.Duties, grant.Duties) &&
		reflect.DeepEqual(certGrant.PredecessorRetirements, grant.PredecessorRetirements)
}

func grantCertificatePresent(cert rootproto.GrantCertificate) bool {
	return cert.Grant.Present() &&
		cert.SignerKeyID != "" &&
		len(cert.Signature) != 0
}
