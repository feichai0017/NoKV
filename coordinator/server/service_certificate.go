package server

import (
	"context"
	"fmt"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Service) authorityEvidence(ctx context.Context, duty rootproto.DutyID, usage rootproto.DutyBound) (*metapb.RootAuthorityEvidence, error) {
	if s == nil || !s.coordinatorGrantEnabled() {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	snapshot, err := s.currentRootSnapshotCoveringAuthority(duty, usage)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "load rooted authority evidence snapshot: "+err.Error())
	}
	grant := snapshot.ActiveGrant
	cert, err := grantCertificate(grant)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	observedFloor := uint64(0)
	for _, retirement := range snapshot.RetiredGrants {
		if retirement.Era > observedFloor {
			observedFloor = retirement.Era
		}
	}
	return metawire.RootAuthorityEvidenceToProto(rootproto.AuthorityEvidence{
		Certificate: cert,
		Usage: rootproto.AuthorityUsage{
			DutyID: duty,
			Scope:  rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			Usage:  usage,
		},
		ObservedRetirements:     snapshot.RetiredGrants,
		ObservedRetiredEraFloor: observedFloor,
	}), nil
}

func (s *Service) currentRootSnapshotCoveringAuthority(duty rootproto.DutyID, usage rootproto.DutyBound) (rootview.Snapshot, error) {
	snapshot, err := s.currentRootSnapshot()
	if err != nil {
		return rootview.Snapshot{}, err
	}
	if authoritySnapshotCovers(snapshot, duty, usage) {
		return snapshot, nil
	}
	snapshot, err = s.reloadRootedView(true)
	if err != nil {
		return rootview.Snapshot{}, err
	}
	if authoritySnapshotCovers(snapshot, duty, usage) {
		return snapshot, nil
	}
	return rootview.Snapshot{}, status.Errorf(
		codes.FailedPrecondition,
		"rooted grant does not cover duty=%s grant_id=%s era=%d",
		rootproto.DutyName(duty),
		snapshot.ActiveGrant.GrantID,
		snapshot.ActiveGrant.Era,
	)
}

func authoritySnapshotCovers(snapshot rootview.Snapshot, duty rootproto.DutyID, usage rootproto.DutyBound) bool {
	grant := snapshot.ActiveGrant
	if !grant.Present() {
		return false
	}
	dutyGrant, ok := grant.Duty(duty)
	if !ok {
		return false
	}
	return authorityBoundCovers(dutyGrant.Bound, usage)
}

func authorityBoundCovers(grant, usage rootproto.DutyBound) bool {
	if grant.Kind != usage.Kind {
		return false
	}
	switch usage.Kind {
	case rootproto.DutyBoundMonotone:
		return usage.MonotoneUpper <= grant.MonotoneUpper
	case rootproto.DutyBoundVersion:
		return usage.DescriptorRevisionCeiling <= grant.DescriptorRevisionCeiling &&
			usage.MaxRootLag <= grant.MaxRootLag
	case rootproto.DutyBoundBudget:
		return usage.Budget <= grant.Budget
	case rootproto.DutyBoundEpoch:
		return usage.Epoch <= grant.Epoch
	default:
		return false
	}
}

func grantCertificate(grant rootproto.AuthorityGrant) (rootproto.GrantCertificate, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	if err != nil {
		return rootproto.GrantCertificate{}, err
	}
	signature := rootproto.SignGrantBytes(payload)
	if len(signature) == 0 {
		return rootproto.GrantCertificate{}, fmt.Errorf("root grant signing key is not configured")
	}
	return rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   signature,
	}, nil
}
