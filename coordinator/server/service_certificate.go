package server

import (
	"context"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	eunomia "github.com/feichai0017/NoKV/meta/root/protocol/eunomia"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) authorityCertificate(ctx context.Context, mandate uint32, consumedFrontier, descriptorRevision uint64) (*coordpb.AuthorityCertificate, error) {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	snapshot, err := s.currentRootSnapshotCoveringAuthority(mandate, consumedFrontier, descriptorRevision)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "load rooted authority certificate snapshot: "+err.Error())
	}
	lease := snapshot.Tenure
	if lease.Era == rootproto.MandateWitnessEraAttached || lease.Era == rootproto.MandateWitnessEraSuppressed {
		return nil, nil
	}
	frontiers := authorityFrontiersFromRootSnapshot(snapshot)
	legacyDigest := ""
	if snapshot.Legacy.Present() {
		legacyDigest = rootstate.DigestOfLegacy(snapshot.Legacy)
	}
	cert, err := rootproto.NewAuthorityCertificate(
		lease.HolderID,
		lease.Era,
		lease.Mandate,
		lease.ExpiresUnixNano,
		rootproto.AuthorityRootToken{
			Term:     snapshot.RootToken.Cursor.Term,
			Index:    snapshot.RootToken.Cursor.Index,
			Revision: snapshot.RootToken.Revision,
		},
		lease.LineageDigest,
		snapshot.Legacy.Era,
		legacyDigest,
		maxUint64(descriptorRevision, rootstate.MaxDescriptorRevision(snapshot.Descriptors)),
		frontiers,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return authorityCertificateToProto(cert), nil
}

func (s *Service) currentRootSnapshotCoveringAuthority(mandate uint32, consumedFrontier, descriptorRevision uint64) (rootview.Snapshot, error) {
	snapshot, err := s.currentRootSnapshot()
	if err != nil {
		return rootview.Snapshot{}, err
	}
	if authoritySnapshotCovers(snapshot, mandate, consumedFrontier, descriptorRevision) {
		return snapshot, nil
	}
	snapshot, err = s.reloadRootedView(true)
	if err != nil {
		return rootview.Snapshot{}, err
	}
	if authoritySnapshotCovers(snapshot, mandate, consumedFrontier, descriptorRevision) {
		return snapshot, nil
	}
	return rootview.Snapshot{}, status.Errorf(
		codes.FailedPrecondition,
		"rooted authority certificate frontier not covered: duty=%s required_frontier=%d rooted_frontier=%d required_descriptor_revision=%d rooted_descriptor_revision=%d",
		rootproto.MandateName(mandate),
		consumedFrontier,
		authorityFrontiersFromRootSnapshot(snapshot).Frontier(mandate),
		descriptorRevision,
		rootstate.MaxDescriptorRevision(snapshot.Descriptors),
	)
}

func authoritySnapshotCovers(snapshot rootview.Snapshot, mandate uint32, consumedFrontier, descriptorRevision uint64) bool {
	if descriptorRevision > rootstate.MaxDescriptorRevision(snapshot.Descriptors) {
		return false
	}
	if mandate == 0 || consumedFrontier == 0 {
		return true
	}
	return authorityFrontiersFromRootSnapshot(snapshot).Frontier(mandate) >= consumedFrontier
}

func authorityFrontiersFromRootSnapshot(snapshot rootview.Snapshot) rootproto.MandateFrontiers {
	return eunomia.Frontiers(rootstate.State{
		IDFence:  snapshot.Allocator.IDCurrent,
		TSOFence: snapshot.Allocator.TSCurrent,
	}, rootstate.MaxDescriptorRevision(snapshot.Descriptors))
}

func authorityCertificateToProto(cert rootproto.AuthorityCertificate) *coordpb.AuthorityCertificate {
	if !cert.Present() {
		return nil
	}
	frontiers := cert.InheritedFrontiers.Entries()
	out := &coordpb.AuthorityCertificate{
		HolderId:           cert.HolderID,
		Era:                cert.Era,
		Mandate:            cert.Mandate,
		ExpiresUnixNano:    cert.ExpiresUnixNano,
		IssuedRootToken:    &coordpb.RootToken{Term: cert.IssuedRootToken.Term, Index: cert.IssuedRootToken.Index, Revision: cert.IssuedRootToken.Revision},
		LineageDigest:      cert.LineageDigest,
		ObservedLegacyEra:  cert.ObservedLegacyEra,
		LegacyDigest:       cert.LegacyDigest,
		DescriptorRevision: cert.DescriptorRevision,
		InheritedFrontiers: make([]*coordpb.AuthorityFrontier, 0, len(frontiers)),
	}
	for _, frontier := range frontiers {
		out.InheritedFrontiers = append(out.InheritedFrontiers, &coordpb.AuthorityFrontier{
			Mandate:  frontier.Mandate,
			Frontier: frontier.Frontier,
		})
	}
	return out
}
