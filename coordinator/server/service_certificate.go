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
	snapshot, err := s.currentRootSnapshot()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "load rooted authority certificate snapshot: "+err.Error())
	}
	lease := snapshot.Tenure
	if lease.Era == rootproto.MandateWitnessEraAttached || lease.Era == rootproto.MandateWitnessEraSuppressed {
		return nil, nil
	}
	frontiers := s.authorityFrontiersForCertificate(snapshot, mandate, consumedFrontier, descriptorRevision)
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

func (s *Service) authorityFrontiersForCertificate(snapshot rootview.Snapshot, mandate uint32, consumedFrontier, descriptorRevision uint64) rootproto.MandateFrontiers {
	frontiers := eunomia.Frontiers(rootstate.State{
		IDFence:  snapshot.Allocator.IDCurrent,
		TSOFence: snapshot.Allocator.TSCurrent,
	}, rootstate.MaxDescriptorRevision(snapshot.Descriptors))
	s.allocMu.Lock()
	frontiers = eunomia.Frontiers(rootstate.State{
		IDFence:  maxUint64(snapshot.Allocator.IDCurrent, s.currentIDFenceLocked()),
		TSOFence: maxUint64(snapshot.Allocator.TSCurrent, s.currentTSOFenceLocked()),
	}, maxUint64(descriptorRevision, rootstate.MaxDescriptorRevision(snapshot.Descriptors)))
	s.allocMu.Unlock()
	if mandate != 0 && consumedFrontier > frontiers.Frontier(mandate) {
		frontiers = frontiers.WithFrontier(mandate, consumedFrontier)
	}
	return frontiers
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
