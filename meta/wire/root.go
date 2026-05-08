package wire

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func RootCursorToProto(cursor rootproto.Cursor) *metapb.RootCursor {
	return &metapb.RootCursor{Term: cursor.Term, Index: cursor.Index}
}

func RootCursorFromProto(pbCursor *metapb.RootCursor) rootproto.Cursor {
	if pbCursor == nil {
		return rootproto.Cursor{}
	}
	return rootproto.Cursor{Term: pbCursor.Term, Index: pbCursor.Index}
}

func RootStateToProto(state rootstate.State) *metapb.RootState {
	return &metapb.RootState{
		ClusterEpoch:      state.ClusterEpoch,
		MembershipEpoch:   state.MembershipEpoch,
		LastCommitted:     RootCursorToProto(state.LastCommitted),
		IdFence:           state.IDFence,
		TsoFence:          state.TSOFence,
		ActiveGrants:      RootAuthorityGrantsToProto(state.ActiveGrants),
		RetiredGrants:     RootGrantRetirementsToProto(state.RetiredGrants),
		GrantInheritances: RootGrantInheritancesToProto(state.GrantInheritances),
		RetiredEraFloor:   state.RetiredEraFloor,
	}
}

func RootStateFromProto(pbState *metapb.RootState) rootstate.State {
	if pbState == nil {
		return rootstate.State{}
	}
	return rootstate.State{
		ClusterEpoch:      pbState.ClusterEpoch,
		MembershipEpoch:   pbState.MembershipEpoch,
		LastCommitted:     RootCursorFromProto(pbState.LastCommitted),
		IDFence:           pbState.IdFence,
		TSOFence:          pbState.TsoFence,
		ActiveGrants:      RootAuthorityGrantsFromProto(pbState.GetActiveGrants()),
		RetiredGrants:     RootGrantRetirementsFromProto(pbState.GetRetiredGrants()),
		GrantInheritances: RootGrantInheritancesFromProto(pbState.GetGrantInheritances()),
		RetiredEraFloor:   pbState.GetRetiredEraFloor(),
	}
}

func RootAuthorityGrantToProto(grant rootproto.AuthorityGrant) *metapb.RootAuthorityGrant {
	if !grant.Present() {
		return nil
	}
	return &metapb.RootAuthorityGrant{
		GrantId:                grant.GrantID,
		HolderId:               grant.HolderID,
		Era:                    grant.Era,
		ExpiresUnixNano:        grant.ExpiresUnixNano,
		IssuedAt:               RootCursorToProto(grant.IssuedAt),
		IssuedRootToken:        RootTailTokenFromAuthorityToken(grant.IssuedRootToken),
		Duties:                 RootDutyGrantsToProto(grant.Duties),
		PredecessorRetirements: RootGrantRetirementsToProto(grant.PredecessorRetirements),
	}
}

func RootAuthorityGrantFromProto(grant *metapb.RootAuthorityGrant) rootproto.AuthorityGrant {
	if grant == nil {
		return rootproto.AuthorityGrant{}
	}
	return rootproto.AuthorityGrant{
		GrantID:                grant.GetGrantId(),
		HolderID:               grant.GetHolderId(),
		Era:                    grant.GetEra(),
		ExpiresUnixNano:        grant.GetExpiresUnixNano(),
		IssuedAt:               RootCursorFromProto(grant.GetIssuedAt()),
		IssuedRootToken:        AuthorityTokenFromRootTailToken(grant.GetIssuedRootToken()),
		Duties:                 RootDutyGrantsFromProto(grant.GetDuties()),
		PredecessorRetirements: RootGrantRetirementsFromProto(grant.GetPredecessorRetirements()),
	}
}

func RootAuthorityGrantsToProto(grants []rootproto.AuthorityGrant) []*metapb.RootAuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]*metapb.RootAuthorityGrant, 0, len(grants))
	for _, grant := range grants {
		if pbGrant := RootAuthorityGrantToProto(grant); pbGrant != nil {
			out = append(out, pbGrant)
		}
	}
	return out
}

func RootAuthorityGrantsFromProto(grants []*metapb.RootAuthorityGrant) []rootproto.AuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.AuthorityGrant, 0, len(grants))
	for _, grant := range grants {
		if parsed := RootAuthorityGrantFromProto(grant); parsed.Present() {
			out = append(out, parsed)
		}
	}
	return out
}

func RootGrantRetirementToProto(retirement rootproto.GrantRetirement) *metapb.RootGrantRetirement {
	if !retirement.Present() {
		return nil
	}
	return &metapb.RootGrantRetirement{
		GrantId:            retirement.GrantID,
		HolderId:           retirement.HolderID,
		Era:                retirement.Era,
		Mode:               RootGrantRetirementModeToProto(retirement.Mode),
		Bounds:             RootDutyGrantsToProto(retirement.Bounds),
		RetiredAt:          RootCursorToProto(retirement.RetiredAt),
		InheritedByGrantId: retirement.InheritedByGrantID,
	}
}

func RootGrantRetirementFromProto(retirement *metapb.RootGrantRetirement) rootproto.GrantRetirement {
	if retirement == nil {
		return rootproto.GrantRetirement{}
	}
	return rootproto.GrantRetirement{
		GrantID:            retirement.GetGrantId(),
		HolderID:           retirement.GetHolderId(),
		Era:                retirement.GetEra(),
		Mode:               RootGrantRetirementModeFromProto(retirement.GetMode()),
		Bounds:             RootDutyGrantsFromProto(retirement.GetBounds()),
		RetiredAt:          RootCursorFromProto(retirement.GetRetiredAt()),
		InheritedByGrantID: retirement.GetInheritedByGrantId(),
	}
}

func RootGrantRetirementsToProto(retirements []rootproto.GrantRetirement) []*metapb.RootGrantRetirement {
	if len(retirements) == 0 {
		return nil
	}
	out := make([]*metapb.RootGrantRetirement, 0, len(retirements))
	for _, retirement := range retirements {
		if pbRetirement := RootGrantRetirementToProto(retirement); pbRetirement != nil {
			out = append(out, pbRetirement)
		}
	}
	return out
}

func RootGrantRetirementsFromProto(retirements []*metapb.RootGrantRetirement) []rootproto.GrantRetirement {
	if len(retirements) == 0 {
		return nil
	}
	out := make([]rootproto.GrantRetirement, 0, len(retirements))
	for _, retirement := range retirements {
		if parsed := RootGrantRetirementFromProto(retirement); parsed.Present() {
			out = append(out, parsed)
		}
	}
	return out
}

func RootGrantInheritanceToProto(inheritance rootproto.GrantInheritance) *metapb.RootGrantInheritance {
	if inheritance.PredecessorGrantID == "" || inheritance.SuccessorGrantID == "" {
		return nil
	}
	return &metapb.RootGrantInheritance{
		PredecessorGrantId: inheritance.PredecessorGrantID,
		SuccessorGrantId:   inheritance.SuccessorGrantID,
		InheritedAt:        RootCursorToProto(inheritance.InheritedAt),
	}
}

func RootGrantInheritanceFromProto(inheritance *metapb.RootGrantInheritance) rootproto.GrantInheritance {
	if inheritance == nil {
		return rootproto.GrantInheritance{}
	}
	return rootproto.GrantInheritance{
		PredecessorGrantID: inheritance.GetPredecessorGrantId(),
		SuccessorGrantID:   inheritance.GetSuccessorGrantId(),
		InheritedAt:        RootCursorFromProto(inheritance.GetInheritedAt()),
	}
}

func RootGrantInheritancesToProto(inheritances []rootproto.GrantInheritance) []*metapb.RootGrantInheritance {
	if len(inheritances) == 0 {
		return nil
	}
	out := make([]*metapb.RootGrantInheritance, 0, len(inheritances))
	for _, inheritance := range inheritances {
		if pbInheritance := RootGrantInheritanceToProto(inheritance); pbInheritance != nil {
			out = append(out, pbInheritance)
		}
	}
	return out
}

func RootGrantInheritancesFromProto(inheritances []*metapb.RootGrantInheritance) []rootproto.GrantInheritance {
	if len(inheritances) == 0 {
		return nil
	}
	out := make([]rootproto.GrantInheritance, 0, len(inheritances))
	for _, inheritance := range inheritances {
		parsed := RootGrantInheritanceFromProto(inheritance)
		if parsed.PredecessorGrantID != "" && parsed.SuccessorGrantID != "" {
			out = append(out, parsed)
		}
	}
	return out
}

func RootEunomiaStateToProto(state rootstate.EunomiaState) *metapb.RootEunomiaState {
	return &metapb.RootEunomiaState{
		ActiveGrants:      RootAuthorityGrantsToProto(state.ActiveGrants),
		RetiredGrants:     RootGrantRetirementsToProto(state.RetiredGrants),
		GrantInheritances: RootGrantInheritancesToProto(state.GrantInheritances),
		RetiredEraFloor:   state.RetiredEraFloor,
	}
}

func RootEunomiaStateFromProto(state *metapb.RootEunomiaState) rootstate.EunomiaState {
	if state == nil {
		return rootstate.EunomiaState{}
	}
	return rootstate.EunomiaState{
		ActiveGrants:      RootAuthorityGrantsFromProto(state.GetActiveGrants()),
		RetiredGrants:     RootGrantRetirementsFromProto(state.GetRetiredGrants()),
		GrantInheritances: RootGrantInheritancesFromProto(state.GetGrantInheritances()),
		RetiredEraFloor:   state.GetRetiredEraFloor(),
	}
}

func RootGrantCommandToProto(cmd rootproto.GrantCommand) *metapb.RootGrantCommand {
	return &metapb.RootGrantCommand{
		Kind:                RootGrantActToProto(cmd.Kind),
		HolderId:            cmd.HolderID,
		GrantId:             cmd.GrantID,
		ExpiresUnixNano:     cmd.ExpiresUnixNano,
		NowUnixNano:         cmd.NowUnixNano,
		RequestedDuties:     RootDutyGrantsToProto(cmd.RequestedDuties),
		ExactUsages:         RootAuthorityUsagesToProto(cmd.ExactUsages),
		PredecessorGrantIds: append([]string(nil), cmd.PredecessorGrantIDs...),
	}
}

func RootGrantCommandFromProto(cmd *metapb.RootGrantCommand) rootproto.GrantCommand {
	if cmd == nil {
		return rootproto.GrantCommand{}
	}
	return rootproto.GrantCommand{
		Kind:                RootGrantActFromProto(cmd.GetKind()),
		HolderID:            cmd.GetHolderId(),
		GrantID:             cmd.GetGrantId(),
		ExpiresUnixNano:     cmd.GetExpiresUnixNano(),
		NowUnixNano:         cmd.GetNowUnixNano(),
		RequestedDuties:     RootDutyGrantsFromProto(cmd.GetRequestedDuties()),
		ExactUsages:         RootAuthorityUsagesFromProto(cmd.GetExactUsages()),
		PredecessorGrantIDs: append([]string(nil), cmd.GetPredecessorGrantIds()...),
	}
}

func rootEventSnapshotEpochToProto(epoch *rootevent.SnapshotEpoch) *metapb.RootSnapshotEpoch {
	if epoch == nil {
		return nil
	}
	return &metapb.RootSnapshotEpoch{
		SnapshotId:  epoch.SnapshotID,
		Mount:       epoch.Mount,
		RootInode:   epoch.RootInode,
		ReadVersion: epoch.ReadVersion,
		PublishedAt: RootCursorToProto(epoch.PublishedAt),
	}
}

func rootEventSnapshotEpochFromProto(epoch *metapb.RootSnapshotEpoch) *rootevent.SnapshotEpoch {
	if epoch == nil {
		return nil
	}
	return &rootevent.SnapshotEpoch{
		SnapshotID:  epoch.GetSnapshotId(),
		Mount:       epoch.GetMount(),
		RootInode:   epoch.GetRootInode(),
		ReadVersion: epoch.GetReadVersion(),
		PublishedAt: RootCursorFromProto(epoch.GetPublishedAt()),
	}
}

func rootEventMountToProto(mount *rootevent.Mount) *metapb.RootMount {
	if mount == nil {
		return nil
	}
	return &metapb.RootMount{
		MountId:       mount.MountID,
		RootInode:     mount.RootInode,
		SchemaVersion: mount.SchemaVersion,
		RegisteredAt:  RootCursorToProto(mount.RegisteredAt),
		RetiredAt:     RootCursorToProto(mount.RetiredAt),
	}
}

func rootEventMountFromProto(mount *metapb.RootMount) *rootevent.Mount {
	if mount == nil {
		return nil
	}
	return &rootevent.Mount{
		MountID:       mount.GetMountId(),
		RootInode:     mount.GetRootInode(),
		SchemaVersion: mount.GetSchemaVersion(),
		RegisteredAt:  RootCursorFromProto(mount.GetRegisteredAt()),
		RetiredAt:     RootCursorFromProto(mount.GetRetiredAt()),
	}
}

func rootEventSubtreeAuthorityToProto(subtree *rootevent.SubtreeAuthority) *metapb.RootSubtreeAuthority {
	if subtree == nil {
		return nil
	}
	return &metapb.RootSubtreeAuthority{
		SubtreeId:              subtree.SubtreeID,
		Mount:                  subtree.Mount,
		RootInode:              subtree.RootInode,
		AuthorityId:            subtree.AuthorityID,
		Era:                    subtree.Era,
		Frontier:               subtree.Frontier,
		DeclaredAt:             RootCursorToProto(subtree.DeclaredAt),
		HandoffStartedAt:       RootCursorToProto(subtree.HandoffStartedAt),
		HandoffCompletedAt:     RootCursorToProto(subtree.HandoffCompletedAt),
		PredecessorAuthorityId: subtree.PredecessorAuthorityID,
		PredecessorEra:         subtree.PredecessorEra,
		PredecessorFrontier:    subtree.PredecessorFrontier,
		SuccessorAuthorityId:   subtree.SuccessorAuthorityID,
		SuccessorEra:           subtree.SuccessorEra,
		InheritedFrontier:      subtree.InheritedFrontier,
	}
}

func rootEventSubtreeAuthorityFromProto(subtree *metapb.RootSubtreeAuthority) *rootevent.SubtreeAuthority {
	if subtree == nil {
		return nil
	}
	return &rootevent.SubtreeAuthority{
		SubtreeID:              subtree.GetSubtreeId(),
		Mount:                  subtree.GetMount(),
		RootInode:              subtree.GetRootInode(),
		AuthorityID:            subtree.GetAuthorityId(),
		Era:                    subtree.GetEra(),
		Frontier:               subtree.GetFrontier(),
		DeclaredAt:             RootCursorFromProto(subtree.GetDeclaredAt()),
		HandoffStartedAt:       RootCursorFromProto(subtree.GetHandoffStartedAt()),
		HandoffCompletedAt:     RootCursorFromProto(subtree.GetHandoffCompletedAt()),
		PredecessorAuthorityID: subtree.GetPredecessorAuthorityId(),
		PredecessorEra:         subtree.GetPredecessorEra(),
		PredecessorFrontier:    subtree.GetPredecessorFrontier(),
		SuccessorAuthorityID:   subtree.GetSuccessorAuthorityId(),
		SuccessorEra:           subtree.GetSuccessorEra(),
		InheritedFrontier:      subtree.GetInheritedFrontier(),
	}
}

func rootEventQuotaFenceToProto(fence *rootevent.QuotaFence) *metapb.RootQuotaFence {
	if fence == nil {
		return nil
	}
	return &metapb.RootQuotaFence{
		SubjectId:   fence.SubjectID,
		Mount:       fence.Mount,
		SubtreeRoot: fence.RootInode,
		LimitBytes:  fence.LimitBytes,
		LimitInodes: fence.LimitInodes,
		Era:         fence.Era,
		Frontier:    fence.Frontier,
		UpdatedAt:   RootCursorToProto(fence.UpdatedAt),
	}
}

func rootEventQuotaFenceFromProto(fence *metapb.RootQuotaFence) *rootevent.QuotaFence {
	if fence == nil {
		return nil
	}
	out := &rootevent.QuotaFence{
		SubjectID:   fence.GetSubjectId(),
		Mount:       fence.GetMount(),
		RootInode:   fence.GetSubtreeRoot(),
		LimitBytes:  fence.GetLimitBytes(),
		LimitInodes: fence.GetLimitInodes(),
		Era:         fence.GetEra(),
		Frontier:    fence.GetFrontier(),
		UpdatedAt:   RootCursorFromProto(fence.GetUpdatedAt()),
	}
	if out.SubjectID == "" {
		out.SubjectID = rootevent.QuotaFenceID(out.Mount, out.RootInode)
	}
	return out
}

func RootTailTokenFromAuthorityToken(token rootproto.AuthorityRootToken) *metapb.RootTailToken {
	if token.Term == 0 && token.Index == 0 && token.Revision == 0 {
		return nil
	}
	return &metapb.RootTailToken{
		Cursor:   RootCursorToProto(rootproto.Cursor{Term: token.Term, Index: token.Index}),
		Revision: token.Revision,
	}
}

func AuthorityTokenFromRootTailToken(token *metapb.RootTailToken) rootproto.AuthorityRootToken {
	if token == nil {
		return rootproto.AuthorityRootToken{}
	}
	cursor := RootCursorFromProto(token.GetCursor())
	return rootproto.AuthorityRootToken{Term: cursor.Term, Index: cursor.Index, Revision: token.GetRevision()}
}

func RootDutyScopeToProto(scope rootproto.DutyScope) *metapb.RootDutyScope {
	return &metapb.RootDutyScope{
		Kind:        RootDutyScopeKindToProto(scope.Kind),
		MountId:     scope.MountID,
		SubtreeRoot: scope.SubtreeRoot,
		StartKey:    append([]byte(nil), scope.StartKey...),
		EndKey:      append([]byte(nil), scope.EndKey...),
	}
}

func RootDutyScopeFromProto(scope *metapb.RootDutyScope) rootproto.DutyScope {
	if scope == nil {
		return rootproto.DutyScope{}
	}
	return rootproto.DutyScope{
		Kind:        RootDutyScopeKindFromProto(scope.GetKind()),
		MountID:     scope.GetMountId(),
		SubtreeRoot: scope.GetSubtreeRoot(),
		StartKey:    append([]byte(nil), scope.GetStartKey()...),
		EndKey:      append([]byte(nil), scope.GetEndKey()...),
	}
}

func RootDutyBoundToProto(bound rootproto.DutyBound) *metapb.RootDutyBound {
	switch bound.Kind {
	case rootproto.DutyBoundMonotone:
		return &metapb.RootDutyBound{Bound: &metapb.RootDutyBound_Monotone{Monotone: &metapb.RootMonotoneBound{Upper: bound.MonotoneUpper}}}
	case rootproto.DutyBoundVersion:
		return &metapb.RootDutyBound{Bound: &metapb.RootDutyBound_Version{Version: &metapb.RootVersionBound{
			RootToken:                 RootTailTokenFromAuthorityToken(bound.VersionRootToken),
			DescriptorRevisionCeiling: bound.DescriptorRevisionCeiling,
			MaxRootLag:                bound.MaxRootLag,
		}}}
	case rootproto.DutyBoundBudget:
		return &metapb.RootDutyBound{Bound: &metapb.RootDutyBound_Budget{Budget: &metapb.RootBudgetBound{Budget: bound.Budget}}}
	case rootproto.DutyBoundEpoch:
		return &metapb.RootDutyBound{Bound: &metapb.RootDutyBound_Epoch{Epoch: &metapb.RootEpochBound{Epoch: bound.Epoch}}}
	default:
		return nil
	}
}

func RootDutyBoundFromProto(bound *metapb.RootDutyBound) rootproto.DutyBound {
	if bound == nil {
		return rootproto.DutyBound{}
	}
	if monotone := bound.GetMonotone(); monotone != nil {
		return rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: monotone.GetUpper()}
	}
	if version := bound.GetVersion(); version != nil {
		return rootproto.DutyBound{
			Kind:                      rootproto.DutyBoundVersion,
			VersionRootToken:          AuthorityTokenFromRootTailToken(version.GetRootToken()),
			DescriptorRevisionCeiling: version.GetDescriptorRevisionCeiling(),
			MaxRootLag:                version.GetMaxRootLag(),
		}
	}
	if budget := bound.GetBudget(); budget != nil {
		return rootproto.DutyBound{Kind: rootproto.DutyBoundBudget, Budget: budget.GetBudget()}
	}
	if epoch := bound.GetEpoch(); epoch != nil {
		return rootproto.DutyBound{Kind: rootproto.DutyBoundEpoch, Epoch: epoch.GetEpoch()}
	}
	return rootproto.DutyBound{}
}

func RootDutyGrantToProto(grant rootproto.DutyGrant) *metapb.RootDutyGrant {
	if grant.DutyID == "" {
		return nil
	}
	return &metapb.RootDutyGrant{
		DutyId: string(grant.DutyID),
		Scope:  RootDutyScopeToProto(grant.Scope),
		Bound:  RootDutyBoundToProto(grant.Bound),
	}
}

func RootDutyGrantFromProto(grant *metapb.RootDutyGrant) rootproto.DutyGrant {
	if grant == nil {
		return rootproto.DutyGrant{}
	}
	return rootproto.DutyGrant{
		DutyID: rootproto.DutyID(grant.GetDutyId()),
		Scope:  RootDutyScopeFromProto(grant.GetScope()),
		Bound:  RootDutyBoundFromProto(grant.GetBound()),
	}
}

func RootDutyGrantsToProto(grants []rootproto.DutyGrant) []*metapb.RootDutyGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]*metapb.RootDutyGrant, 0, len(grants))
	for _, grant := range grants {
		if pbGrant := RootDutyGrantToProto(grant); pbGrant != nil {
			out = append(out, pbGrant)
		}
	}
	return out
}

func RootDutyGrantsFromProto(grants []*metapb.RootDutyGrant) []rootproto.DutyGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.DutyGrant, 0, len(grants))
	for _, grant := range grants {
		parsed := RootDutyGrantFromProto(grant)
		if parsed.DutyID != "" {
			out = append(out, parsed)
		}
	}
	return out
}

func RootAuthorityUsageToProto(usage rootproto.AuthorityUsage) *metapb.RootAuthorityUsage {
	if usage.DutyID == "" {
		return nil
	}
	return &metapb.RootAuthorityUsage{
		DutyId: string(usage.DutyID),
		Scope:  RootDutyScopeToProto(usage.Scope),
		Usage:  RootDutyBoundToProto(usage.Usage),
	}
}

func RootAuthorityUsageFromProto(usage *metapb.RootAuthorityUsage) rootproto.AuthorityUsage {
	if usage == nil {
		return rootproto.AuthorityUsage{}
	}
	return rootproto.AuthorityUsage{
		DutyID: rootproto.DutyID(usage.GetDutyId()),
		Scope:  RootDutyScopeFromProto(usage.GetScope()),
		Usage:  RootDutyBoundFromProto(usage.GetUsage()),
	}
}

func RootAuthorityUsagesToProto(usages []rootproto.AuthorityUsage) []*metapb.RootAuthorityUsage {
	if len(usages) == 0 {
		return nil
	}
	out := make([]*metapb.RootAuthorityUsage, 0, len(usages))
	for _, usage := range usages {
		if pbUsage := RootAuthorityUsageToProto(usage); pbUsage != nil {
			out = append(out, pbUsage)
		}
	}
	return out
}

func RootAuthorityUsagesFromProto(usages []*metapb.RootAuthorityUsage) []rootproto.AuthorityUsage {
	if len(usages) == 0 {
		return nil
	}
	out := make([]rootproto.AuthorityUsage, 0, len(usages))
	for _, usage := range usages {
		parsed := RootAuthorityUsageFromProto(usage)
		if parsed.DutyID != "" {
			out = append(out, parsed)
		}
	}
	return out
}

func RootGrantCertificateToProto(cert rootproto.GrantCertificate) *metapb.RootGrantCertificate {
	if !cert.Grant.Present() {
		return nil
	}
	return &metapb.RootGrantCertificate{
		Grant:       RootAuthorityGrantToProto(cert.Grant),
		SignerKeyId: cert.SignerKeyID,
		Signature:   append([]byte(nil), cert.Signature...),
	}
}

func RootGrantCertificateFromProto(cert *metapb.RootGrantCertificate) rootproto.GrantCertificate {
	if cert == nil {
		return rootproto.GrantCertificate{}
	}
	return rootproto.GrantCertificate{
		Grant:       RootAuthorityGrantFromProto(cert.GetGrant()),
		SignerKeyID: cert.GetSignerKeyId(),
		Signature:   append([]byte(nil), cert.GetSignature()...),
	}
}

func RootAuthorityEvidenceToProto(evidence rootproto.AuthorityEvidence) *metapb.RootAuthorityEvidence {
	if !evidence.Certificate.Grant.Present() {
		return nil
	}
	return &metapb.RootAuthorityEvidence{
		Certificate:             RootGrantCertificateToProto(evidence.Certificate),
		Usage:                   RootAuthorityUsageToProto(evidence.Usage),
		ObservedRetirements:     RootGrantRetirementsToProto(evidence.ObservedRetirements),
		ObservedRetiredEraFloor: evidence.ObservedRetiredEraFloor,
		ServedUnixNano:          evidence.ServedUnixNano,
	}
}

func RootAuthorityEvidenceFromProto(evidence *metapb.RootAuthorityEvidence) rootproto.AuthorityEvidence {
	if evidence == nil {
		return rootproto.AuthorityEvidence{}
	}
	return rootproto.AuthorityEvidence{
		Certificate:             RootGrantCertificateFromProto(evidence.GetCertificate()),
		Usage:                   RootAuthorityUsageFromProto(evidence.GetUsage()),
		ObservedRetirements:     RootGrantRetirementsFromProto(evidence.GetObservedRetirements()),
		ObservedRetiredEraFloor: evidence.GetObservedRetiredEraFloor(),
		ServedUnixNano:          evidence.GetServedUnixNano(),
	}
}

func RootDutyScopeKindToProto(kind rootproto.DutyScopeKind) metapb.RootDutyScopeKind {
	switch kind {
	case rootproto.DutyScopeGlobal:
		return metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_GLOBAL
	case rootproto.DutyScopeMount:
		return metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_MOUNT
	case rootproto.DutyScopeSubtree:
		return metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_SUBTREE
	case rootproto.DutyScopeRegionRange:
		return metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_REGION_RANGE
	default:
		return metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_UNSPECIFIED
	}
}

func RootDutyScopeKindFromProto(kind metapb.RootDutyScopeKind) rootproto.DutyScopeKind {
	switch kind {
	case metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_GLOBAL:
		return rootproto.DutyScopeGlobal
	case metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_MOUNT:
		return rootproto.DutyScopeMount
	case metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_SUBTREE:
		return rootproto.DutyScopeSubtree
	case metapb.RootDutyScopeKind_ROOT_DUTY_SCOPE_KIND_REGION_RANGE:
		return rootproto.DutyScopeRegionRange
	default:
		return rootproto.DutyScopeUnspecified
	}
}

func RootGrantRetirementModeToProto(mode rootproto.GrantRetirementMode) metapb.RootGrantRetirementMode {
	switch mode {
	case rootproto.GrantRetirementSealedExact:
		return metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_SEALED_EXACT
	case rootproto.GrantRetirementExpiredBound:
		return metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_EXPIRED_BOUND
	default:
		return metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_UNSPECIFIED
	}
}

func RootGrantRetirementModeFromProto(mode metapb.RootGrantRetirementMode) rootproto.GrantRetirementMode {
	switch mode {
	case metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_SEALED_EXACT:
		return rootproto.GrantRetirementSealedExact
	case metapb.RootGrantRetirementMode_ROOT_GRANT_RETIREMENT_MODE_EXPIRED_BOUND:
		return rootproto.GrantRetirementExpiredBound
	default:
		return rootproto.GrantRetirementUnspecified
	}
}

func RootGrantActToProto(kind rootproto.GrantAct) metapb.RootGrantAct {
	switch kind {
	case rootproto.GrantActIssue:
		return metapb.RootGrantAct_ROOT_GRANT_ACT_ISSUE
	case rootproto.GrantActSeal:
		return metapb.RootGrantAct_ROOT_GRANT_ACT_SEAL
	case rootproto.GrantActRetireExpired:
		return metapb.RootGrantAct_ROOT_GRANT_ACT_RETIRE_EXPIRED
	case rootproto.GrantActInherit:
		return metapb.RootGrantAct_ROOT_GRANT_ACT_INHERIT
	default:
		return metapb.RootGrantAct_ROOT_GRANT_ACT_UNSPECIFIED
	}
}

func RootGrantActFromProto(kind metapb.RootGrantAct) rootproto.GrantAct {
	switch kind {
	case metapb.RootGrantAct_ROOT_GRANT_ACT_ISSUE:
		return rootproto.GrantActIssue
	case metapb.RootGrantAct_ROOT_GRANT_ACT_SEAL:
		return rootproto.GrantActSeal
	case metapb.RootGrantAct_ROOT_GRANT_ACT_RETIRE_EXPIRED:
		return rootproto.GrantActRetireExpired
	case metapb.RootGrantAct_ROOT_GRANT_ACT_INHERIT:
		return rootproto.GrantActInherit
	default:
		return rootproto.GrantActUnknown
	}
}

func RootSnapshotToProto(snapshot rootstate.Snapshot, tailOffset uint64) *metapb.RootCheckpoint {
	stores := make([]*metapb.RootStore, 0, len(snapshot.Stores))
	for storeID, membership := range snapshot.Stores {
		stores = append(stores, RootStoreMembershipToProto(storeID, membership))
	}
	snapshotEpochs := make([]*metapb.RootSnapshotEpoch, 0, len(snapshot.SnapshotEpochs))
	for _, epoch := range snapshot.SnapshotEpochs {
		snapshotEpochs = append(snapshotEpochs, RootSnapshotEpochToProto(epoch))
	}
	mounts := make([]*metapb.RootMount, 0, len(snapshot.Mounts))
	for _, mount := range snapshot.Mounts {
		mounts = append(mounts, RootMountToProto(mount))
	}
	subtrees := make([]*metapb.RootSubtreeAuthority, 0, len(snapshot.Subtrees))
	for _, subtree := range snapshot.Subtrees {
		subtrees = append(subtrees, RootSubtreeAuthorityToProto(subtree))
	}
	quotas := make([]*metapb.RootQuotaFence, 0, len(snapshot.Quotas))
	for _, quota := range snapshot.Quotas {
		quotas = append(quotas, RootQuotaFenceToProto(quota))
	}
	descriptors := make([]*metapb.RegionDescriptor, 0, len(snapshot.Descriptors))
	for _, desc := range snapshot.Descriptors {
		descriptors = append(descriptors, DescriptorToProto(desc))
	}
	pending := make([]*metapb.RootPendingPeerChange, 0, len(snapshot.PendingPeerChanges))
	for regionID, change := range snapshot.PendingPeerChanges {
		pending = append(pending, RootPendingPeerChangeToProto(regionID, change))
	}
	pendingRanges := make([]*metapb.RootPendingRangeChange, 0, len(snapshot.PendingRangeChanges))
	for regionID, change := range snapshot.PendingRangeChanges {
		pendingRanges = append(pendingRanges, RootPendingRangeChangeToProto(regionID, change))
	}
	return &metapb.RootCheckpoint{
		State:               RootStateToProto(snapshot.State),
		Descriptors:         descriptors,
		TailOffset:          tailOffset,
		PendingPeerChanges:  pending,
		PendingRangeChanges: pendingRanges,
		Stores:              stores,
		SnapshotEpochs:      snapshotEpochs,
		Mounts:              mounts,
		Subtrees:            subtrees,
		Quotas:              quotas,
	}
}

func RootSnapshotFromProto(pbCheckpoint *metapb.RootCheckpoint) (rootstate.Snapshot, uint64) {
	if pbCheckpoint == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}, 0
	}
	snapshot := rootstate.Snapshot{
		State:               RootStateFromProto(pbCheckpoint.State),
		Descriptors:         make(map[uint64]topology.Descriptor, len(pbCheckpoint.Descriptors)),
		PendingPeerChanges:  make(map[uint64]rootstate.PendingPeerChange, len(pbCheckpoint.PendingPeerChanges)),
		PendingRangeChanges: make(map[uint64]rootstate.PendingRangeChange, len(pbCheckpoint.PendingRangeChanges)),
	}
	if len(pbCheckpoint.Stores) > 0 {
		snapshot.Stores = make(map[uint64]rootstate.StoreMembership, len(pbCheckpoint.Stores))
	}
	for _, pbStore := range pbCheckpoint.Stores {
		storeID, membership := RootStoreMembershipFromProto(pbStore)
		if storeID == 0 {
			continue
		}
		snapshot.Stores[storeID] = membership
	}
	if len(pbCheckpoint.SnapshotEpochs) > 0 {
		snapshot.SnapshotEpochs = make(map[string]rootstate.SnapshotEpoch, len(pbCheckpoint.SnapshotEpochs))
	}
	for _, pbEpoch := range pbCheckpoint.SnapshotEpochs {
		epoch := RootSnapshotEpochFromProto(pbEpoch)
		if epoch.SnapshotID == "" {
			continue
		}
		snapshot.SnapshotEpochs[epoch.SnapshotID] = epoch
	}
	if len(pbCheckpoint.Mounts) > 0 {
		snapshot.Mounts = make(map[string]rootstate.MountRecord, len(pbCheckpoint.Mounts))
	}
	for _, pbMount := range pbCheckpoint.Mounts {
		mount := RootMountFromProto(pbMount)
		if mount.MountID == "" {
			continue
		}
		snapshot.Mounts[mount.MountID] = mount
	}
	if len(pbCheckpoint.Subtrees) > 0 {
		snapshot.Subtrees = make(map[string]rootstate.SubtreeAuthority, len(pbCheckpoint.Subtrees))
	}
	for _, pbSubtree := range pbCheckpoint.Subtrees {
		subtree := RootSubtreeAuthorityFromProto(pbSubtree)
		if subtree.SubtreeID == "" {
			continue
		}
		snapshot.Subtrees[subtree.SubtreeID] = subtree
	}
	if len(pbCheckpoint.Quotas) > 0 {
		snapshot.Quotas = make(map[string]rootstate.QuotaFence, len(pbCheckpoint.Quotas))
	}
	for _, pbQuota := range pbCheckpoint.Quotas {
		quota := RootQuotaFenceFromProto(pbQuota)
		if quota.SubjectID == "" {
			continue
		}
		snapshot.Quotas[quota.SubjectID] = quota
	}
	for _, pbDesc := range pbCheckpoint.Descriptors {
		desc := DescriptorFromProto(pbDesc)
		if desc.RegionID == 0 {
			continue
		}
		snapshot.Descriptors[desc.RegionID] = desc
	}
	for _, pbPending := range pbCheckpoint.PendingPeerChanges {
		regionID, change := RootPendingPeerChangeFromProto(pbPending)
		if regionID == 0 {
			continue
		}
		snapshot.PendingPeerChanges[regionID] = change
	}
	for _, pbPending := range pbCheckpoint.PendingRangeChanges {
		regionID, change := RootPendingRangeChangeFromProto(pbPending)
		if regionID == 0 {
			continue
		}
		snapshot.PendingRangeChanges[regionID] = change
	}
	return snapshot, pbCheckpoint.TailOffset
}

func RootMountToProto(mount rootstate.MountRecord) *metapb.RootMount {
	return &metapb.RootMount{
		MountId:       mount.MountID,
		RootInode:     mount.RootInode,
		SchemaVersion: mount.SchemaVersion,
		State:         rootMountStateToProto(mount.State),
		RegisteredAt:  RootCursorToProto(mount.RegisteredAt),
		RetiredAt:     RootCursorToProto(mount.RetiredAt),
	}
}

func RootSubtreeAuthorityToProto(subtree rootstate.SubtreeAuthority) *metapb.RootSubtreeAuthority {
	return &metapb.RootSubtreeAuthority{
		SubtreeId:              subtree.SubtreeID,
		Mount:                  subtree.Mount,
		RootInode:              subtree.RootInode,
		AuthorityId:            subtree.AuthorityID,
		Era:                    subtree.Era,
		Frontier:               subtree.Frontier,
		State:                  rootSubtreeAuthorityStateToProto(subtree.State),
		DeclaredAt:             RootCursorToProto(subtree.DeclaredAt),
		HandoffStartedAt:       RootCursorToProto(subtree.HandoffStartedAt),
		HandoffCompletedAt:     RootCursorToProto(subtree.HandoffCompletedAt),
		PredecessorAuthorityId: subtree.PredecessorAuthorityID,
		PredecessorEra:         subtree.PredecessorEra,
		PredecessorFrontier:    subtree.PredecessorFrontier,
		SuccessorAuthorityId:   subtree.SuccessorAuthorityID,
		SuccessorEra:           subtree.SuccessorEra,
		InheritedFrontier:      subtree.InheritedFrontier,
	}
}

func RootSubtreeAuthorityFromProto(pbSubtree *metapb.RootSubtreeAuthority) rootstate.SubtreeAuthority {
	if pbSubtree == nil {
		return rootstate.SubtreeAuthority{}
	}
	subtree := rootstate.SubtreeAuthority{
		SubtreeID:              pbSubtree.GetSubtreeId(),
		Mount:                  pbSubtree.GetMount(),
		RootInode:              pbSubtree.GetRootInode(),
		AuthorityID:            pbSubtree.GetAuthorityId(),
		Era:                    pbSubtree.GetEra(),
		Frontier:               pbSubtree.GetFrontier(),
		State:                  rootSubtreeAuthorityStateFromProto(pbSubtree.GetState()),
		DeclaredAt:             RootCursorFromProto(pbSubtree.GetDeclaredAt()),
		HandoffStartedAt:       RootCursorFromProto(pbSubtree.GetHandoffStartedAt()),
		HandoffCompletedAt:     RootCursorFromProto(pbSubtree.GetHandoffCompletedAt()),
		PredecessorAuthorityID: pbSubtree.GetPredecessorAuthorityId(),
		PredecessorEra:         pbSubtree.GetPredecessorEra(),
		PredecessorFrontier:    pbSubtree.GetPredecessorFrontier(),
		SuccessorAuthorityID:   pbSubtree.GetSuccessorAuthorityId(),
		SuccessorEra:           pbSubtree.GetSuccessorEra(),
		InheritedFrontier:      pbSubtree.GetInheritedFrontier(),
	}
	if subtree.SubtreeID == "" {
		subtree.SubtreeID = rootstate.SubtreeAuthorityKey(subtree.Mount, subtree.RootInode)
	}
	return subtree
}

func RootQuotaFenceToProto(fence rootstate.QuotaFence) *metapb.RootQuotaFence {
	return &metapb.RootQuotaFence{
		SubjectId:   fence.SubjectID,
		Mount:       fence.Mount,
		SubtreeRoot: fence.RootInode,
		LimitBytes:  fence.LimitBytes,
		LimitInodes: fence.LimitInodes,
		Era:         fence.Era,
		Frontier:    fence.Frontier,
		UpdatedAt:   RootCursorToProto(fence.UpdatedAt),
	}
}

func RootQuotaFenceFromProto(pbFence *metapb.RootQuotaFence) rootstate.QuotaFence {
	if pbFence == nil {
		return rootstate.QuotaFence{}
	}
	fence := rootstate.QuotaFence{
		SubjectID:   pbFence.GetSubjectId(),
		Mount:       pbFence.GetMount(),
		RootInode:   pbFence.GetSubtreeRoot(),
		LimitBytes:  pbFence.GetLimitBytes(),
		LimitInodes: pbFence.GetLimitInodes(),
		Era:         pbFence.GetEra(),
		Frontier:    pbFence.GetFrontier(),
		UpdatedAt:   RootCursorFromProto(pbFence.GetUpdatedAt()),
	}
	if fence.SubjectID == "" {
		fence.SubjectID = rootstate.QuotaFenceKey(fence.Mount, fence.RootInode)
	}
	return fence
}

func RootMountFromProto(pbMount *metapb.RootMount) rootstate.MountRecord {
	if pbMount == nil {
		return rootstate.MountRecord{}
	}
	return rootstate.MountRecord{
		MountID:       pbMount.GetMountId(),
		RootInode:     pbMount.GetRootInode(),
		SchemaVersion: pbMount.GetSchemaVersion(),
		State:         rootMountStateFromProto(pbMount.GetState()),
		RegisteredAt:  RootCursorFromProto(pbMount.GetRegisteredAt()),
		RetiredAt:     RootCursorFromProto(pbMount.GetRetiredAt()),
	}
}

func RootSnapshotEpochToProto(epoch rootstate.SnapshotEpoch) *metapb.RootSnapshotEpoch {
	return &metapb.RootSnapshotEpoch{
		SnapshotId:  epoch.SnapshotID,
		Mount:       epoch.Mount,
		RootInode:   epoch.RootInode,
		ReadVersion: epoch.ReadVersion,
		PublishedAt: RootCursorToProto(epoch.PublishedAt),
	}
}

func RootSnapshotEpochFromProto(pbEpoch *metapb.RootSnapshotEpoch) rootstate.SnapshotEpoch {
	if pbEpoch == nil {
		return rootstate.SnapshotEpoch{}
	}
	return rootstate.SnapshotEpoch{
		SnapshotID:  pbEpoch.GetSnapshotId(),
		Mount:       pbEpoch.GetMount(),
		RootInode:   pbEpoch.GetRootInode(),
		ReadVersion: pbEpoch.GetReadVersion(),
		PublishedAt: RootCursorFromProto(pbEpoch.GetPublishedAt()),
	}
}

func RootStoreMembershipToProto(storeID uint64, membership rootstate.StoreMembership) *metapb.RootStore {
	if membership.StoreID != 0 {
		storeID = membership.StoreID
	}
	return &metapb.RootStore{
		StoreId:   storeID,
		State:     rootStoreStateToProto(membership.State),
		JoinedAt:  RootCursorToProto(membership.JoinedAt),
		RetiredAt: RootCursorToProto(membership.RetiredAt),
	}
}

func RootStoreMembershipFromProto(pbStore *metapb.RootStore) (uint64, rootstate.StoreMembership) {
	if pbStore == nil || pbStore.GetStoreId() == 0 {
		return 0, rootstate.StoreMembership{}
	}
	return pbStore.GetStoreId(), rootstate.StoreMembership{
		StoreID:   pbStore.GetStoreId(),
		State:     rootStoreStateFromProto(pbStore.GetState()),
		JoinedAt:  RootCursorFromProto(pbStore.GetJoinedAt()),
		RetiredAt: RootCursorFromProto(pbStore.GetRetiredAt()),
	}
}

func rootStoreStateToProto(state rootstate.StoreMembershipState) metapb.RootStoreState {
	switch state {
	case rootstate.StoreMembershipActive:
		return metapb.RootStoreState_ROOT_STORE_STATE_ACTIVE
	case rootstate.StoreMembershipRetired:
		return metapb.RootStoreState_ROOT_STORE_STATE_RETIRED
	default:
		return metapb.RootStoreState_ROOT_STORE_STATE_UNSPECIFIED
	}
}

func rootStoreStateFromProto(state metapb.RootStoreState) rootstate.StoreMembershipState {
	switch state {
	case metapb.RootStoreState_ROOT_STORE_STATE_ACTIVE:
		return rootstate.StoreMembershipActive
	case metapb.RootStoreState_ROOT_STORE_STATE_RETIRED:
		return rootstate.StoreMembershipRetired
	default:
		return rootstate.StoreMembershipUnknown
	}
}

func RootPendingPeerChangeToProto(regionID uint64, change rootstate.PendingPeerChange) *metapb.RootPendingPeerChange {
	return &metapb.RootPendingPeerChange{
		RegionId: regionID,
		StoreId:  change.StoreID,
		PeerId:   change.PeerID,
		Kind:     rootPendingPeerChangeKindToProto(change.Kind),
		Target:   DescriptorToProto(change.Target),
		Base:     DescriptorToProto(change.Base),
	}
}

func RootPendingPeerChangeFromProto(pbPending *metapb.RootPendingPeerChange) (uint64, rootstate.PendingPeerChange) {
	if pbPending == nil || pbPending.GetRegionId() == 0 {
		return 0, rootstate.PendingPeerChange{}
	}
	return pbPending.GetRegionId(), rootstate.PendingPeerChange{
		Kind:    rootPendingPeerChangeKindFromProto(pbPending.GetKind()),
		StoreID: pbPending.GetStoreId(),
		PeerID:  pbPending.GetPeerId(),
		Base:    DescriptorFromProto(pbPending.GetBase()),
		Target:  DescriptorFromProto(pbPending.GetTarget()),
	}
}

func RootPendingRangeChangeToProto(regionID uint64, change rootstate.PendingRangeChange) *metapb.RootPendingRangeChange {
	return &metapb.RootPendingRangeChange{
		RegionId:       regionID,
		Kind:           rootPendingRangeChangeKindToProto(change.Kind),
		ParentRegionId: change.ParentRegionID,
		LeftRegionId:   change.LeftRegionID,
		RightRegionId:  change.RightRegionID,
		Left:           DescriptorToProto(change.Left),
		Right:          DescriptorToProto(change.Right),
		Merged:         DescriptorToProto(change.Merged),
		BaseParent:     DescriptorToProto(change.BaseParent),
		BaseLeft:       DescriptorToProto(change.BaseLeft),
		BaseRight:      DescriptorToProto(change.BaseRight),
	}
}

func RootPendingRangeChangeFromProto(pbPending *metapb.RootPendingRangeChange) (uint64, rootstate.PendingRangeChange) {
	if pbPending == nil || pbPending.GetRegionId() == 0 {
		return 0, rootstate.PendingRangeChange{}
	}
	return pbPending.GetRegionId(), rootstate.PendingRangeChange{
		Kind:           rootPendingRangeChangeKindFromProto(pbPending.GetKind()),
		ParentRegionID: pbPending.GetParentRegionId(),
		LeftRegionID:   pbPending.GetLeftRegionId(),
		RightRegionID:  pbPending.GetRightRegionId(),
		BaseParent:     DescriptorFromProto(pbPending.GetBaseParent()),
		BaseLeft:       DescriptorFromProto(pbPending.GetBaseLeft()),
		BaseRight:      DescriptorFromProto(pbPending.GetBaseRight()),
		Left:           DescriptorFromProto(pbPending.GetLeft()),
		Right:          DescriptorFromProto(pbPending.GetRight()),
		Merged:         DescriptorFromProto(pbPending.GetMerged()),
	}
}

func rootPendingPeerChangeKindToProto(kind rootstate.PendingPeerChangeKind) metapb.RootPendingPeerChangeKind {
	switch kind {
	case rootstate.PendingPeerChangeAddition:
		return metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_ADDITION
	case rootstate.PendingPeerChangeRemoval:
		return metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_REMOVAL
	default:
		return metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_UNSPECIFIED
	}
}

func rootPendingPeerChangeKindFromProto(kind metapb.RootPendingPeerChangeKind) rootstate.PendingPeerChangeKind {
	switch kind {
	case metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_ADDITION:
		return rootstate.PendingPeerChangeAddition
	case metapb.RootPendingPeerChangeKind_ROOT_PENDING_PEER_CHANGE_KIND_REMOVAL:
		return rootstate.PendingPeerChangeRemoval
	default:
		return rootstate.PendingPeerChangeUnknown
	}
}

func rootPendingRangeChangeKindToProto(kind rootstate.PendingRangeChangeKind) metapb.RootPendingRangeChangeKind {
	switch kind {
	case rootstate.PendingRangeChangeSplit:
		return metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_SPLIT
	case rootstate.PendingRangeChangeMerge:
		return metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_MERGE
	default:
		return metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_UNSPECIFIED
	}
}

func rootPendingRangeChangeKindFromProto(kind metapb.RootPendingRangeChangeKind) rootstate.PendingRangeChangeKind {
	switch kind {
	case metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_SPLIT:
		return rootstate.PendingRangeChangeSplit
	case metapb.RootPendingRangeChangeKind_ROOT_PENDING_RANGE_CHANGE_KIND_MERGE:
		return rootstate.PendingRangeChangeMerge
	default:
		return rootstate.PendingRangeChangeUnknown
	}
}

func RootEventToProto(event rootevent.Event) *metapb.RootEvent {
	pbEvent := &metapb.RootEvent{Kind: rootEventKindToProto(event.Kind)}
	switch {
	case event.StoreMembership != nil:
		pbEvent.Payload = &metapb.RootEvent_StoreMembership{StoreMembership: &metapb.RootStoreMembership{StoreId: event.StoreMembership.StoreID}}
	case event.AllocatorFence != nil:
		pbEvent.Payload = &metapb.RootEvent_AllocatorFence{AllocatorFence: &metapb.RootAllocatorFence{Minimum: event.AllocatorFence.Minimum}}
	case event.Grant != nil:
		pbEvent.Payload = &metapb.RootEvent_Grant{Grant: RootAuthorityGrantToProto(*event.Grant)}
	case event.GrantRetirement != nil:
		pbEvent.Payload = &metapb.RootEvent_GrantRetirement{GrantRetirement: RootGrantRetirementToProto(*event.GrantRetirement)}
	case event.GrantInheritance != nil:
		pbEvent.Payload = &metapb.RootEvent_GrantInheritance{GrantInheritance: RootGrantInheritanceToProto(*event.GrantInheritance)}
	case event.SnapshotEpoch != nil:
		pbEvent.Payload = &metapb.RootEvent_SnapshotEpoch{SnapshotEpoch: rootEventSnapshotEpochToProto(event.SnapshotEpoch)}
	case event.Mount != nil:
		pbEvent.Payload = &metapb.RootEvent_Mount{Mount: rootEventMountToProto(event.Mount)}
	case event.SubtreeAuthority != nil:
		pbEvent.Payload = &metapb.RootEvent_SubtreeAuthority{SubtreeAuthority: rootEventSubtreeAuthorityToProto(event.SubtreeAuthority)}
	case event.QuotaFence != nil:
		pbEvent.Payload = &metapb.RootEvent_QuotaFence{QuotaFence: rootEventQuotaFenceToProto(event.QuotaFence)}
	case event.RegionDescriptor != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionDescriptor{RegionDescriptor: &metapb.RootRegionDescriptor{Descriptor_: DescriptorToProto(event.RegionDescriptor.Descriptor)}}
	case event.RegionRemoval != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionRemoval{RegionRemoval: &metapb.RootRegionRemoval{RegionId: event.RegionRemoval.RegionID}}
	case event.RangeSplit != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeSplit{RangeSplit: &metapb.RootRangeSplit{
			ParentRegionId: event.RangeSplit.ParentRegionID,
			SplitKey:       append([]byte(nil), event.RangeSplit.SplitKey...),
			Left:           DescriptorToProto(event.RangeSplit.Left),
			Right:          DescriptorToProto(event.RangeSplit.Right),
			BaseParent:     DescriptorToProto(event.RangeSplit.BaseParent),
		}}
	case event.RangeMerge != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeMerge{RangeMerge: &metapb.RootRangeMerge{
			LeftRegionId:  event.RangeMerge.LeftRegionID,
			RightRegionId: event.RangeMerge.RightRegionID,
			Merged:        DescriptorToProto(event.RangeMerge.Merged),
			BaseLeft:      DescriptorToProto(event.RangeMerge.BaseLeft),
			BaseRight:     DescriptorToProto(event.RangeMerge.BaseRight),
		}}
	case event.PeerChange != nil:
		pbEvent.Payload = &metapb.RootEvent_PeerChange{PeerChange: &metapb.RootPeerChange{
			RegionId: event.PeerChange.RegionID,
			StoreId:  event.PeerChange.StoreID,
			PeerId:   event.PeerChange.PeerID,
			Target:   DescriptorToProto(event.PeerChange.Region),
			Base:     DescriptorToProto(event.PeerChange.Base),
		}}
	}
	return pbEvent
}

func RootEventFromProto(pbEvent *metapb.RootEvent) rootevent.Event {
	if pbEvent == nil {
		return rootevent.Event{}
	}
	event := rootevent.Event{Kind: rootEventKindFromProto(pbEvent.Kind)}
	if body := pbEvent.GetStoreMembership(); body != nil {
		event.StoreMembership = &rootevent.StoreMembership{StoreID: body.StoreId}
	}
	if body := pbEvent.GetAllocatorFence(); body != nil {
		event.AllocatorFence = &rootevent.AllocatorFence{Minimum: body.Minimum}
	}
	if body := pbEvent.GetGrant(); body != nil {
		grant := RootAuthorityGrantFromProto(body)
		event.Grant = &grant
	}
	if body := pbEvent.GetGrantRetirement(); body != nil {
		retirement := RootGrantRetirementFromProto(body)
		event.GrantRetirement = &retirement
	}
	if body := pbEvent.GetGrantInheritance(); body != nil {
		inheritance := RootGrantInheritanceFromProto(body)
		event.GrantInheritance = &inheritance
	}
	if body := pbEvent.GetSnapshotEpoch(); body != nil {
		event.SnapshotEpoch = rootEventSnapshotEpochFromProto(body)
	}
	if body := pbEvent.GetMount(); body != nil {
		event.Mount = rootEventMountFromProto(body)
	}
	if body := pbEvent.GetSubtreeAuthority(); body != nil {
		event.SubtreeAuthority = rootEventSubtreeAuthorityFromProto(body)
	}
	if body := pbEvent.GetQuotaFence(); body != nil {
		event.QuotaFence = rootEventQuotaFenceFromProto(body)
	}
	if body := pbEvent.GetRegionDescriptor(); body != nil {
		event.RegionDescriptor = &rootevent.RegionDescriptorRecord{Descriptor: DescriptorFromProto(body.GetDescriptor_())}
	}
	if body := pbEvent.GetRegionRemoval(); body != nil {
		event.RegionRemoval = &rootevent.RegionRemoval{RegionID: body.RegionId}
	}
	if body := pbEvent.GetRangeSplit(); body != nil {
		event.RangeSplit = &rootevent.RangeSplit{
			ParentRegionID: body.ParentRegionId,
			SplitKey:       append([]byte(nil), body.SplitKey...),
			Left:           DescriptorFromProto(body.Left),
			Right:          DescriptorFromProto(body.Right),
			BaseParent:     DescriptorFromProto(body.BaseParent),
		}
	}
	if body := pbEvent.GetRangeMerge(); body != nil {
		event.RangeMerge = &rootevent.RangeMerge{
			LeftRegionID:  body.LeftRegionId,
			RightRegionID: body.RightRegionId,
			Merged:        DescriptorFromProto(body.Merged),
			BaseLeft:      DescriptorFromProto(body.BaseLeft),
			BaseRight:     DescriptorFromProto(body.BaseRight),
		}
	}
	if body := pbEvent.GetPeerChange(); body != nil {
		event.PeerChange = &rootevent.PeerChange{
			RegionID: body.RegionId,
			StoreID:  body.StoreId,
			PeerID:   body.PeerId,
			Region:   DescriptorFromProto(body.GetTarget()),
			Base:     DescriptorFromProto(body.GetBase()),
		}
	}
	return event
}

func rootEventKindToProto(kind rootevent.Kind) metapb.RootEventKind {
	switch kind {
	case rootevent.KindStoreJoined:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED
	case rootevent.KindStoreRetired:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_RETIRED
	case rootevent.KindIDAllocatorFenced:
		return metapb.RootEventKind_ROOT_EVENT_KIND_ID_ALLOCATOR_FENCED
	case rootevent.KindRegionBootstrap:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP
	case rootevent.KindRegionDescriptorPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED
	case rootevent.KindRegionTombstoned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED
	case rootevent.KindTSOAllocatorFenced:
		return metapb.RootEventKind_ROOT_EVENT_KIND_TSO_ALLOCATOR_FENCED
	case rootevent.KindRegionSplitPlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_PLANNED
	case rootevent.KindRegionSplitCommitted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED
	case rootevent.KindRegionSplitCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_CANCELLED
	case rootevent.KindRegionMergePlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_PLANNED
	case rootevent.KindRegionMerged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED
	case rootevent.KindRegionMergeCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_CANCELLED
	case rootevent.KindPeerAdditionPlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_PLANNED
	case rootevent.KindPeerRemovalPlanned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_PLANNED
	case rootevent.KindPeerAdded:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED
	case rootevent.KindPeerRemoved:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED
	case rootevent.KindPeerAdditionCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_CANCELLED
	case rootevent.KindPeerRemovalCancelled:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_CANCELLED
	case rootevent.KindGrantIssued:
		return metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_ISSUED
	case rootevent.KindGrantSealed:
		return metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_SEALED
	case rootevent.KindGrantRetired:
		return metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_RETIRED
	case rootevent.KindGrantInherited:
		return metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_INHERITED
	case rootevent.KindSnapshotEpochPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_PUBLISHED
	case rootevent.KindSnapshotEpochRetired:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_RETIRED
	case rootevent.KindMountRegistered:
		return metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_REGISTERED
	case rootevent.KindMountRetired:
		return metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_RETIRED
	case rootevent.KindSubtreeAuthorityDeclared:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SUBTREE_AUTHORITY_DECLARED
	case rootevent.KindSubtreeHandoffStarted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SUBTREE_HANDOFF_STARTED
	case rootevent.KindSubtreeHandoffCompleted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SUBTREE_HANDOFF_COMPLETED
	case rootevent.KindQuotaFenceUpdated:
		return metapb.RootEventKind_ROOT_EVENT_KIND_QUOTA_FENCE_UPDATED
	default:
		return metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED
	}
}

func rootEventKindFromProto(kind metapb.RootEventKind) rootevent.Kind {
	switch kind {
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED:
		return rootevent.KindStoreJoined
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_RETIRED:
		return rootevent.KindStoreRetired
	case metapb.RootEventKind_ROOT_EVENT_KIND_ID_ALLOCATOR_FENCED:
		return rootevent.KindIDAllocatorFenced
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP:
		return rootevent.KindRegionBootstrap
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED:
		return rootevent.KindRegionDescriptorPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED:
		return rootevent.KindRegionTombstoned
	case metapb.RootEventKind_ROOT_EVENT_KIND_TSO_ALLOCATOR_FENCED:
		return rootevent.KindTSOAllocatorFenced
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_PLANNED:
		return rootevent.KindRegionSplitPlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED:
		return rootevent.KindRegionSplitCommitted
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_CANCELLED:
		return rootevent.KindRegionSplitCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_PLANNED:
		return rootevent.KindRegionMergePlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED:
		return rootevent.KindRegionMerged
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGE_CANCELLED:
		return rootevent.KindRegionMergeCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_PLANNED:
		return rootevent.KindPeerAdditionPlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_PLANNED:
		return rootevent.KindPeerRemovalPlanned
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED:
		return rootevent.KindPeerAdded
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED:
		return rootevent.KindPeerRemoved
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDITION_CANCELLED:
		return rootevent.KindPeerAdditionCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVAL_CANCELLED:
		return rootevent.KindPeerRemovalCancelled
	case metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_ISSUED:
		return rootevent.KindGrantIssued
	case metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_SEALED:
		return rootevent.KindGrantSealed
	case metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_RETIRED:
		return rootevent.KindGrantRetired
	case metapb.RootEventKind_ROOT_EVENT_KIND_GRANT_INHERITED:
		return rootevent.KindGrantInherited
	case metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_PUBLISHED:
		return rootevent.KindSnapshotEpochPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_RETIRED:
		return rootevent.KindSnapshotEpochRetired
	case metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_REGISTERED:
		return rootevent.KindMountRegistered
	case metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_RETIRED:
		return rootevent.KindMountRetired
	case metapb.RootEventKind_ROOT_EVENT_KIND_SUBTREE_AUTHORITY_DECLARED:
		return rootevent.KindSubtreeAuthorityDeclared
	case metapb.RootEventKind_ROOT_EVENT_KIND_SUBTREE_HANDOFF_STARTED:
		return rootevent.KindSubtreeHandoffStarted
	case metapb.RootEventKind_ROOT_EVENT_KIND_SUBTREE_HANDOFF_COMPLETED:
		return rootevent.KindSubtreeHandoffCompleted
	case metapb.RootEventKind_ROOT_EVENT_KIND_QUOTA_FENCE_UPDATED:
		return rootevent.KindQuotaFenceUpdated
	default:
		return rootevent.KindUnknown
	}
}

func rootMountStateToProto(state rootstate.MountState) metapb.RootMountState {
	switch state {
	case rootstate.MountStateActive:
		return metapb.RootMountState_ROOT_MOUNT_STATE_ACTIVE
	case rootstate.MountStateRetired:
		return metapb.RootMountState_ROOT_MOUNT_STATE_RETIRED
	default:
		return metapb.RootMountState_ROOT_MOUNT_STATE_UNSPECIFIED
	}
}

func rootMountStateFromProto(state metapb.RootMountState) rootstate.MountState {
	switch state {
	case metapb.RootMountState_ROOT_MOUNT_STATE_ACTIVE:
		return rootstate.MountStateActive
	case metapb.RootMountState_ROOT_MOUNT_STATE_RETIRED:
		return rootstate.MountStateRetired
	default:
		return rootstate.MountStateUnknown
	}
}

func rootSubtreeAuthorityStateToProto(state rootstate.SubtreeAuthorityState) metapb.RootSubtreeAuthorityState {
	switch state {
	case rootstate.SubtreeAuthorityActive:
		return metapb.RootSubtreeAuthorityState_ROOT_SUBTREE_AUTHORITY_STATE_ACTIVE
	case rootstate.SubtreeAuthorityHandoff:
		return metapb.RootSubtreeAuthorityState_ROOT_SUBTREE_AUTHORITY_STATE_HANDOFF
	default:
		return metapb.RootSubtreeAuthorityState_ROOT_SUBTREE_AUTHORITY_STATE_UNSPECIFIED
	}
}

func rootSubtreeAuthorityStateFromProto(state metapb.RootSubtreeAuthorityState) rootstate.SubtreeAuthorityState {
	switch state {
	case metapb.RootSubtreeAuthorityState_ROOT_SUBTREE_AUTHORITY_STATE_ACTIVE:
		return rootstate.SubtreeAuthorityActive
	case metapb.RootSubtreeAuthorityState_ROOT_SUBTREE_AUTHORITY_STATE_HANDOFF:
		return rootstate.SubtreeAuthorityHandoff
	default:
		return rootstate.SubtreeAuthorityUnknown
	}
}
