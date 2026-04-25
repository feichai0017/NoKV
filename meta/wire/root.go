package wire

import (
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
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
		ClusterEpoch:    state.ClusterEpoch,
		MembershipEpoch: state.MembershipEpoch,
		LastCommitted:   RootCursorToProto(state.LastCommitted),
		IdFence:         state.IDFence,
		TsoFence:        state.TSOFence,
		Tenure:          RootTenureToProto(state.Tenure),
		Legacy:          RootLegacyToProto(state.Legacy),
		Handover:        RootHandoverToProto(state.Handover),
	}
}

func RootStateFromProto(pbState *metapb.RootState) rootstate.State {
	if pbState == nil {
		return rootstate.State{}
	}
	return rootstate.State{
		ClusterEpoch:    pbState.ClusterEpoch,
		MembershipEpoch: pbState.MembershipEpoch,
		LastCommitted:   RootCursorFromProto(pbState.LastCommitted),
		IDFence:         pbState.IdFence,
		TSOFence:        pbState.TsoFence,
		Tenure:          RootTenureFromProto(pbState.GetTenure()),
		Legacy:          RootLegacyFromProto(pbState.GetLegacy()),
		Handover:        RootHandoverFromProto(pbState.GetHandover()),
	}
}

func RootTenureToProto(lease rootstate.Tenure) *metapb.RootTenure {
	if !lease.Present() {
		return nil
	}
	return &metapb.RootTenure{
		HolderId:        lease.HolderID,
		ExpiresUnixNano: lease.ExpiresUnixNano,
		Era:             lease.Era,
		IssuedAt:        RootCursorToProto(lease.IssuedAt),
		Mandate:         lease.Mandate,
		LineageDigest:   lease.LineageDigest,
	}
}

func RootTenureFromProto(lease *metapb.RootTenure) rootstate.Tenure {
	if lease == nil {
		return rootstate.Tenure{}
	}
	return rootstate.Tenure{
		HolderID:        lease.GetHolderId(),
		ExpiresUnixNano: lease.GetExpiresUnixNano(),
		Era:             lease.GetEra(),
		IssuedAt:        RootCursorFromProto(lease.GetIssuedAt()),
		Mandate:         lease.GetMandate(),
		LineageDigest:   lease.GetLineageDigest(),
	}
}

func RootLegacyToProto(seal rootstate.Legacy) *metapb.RootLegacy {
	if !seal.Present() {
		return nil
	}
	return &metapb.RootLegacy{
		HolderId:  seal.HolderID,
		Era:       seal.Era,
		Mandate:   seal.Mandate,
		Frontiers: RootMandateFrontiersToProto(seal.Frontiers),
		SealedAt:  RootCursorToProto(seal.SealedAt),
	}
}

func RootLegacyFromProto(seal *metapb.RootLegacy) rootstate.Legacy {
	if seal == nil {
		return rootstate.Legacy{}
	}
	return rootstate.Legacy{
		HolderID:  seal.GetHolderId(),
		Era:       seal.GetEra(),
		Mandate:   seal.GetMandate(),
		Frontiers: RootMandateFrontiersFromProto(seal.GetFrontiers()),
		SealedAt:  RootCursorFromProto(seal.GetSealedAt()),
	}
}

func RootHandoverToProto(handover rootstate.Handover) *metapb.RootHandover {
	if !handover.Present() {
		return nil
	}
	return &metapb.RootHandover{
		HolderId:     handover.HolderID,
		LegacyEra:    handover.LegacyEra,
		SuccessorEra: handover.SuccessorEra,
		LegacyDigest: handover.LegacyDigest,
		Stage:        rootHandoverStageToProto(handover.Stage),
		ConfirmedAt:  RootCursorToProto(handover.ConfirmedAt),
		ClosedAt:     RootCursorToProto(handover.ClosedAt),
		ReattachedAt: RootCursorToProto(handover.ReattachedAt),
	}
}

func RootHandoverFromProto(handover *metapb.RootHandover) rootstate.Handover {
	if handover == nil {
		return rootstate.Handover{}
	}
	return rootstate.Handover{
		HolderID:     handover.GetHolderId(),
		LegacyEra:    handover.GetLegacyEra(),
		SuccessorEra: handover.GetSuccessorEra(),
		LegacyDigest: handover.GetLegacyDigest(),
		Stage:        rootHandoverStageFromProto(handover.GetStage()),
		ConfirmedAt:  RootCursorFromProto(handover.GetConfirmedAt()),
		ClosedAt:     RootCursorFromProto(handover.GetClosedAt()),
		ReattachedAt: RootCursorFromProto(handover.GetReattachedAt()),
	}
}

func RootMandateFrontiersToProto(frontiers rootproto.MandateFrontiers) []*metapb.RootMandateFrontier {
	if frontiers.Len() == 0 {
		return nil
	}
	entries := frontiers.Entries()
	out := make([]*metapb.RootMandateFrontier, 0, len(entries))
	for _, entry := range entries {
		out = append(out, &metapb.RootMandateFrontier{Mandate: entry.Mandate, Frontier: entry.Frontier})
	}
	return out
}

func RootMandateFrontiersFromProto(frontiers []*metapb.RootMandateFrontier) rootproto.MandateFrontiers {
	entries := make([]rootproto.MandateFrontier, 0, len(frontiers))
	for _, entry := range frontiers {
		if entry == nil || entry.GetMandate() == 0 {
			continue
		}
		entries = append(entries, rootproto.MandateFrontier{
			Mandate:  entry.GetMandate(),
			Frontier: entry.GetFrontier(),
		})
	}
	return rootproto.NewMandateFrontiers(entries...)
}

func RootSuccessionStateToProto(state rootstate.SuccessionState) *metapb.RootSuccessionState {
	return &metapb.RootSuccessionState{
		Tenure:   RootTenureToProto(state.Tenure),
		Legacy:   RootLegacyToProto(state.Legacy),
		Handover: RootHandoverToProto(state.Handover),
	}
}

func RootSuccessionStateFromProto(state *metapb.RootSuccessionState) rootstate.SuccessionState {
	if state == nil {
		return rootstate.SuccessionState{}
	}
	return rootstate.SuccessionState{
		Tenure:   RootTenureFromProto(state.GetTenure()),
		Legacy:   RootLegacyFromProto(state.GetLegacy()),
		Handover: RootHandoverFromProto(state.GetHandover()),
	}
}

func RootTenureCommandToProto(cmd rootproto.TenureCommand) *metapb.RootTenureCommand {
	return &metapb.RootTenureCommand{
		Kind:               rootTenureActToProto(cmd.Kind),
		HolderId:           cmd.HolderID,
		ExpiresUnixNano:    cmd.ExpiresUnixNano,
		NowUnixNano:        cmd.NowUnixNano,
		LineageDigest:      cmd.LineageDigest,
		InheritedFrontiers: RootMandateFrontiersToProto(cmd.InheritedFrontiers),
	}
}

func RootTenureCommandFromProto(cmd *metapb.RootTenureCommand) rootproto.TenureCommand {
	if cmd == nil {
		return rootproto.TenureCommand{}
	}
	return rootproto.TenureCommand{
		Kind:               rootTenureActFromProto(cmd.GetKind()),
		HolderID:           cmd.GetHolderId(),
		ExpiresUnixNano:    cmd.GetExpiresUnixNano(),
		NowUnixNano:        cmd.GetNowUnixNano(),
		LineageDigest:      cmd.GetLineageDigest(),
		InheritedFrontiers: RootMandateFrontiersFromProto(cmd.GetInheritedFrontiers()),
	}
}

func RootHandoverCommandToProto(cmd rootproto.HandoverCommand) *metapb.RootHandoverCommand {
	return &metapb.RootHandoverCommand{
		Kind:        rootHandoverActToProto(cmd.Kind),
		HolderId:    cmd.HolderID,
		NowUnixNano: cmd.NowUnixNano,
		Frontiers:   RootMandateFrontiersToProto(cmd.Frontiers),
	}
}

func RootHandoverCommandFromProto(cmd *metapb.RootHandoverCommand) rootproto.HandoverCommand {
	if cmd == nil {
		return rootproto.HandoverCommand{}
	}
	return rootproto.HandoverCommand{
		Kind:        rootHandoverActFromProto(cmd.GetKind()),
		HolderID:    cmd.GetHolderId(),
		NowUnixNano: cmd.GetNowUnixNano(),
		Frontiers:   RootMandateFrontiersFromProto(cmd.GetFrontiers()),
	}
}

func rootEventTenureToProto(lease *rootevent.Tenure) *metapb.RootTenure {
	if lease == nil {
		return nil
	}
	return &metapb.RootTenure{
		HolderId:           lease.HolderID,
		ExpiresUnixNano:    lease.ExpiresUnixNano,
		Era:                lease.Era,
		IssuedAt:           RootCursorToProto(lease.IssuedAt),
		Mandate:            lease.Mandate,
		LineageDigest:      lease.LineageDigest,
		InheritedFrontiers: RootMandateFrontiersToProto(lease.Frontiers),
	}
}

func rootEventTenureFromProto(lease *metapb.RootTenure) *rootevent.Tenure {
	if lease == nil {
		return nil
	}
	return &rootevent.Tenure{
		HolderID:        lease.GetHolderId(),
		ExpiresUnixNano: lease.GetExpiresUnixNano(),
		Era:             lease.GetEra(),
		IssuedAt:        RootCursorFromProto(lease.GetIssuedAt()),
		Mandate:         lease.GetMandate(),
		LineageDigest:   lease.GetLineageDigest(),
		Frontiers:       RootMandateFrontiersFromProto(lease.GetInheritedFrontiers()),
	}
}

func rootEventLegacyToProto(seal *rootevent.Legacy) *metapb.RootLegacy {
	if seal == nil {
		return nil
	}
	return &metapb.RootLegacy{
		HolderId:  seal.HolderID,
		Era:       seal.Era,
		Mandate:   seal.Mandate,
		Frontiers: RootMandateFrontiersToProto(seal.Frontiers),
		SealedAt:  RootCursorToProto(seal.SealedAt),
	}
}

func rootEventLegacyFromProto(seal *metapb.RootLegacy) *rootevent.Legacy {
	if seal == nil {
		return nil
	}
	return &rootevent.Legacy{
		HolderID:  seal.GetHolderId(),
		Era:       seal.GetEra(),
		Mandate:   seal.GetMandate(),
		Frontiers: RootMandateFrontiersFromProto(seal.GetFrontiers()),
		SealedAt:  RootCursorFromProto(seal.GetSealedAt()),
	}
}

func rootEventHandoverToProto(handover *rootevent.Handover) *metapb.RootHandover {
	if handover == nil {
		return nil
	}
	return &metapb.RootHandover{
		HolderId:     handover.HolderID,
		LegacyEra:    handover.LegacyEra,
		SuccessorEra: handover.SuccessorEra,
		LegacyDigest: handover.LegacyDigest,
		Stage:        rootHandoverStageToProto(handover.Stage),
		ConfirmedAt:  RootCursorToProto(handover.ConfirmedAt),
		ClosedAt:     RootCursorToProto(handover.ClosedAt),
		ReattachedAt: RootCursorToProto(handover.ReattachedAt),
	}
}

func rootEventHandoverFromProto(handover *metapb.RootHandover) *rootevent.Handover {
	if handover == nil {
		return nil
	}
	return &rootevent.Handover{
		HolderID:     handover.GetHolderId(),
		LegacyEra:    handover.GetLegacyEra(),
		SuccessorEra: handover.GetSuccessorEra(),
		LegacyDigest: handover.GetLegacyDigest(),
		Stage:        rootHandoverStageFromProto(handover.GetStage()),
		ConfirmedAt:  RootCursorFromProto(handover.GetConfirmedAt()),
		ClosedAt:     RootCursorFromProto(handover.GetClosedAt()),
		ReattachedAt: RootCursorFromProto(handover.GetReattachedAt()),
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

func rootHandoverStageToProto(stage rootproto.HandoverStage) metapb.RootHandoverStage {
	switch stage {
	case rootproto.HandoverStageUnspecified:
		return metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_UNSPECIFIED
	case rootproto.HandoverStageConfirmed:
		return metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_CONFIRMED
	case rootproto.HandoverStageClosed:
		return metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_CLOSED
	case rootproto.HandoverStageReattached:
		return metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_REATTACHED
	default:
		return metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_UNSPECIFIED
	}
}

func rootTenureActToProto(kind rootproto.TenureAct) metapb.RootTenureAct {
	switch kind {
	case rootproto.TenureActIssue:
		return metapb.RootTenureAct_ROOT_TENURE_ACT_ISSUE
	case rootproto.TenureActRelease:
		return metapb.RootTenureAct_ROOT_TENURE_ACT_RELEASE
	default:
		return metapb.RootTenureAct_ROOT_TENURE_ACT_UNSPECIFIED
	}
}

func rootTenureActFromProto(kind metapb.RootTenureAct) rootproto.TenureAct {
	switch kind {
	case metapb.RootTenureAct_ROOT_TENURE_ACT_ISSUE:
		return rootproto.TenureActIssue
	case metapb.RootTenureAct_ROOT_TENURE_ACT_RELEASE:
		return rootproto.TenureActRelease
	default:
		return rootproto.TenureActUnknown
	}
}

func rootHandoverActToProto(kind rootproto.HandoverAct) metapb.RootHandoverAct {
	switch kind {
	case rootproto.HandoverActSeal:
		return metapb.RootHandoverAct_ROOT_HANDOVER_ACT_SEAL
	case rootproto.HandoverActConfirm:
		return metapb.RootHandoverAct_ROOT_HANDOVER_ACT_CONFIRM
	case rootproto.HandoverActClose:
		return metapb.RootHandoverAct_ROOT_HANDOVER_ACT_CLOSE
	case rootproto.HandoverActReattach:
		return metapb.RootHandoverAct_ROOT_HANDOVER_ACT_REATTACH
	default:
		return metapb.RootHandoverAct_ROOT_HANDOVER_ACT_UNSPECIFIED
	}
}

func rootHandoverActFromProto(kind metapb.RootHandoverAct) rootproto.HandoverAct {
	switch kind {
	case metapb.RootHandoverAct_ROOT_HANDOVER_ACT_SEAL:
		return rootproto.HandoverActSeal
	case metapb.RootHandoverAct_ROOT_HANDOVER_ACT_CONFIRM:
		return rootproto.HandoverActConfirm
	case metapb.RootHandoverAct_ROOT_HANDOVER_ACT_CLOSE:
		return rootproto.HandoverActClose
	case metapb.RootHandoverAct_ROOT_HANDOVER_ACT_REATTACH:
		return rootproto.HandoverActReattach
	default:
		return rootproto.HandoverActUnknown
	}
}

func rootHandoverStageFromProto(stage metapb.RootHandoverStage) rootproto.HandoverStage {
	switch stage {
	case metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_UNSPECIFIED:
		return rootproto.HandoverStageUnspecified
	case metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_CONFIRMED:
		return rootproto.HandoverStageConfirmed
	case metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_CLOSED:
		return rootproto.HandoverStageClosed
	case metapb.RootHandoverStage_ROOT_HANDOVER_STAGE_REATTACHED:
		return rootproto.HandoverStageReattached
	default:
		return rootproto.HandoverStageUnspecified
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
	}
}

func RootSnapshotFromProto(pbCheckpoint *metapb.RootCheckpoint) (rootstate.Snapshot, uint64) {
	if pbCheckpoint == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0
	}
	snapshot := rootstate.Snapshot{
		State:               RootStateFromProto(pbCheckpoint.State),
		Descriptors:         make(map[uint64]descriptor.Descriptor, len(pbCheckpoint.Descriptors)),
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
	case event.Tenure != nil:
		pbEvent.Payload = &metapb.RootEvent_Tenure{Tenure: rootEventTenureToProto(event.Tenure)}
	case event.Legacy != nil:
		pbEvent.Payload = &metapb.RootEvent_Legacy{Legacy: rootEventLegacyToProto(event.Legacy)}
	case event.Handover != nil:
		pbEvent.Payload = &metapb.RootEvent_Handover{Handover: rootEventHandoverToProto(event.Handover)}
	case event.SnapshotEpoch != nil:
		pbEvent.Payload = &metapb.RootEvent_SnapshotEpoch{SnapshotEpoch: rootEventSnapshotEpochToProto(event.SnapshotEpoch)}
	case event.Mount != nil:
		pbEvent.Payload = &metapb.RootEvent_Mount{Mount: rootEventMountToProto(event.Mount)}
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
	if body := pbEvent.GetTenure(); body != nil {
		event.Tenure = rootEventTenureFromProto(body)
	}
	if body := pbEvent.GetLegacy(); body != nil {
		event.Legacy = rootEventLegacyFromProto(body)
	}
	if body := pbEvent.GetHandover(); body != nil {
		event.Handover = rootEventHandoverFromProto(body)
	}
	if body := pbEvent.GetSnapshotEpoch(); body != nil {
		event.SnapshotEpoch = rootEventSnapshotEpochFromProto(body)
	}
	if body := pbEvent.GetMount(); body != nil {
		event.Mount = rootEventMountFromProto(body)
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
	case rootevent.KindTenure:
		return metapb.RootEventKind_ROOT_EVENT_KIND_TENURE
	case rootevent.KindLegacy:
		return metapb.RootEventKind_ROOT_EVENT_KIND_LEGACY
	case rootevent.KindHandover:
		return metapb.RootEventKind_ROOT_EVENT_KIND_HANDOVER
	case rootevent.KindSnapshotEpochPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_PUBLISHED
	case rootevent.KindSnapshotEpochRetired:
		return metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_RETIRED
	case rootevent.KindMountRegistered:
		return metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_REGISTERED
	case rootevent.KindMountRetired:
		return metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_RETIRED
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
	case metapb.RootEventKind_ROOT_EVENT_KIND_TENURE:
		return rootevent.KindTenure
	case metapb.RootEventKind_ROOT_EVENT_KIND_LEGACY:
		return rootevent.KindLegacy
	case metapb.RootEventKind_ROOT_EVENT_KIND_HANDOVER:
		return rootevent.KindHandover
	case metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_PUBLISHED:
		return rootevent.KindSnapshotEpochPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_SNAPSHOT_EPOCH_RETIRED:
		return rootevent.KindSnapshotEpochRetired
	case metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_REGISTERED:
		return rootevent.KindMountRegistered
	case metapb.RootEventKind_ROOT_EVENT_KIND_MOUNT_RETIRED:
		return rootevent.KindMountRetired
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
