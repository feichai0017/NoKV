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
		ClusterEpoch:       state.ClusterEpoch,
		MembershipEpoch:    state.MembershipEpoch,
		LastCommitted:      RootCursorToProto(state.LastCommitted),
		IdFence:            state.IDFence,
		TsoFence:           state.TSOFence,
		CoordinatorLease:   RootCoordinatorLeaseToProto(state.CoordinatorLease),
		CoordinatorSeal:    RootCoordinatorSealToProto(state.CoordinatorSeal),
		CoordinatorClosure: RootCoordinatorClosureToProto(state.CoordinatorClosure),
	}
}

func RootStateFromProto(pbState *metapb.RootState) rootstate.State {
	if pbState == nil {
		return rootstate.State{}
	}
	return rootstate.State{
		ClusterEpoch:       pbState.ClusterEpoch,
		MembershipEpoch:    pbState.MembershipEpoch,
		LastCommitted:      RootCursorFromProto(pbState.LastCommitted),
		IDFence:            pbState.IdFence,
		TSOFence:           pbState.TsoFence,
		CoordinatorLease:   RootCoordinatorLeaseFromProto(pbState.GetCoordinatorLease()),
		CoordinatorSeal:    RootCoordinatorSealFromProto(pbState.GetCoordinatorSeal()),
		CoordinatorClosure: RootCoordinatorClosureFromProto(pbState.GetCoordinatorClosure()),
	}
}

func RootCoordinatorLeaseToProto(lease rootstate.CoordinatorLease) *metapb.RootCoordinatorLease {
	if !lease.Present() {
		return nil
	}
	return &metapb.RootCoordinatorLease{
		HolderId:          lease.HolderID,
		ExpiresUnixNano:   lease.ExpiresUnixNano,
		CertGeneration:    lease.CertGeneration,
		IssuedCursor:      RootCursorToProto(lease.IssuedCursor),
		DutyMask:          lease.DutyMask,
		PredecessorDigest: lease.PredecessorDigest,
	}
}

func RootCoordinatorLeaseFromProto(lease *metapb.RootCoordinatorLease) rootstate.CoordinatorLease {
	if lease == nil {
		return rootstate.CoordinatorLease{}
	}
	return rootstate.CoordinatorLease{
		HolderID:          lease.GetHolderId(),
		ExpiresUnixNano:   lease.GetExpiresUnixNano(),
		CertGeneration:    lease.GetCertGeneration(),
		IssuedCursor:      RootCursorFromProto(lease.GetIssuedCursor()),
		DutyMask:          lease.GetDutyMask(),
		PredecessorDigest: lease.GetPredecessorDigest(),
	}
}

func RootCoordinatorSealToProto(seal rootstate.CoordinatorSeal) *metapb.RootCoordinatorSeal {
	if !seal.Present() {
		return nil
	}
	return &metapb.RootCoordinatorSeal{
		HolderId:          seal.HolderID,
		CertGeneration:    seal.CertGeneration,
		DutyMask:          seal.DutyMask,
		ConsumedFrontiers: RootDutyFrontiersToProto(seal.Frontiers),
		SealedAtCursor:    RootCursorToProto(seal.SealedAtCursor),
	}
}

func RootCoordinatorSealFromProto(seal *metapb.RootCoordinatorSeal) rootstate.CoordinatorSeal {
	if seal == nil {
		return rootstate.CoordinatorSeal{}
	}
	return rootstate.CoordinatorSeal{
		HolderID:       seal.GetHolderId(),
		CertGeneration: seal.GetCertGeneration(),
		DutyMask:       seal.GetDutyMask(),
		Frontiers:      RootDutyFrontiersFromProto(seal.GetConsumedFrontiers()),
		SealedAtCursor: RootCursorFromProto(seal.GetSealedAtCursor()),
	}
}

func RootCoordinatorClosureToProto(closure rootstate.CoordinatorClosure) *metapb.RootCoordinatorClosure {
	if !closure.Present() {
		return nil
	}
	return &metapb.RootCoordinatorClosure{
		HolderId:            closure.HolderID,
		SealGeneration:      closure.SealGeneration,
		SuccessorGeneration: closure.SuccessorGeneration,
		SealDigest:          closure.SealDigest,
		Stage:               rootCoordinatorClosureStageToProto(closure.Stage),
		ConfirmedAtCursor:   RootCursorToProto(closure.ConfirmedAtCursor),
		ClosedAtCursor:      RootCursorToProto(closure.ClosedAtCursor),
		ReattachedAtCursor:  RootCursorToProto(closure.ReattachedAtCursor),
	}
}

func RootCoordinatorClosureFromProto(closure *metapb.RootCoordinatorClosure) rootstate.CoordinatorClosure {
	if closure == nil {
		return rootstate.CoordinatorClosure{}
	}
	return rootstate.CoordinatorClosure{
		HolderID:            closure.GetHolderId(),
		SealGeneration:      closure.GetSealGeneration(),
		SuccessorGeneration: closure.GetSuccessorGeneration(),
		SealDigest:          closure.GetSealDigest(),
		Stage:               rootCoordinatorClosureStageFromProto(closure.GetStage()),
		ConfirmedAtCursor:   RootCursorFromProto(closure.GetConfirmedAtCursor()),
		ClosedAtCursor:      RootCursorFromProto(closure.GetClosedAtCursor()),
		ReattachedAtCursor:  RootCursorFromProto(closure.GetReattachedAtCursor()),
	}
}

func RootDutyFrontiersToProto(frontiers rootproto.CoordinatorDutyFrontiers) []*metapb.RootDutyFrontier {
	if frontiers.Len() == 0 {
		return nil
	}
	entries := frontiers.Entries()
	out := make([]*metapb.RootDutyFrontier, 0, len(entries))
	for _, entry := range entries {
		out = append(out, &metapb.RootDutyFrontier{DutyMask: entry.DutyMask, Frontier: entry.Frontier})
	}
	return out
}

func RootDutyFrontiersFromProto(frontiers []*metapb.RootDutyFrontier) rootproto.CoordinatorDutyFrontiers {
	entries := make([]rootproto.CoordinatorDutyFrontier, 0, len(frontiers))
	for _, entry := range frontiers {
		if entry == nil || entry.GetDutyMask() == 0 {
			continue
		}
		entries = append(entries, rootproto.CoordinatorDutyFrontier{
			DutyMask: entry.GetDutyMask(),
			Frontier: entry.GetFrontier(),
		})
	}
	return rootproto.NewCoordinatorDutyFrontiers(entries...)
}

func RootCoordinatorProtocolStateToProto(state rootstate.CoordinatorProtocolState) *metapb.RootCoordinatorProtocolState {
	return &metapb.RootCoordinatorProtocolState{
		Lease:   RootCoordinatorLeaseToProto(state.Lease),
		Seal:    RootCoordinatorSealToProto(state.Seal),
		Closure: RootCoordinatorClosureToProto(state.Closure),
	}
}

func RootCoordinatorProtocolStateFromProto(state *metapb.RootCoordinatorProtocolState) rootstate.CoordinatorProtocolState {
	if state == nil {
		return rootstate.CoordinatorProtocolState{}
	}
	return rootstate.CoordinatorProtocolState{
		Lease:   RootCoordinatorLeaseFromProto(state.GetLease()),
		Seal:    RootCoordinatorSealFromProto(state.GetSeal()),
		Closure: RootCoordinatorClosureFromProto(state.GetClosure()),
	}
}

func RootCoordinatorLeaseCommandToProto(cmd rootproto.CoordinatorLeaseCommand) *metapb.RootCoordinatorLeaseCommand {
	return &metapb.RootCoordinatorLeaseCommand{
		Kind:              rootCoordinatorLeaseCommandKindToProto(cmd.Kind),
		HolderId:          cmd.HolderID,
		ExpiresUnixNano:   cmd.ExpiresUnixNano,
		NowUnixNano:       cmd.NowUnixNano,
		PredecessorDigest: cmd.PredecessorDigest,
		HandoffFrontiers:  RootDutyFrontiersToProto(cmd.HandoffFrontiers),
	}
}

func RootCoordinatorLeaseCommandFromProto(cmd *metapb.RootCoordinatorLeaseCommand) rootproto.CoordinatorLeaseCommand {
	if cmd == nil {
		return rootproto.CoordinatorLeaseCommand{}
	}
	return rootproto.CoordinatorLeaseCommand{
		Kind:              rootCoordinatorLeaseCommandKindFromProto(cmd.GetKind()),
		HolderID:          cmd.GetHolderId(),
		ExpiresUnixNano:   cmd.GetExpiresUnixNano(),
		NowUnixNano:       cmd.GetNowUnixNano(),
		PredecessorDigest: cmd.GetPredecessorDigest(),
		HandoffFrontiers:  RootDutyFrontiersFromProto(cmd.GetHandoffFrontiers()),
	}
}

func RootCoordinatorClosureCommandToProto(cmd rootproto.CoordinatorClosureCommand) *metapb.RootCoordinatorClosureCommand {
	return &metapb.RootCoordinatorClosureCommand{
		Kind:        rootCoordinatorClosureCommandKindToProto(cmd.Kind),
		HolderId:    cmd.HolderID,
		NowUnixNano: cmd.NowUnixNano,
		Frontiers:   RootDutyFrontiersToProto(cmd.Frontiers),
	}
}

func RootCoordinatorClosureCommandFromProto(cmd *metapb.RootCoordinatorClosureCommand) rootproto.CoordinatorClosureCommand {
	if cmd == nil {
		return rootproto.CoordinatorClosureCommand{}
	}
	return rootproto.CoordinatorClosureCommand{
		Kind:        rootCoordinatorClosureCommandKindFromProto(cmd.GetKind()),
		HolderID:    cmd.GetHolderId(),
		NowUnixNano: cmd.GetNowUnixNano(),
		Frontiers:   RootDutyFrontiersFromProto(cmd.GetFrontiers()),
	}
}

func rootEventCoordinatorLeaseToProto(lease *rootevent.CoordinatorLease) *metapb.RootCoordinatorLease {
	if lease == nil {
		return nil
	}
	return &metapb.RootCoordinatorLease{
		HolderId:          lease.HolderID,
		ExpiresUnixNano:   lease.ExpiresUnixNano,
		CertGeneration:    lease.CertGeneration,
		IssuedCursor:      RootCursorToProto(lease.IssuedCursor),
		DutyMask:          lease.DutyMask,
		PredecessorDigest: lease.PredecessorDigest,
		HandoffFrontiers:  RootDutyFrontiersToProto(lease.Frontiers),
	}
}

func rootEventCoordinatorLeaseFromProto(lease *metapb.RootCoordinatorLease) *rootevent.CoordinatorLease {
	if lease == nil {
		return nil
	}
	return &rootevent.CoordinatorLease{
		HolderID:          lease.GetHolderId(),
		ExpiresUnixNano:   lease.GetExpiresUnixNano(),
		CertGeneration:    lease.GetCertGeneration(),
		IssuedCursor:      RootCursorFromProto(lease.GetIssuedCursor()),
		DutyMask:          lease.GetDutyMask(),
		PredecessorDigest: lease.GetPredecessorDigest(),
		Frontiers:         RootDutyFrontiersFromProto(lease.GetHandoffFrontiers()),
	}
}

func rootEventCoordinatorSealToProto(seal *rootevent.CoordinatorSeal) *metapb.RootCoordinatorSeal {
	if seal == nil {
		return nil
	}
	return &metapb.RootCoordinatorSeal{
		HolderId:          seal.HolderID,
		CertGeneration:    seal.CertGeneration,
		DutyMask:          seal.DutyMask,
		ConsumedFrontiers: RootDutyFrontiersToProto(seal.Frontiers),
		SealedAtCursor:    RootCursorToProto(seal.SealedAtCursor),
	}
}

func rootEventCoordinatorSealFromProto(seal *metapb.RootCoordinatorSeal) *rootevent.CoordinatorSeal {
	if seal == nil {
		return nil
	}
	return &rootevent.CoordinatorSeal{
		HolderID:       seal.GetHolderId(),
		CertGeneration: seal.GetCertGeneration(),
		DutyMask:       seal.GetDutyMask(),
		Frontiers:      RootDutyFrontiersFromProto(seal.GetConsumedFrontiers()),
		SealedAtCursor: RootCursorFromProto(seal.GetSealedAtCursor()),
	}
}

func rootEventCoordinatorClosureToProto(closure *rootevent.CoordinatorClosure) *metapb.RootCoordinatorClosure {
	if closure == nil {
		return nil
	}
	return &metapb.RootCoordinatorClosure{
		HolderId:            closure.HolderID,
		SealGeneration:      closure.SealGeneration,
		SuccessorGeneration: closure.SuccessorGeneration,
		SealDigest:          closure.SealDigest,
		Stage:               rootCoordinatorClosureStageToProto(closure.Stage),
		ConfirmedAtCursor:   RootCursorToProto(closure.ConfirmedAtCursor),
		ClosedAtCursor:      RootCursorToProto(closure.ClosedAtCursor),
		ReattachedAtCursor:  RootCursorToProto(closure.ReattachedAtCursor),
	}
}

func rootEventCoordinatorClosureFromProto(closure *metapb.RootCoordinatorClosure) *rootevent.CoordinatorClosure {
	if closure == nil {
		return nil
	}
	return &rootevent.CoordinatorClosure{
		HolderID:            closure.GetHolderId(),
		SealGeneration:      closure.GetSealGeneration(),
		SuccessorGeneration: closure.GetSuccessorGeneration(),
		SealDigest:          closure.GetSealDigest(),
		Stage:               rootCoordinatorClosureStageFromProto(closure.GetStage()),
		ConfirmedAtCursor:   RootCursorFromProto(closure.GetConfirmedAtCursor()),
		ClosedAtCursor:      RootCursorFromProto(closure.GetClosedAtCursor()),
		ReattachedAtCursor:  RootCursorFromProto(closure.GetReattachedAtCursor()),
	}
}

func rootCoordinatorClosureStageToProto(stage rootproto.CoordinatorClosureStage) metapb.RootCoordinatorClosureStage {
	switch stage {
	case rootproto.CoordinatorClosureStageConfirmed:
		return metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_CONFIRMED
	case rootproto.CoordinatorClosureStageClosed:
		return metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_CLOSED
	case rootproto.CoordinatorClosureStageReattached:
		return metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_REATTACHED
	default:
		return metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_PENDING_CONFIRM
	}
}

func rootCoordinatorLeaseCommandKindToProto(kind rootproto.CoordinatorLeaseCommandKind) metapb.RootCoordinatorLeaseCommandKind {
	switch kind {
	case rootproto.CoordinatorLeaseCommandIssue:
		return metapb.RootCoordinatorLeaseCommandKind_ROOT_COORDINATOR_LEASE_COMMAND_KIND_ISSUE
	case rootproto.CoordinatorLeaseCommandRelease:
		return metapb.RootCoordinatorLeaseCommandKind_ROOT_COORDINATOR_LEASE_COMMAND_KIND_RELEASE
	default:
		return metapb.RootCoordinatorLeaseCommandKind_ROOT_COORDINATOR_LEASE_COMMAND_KIND_UNSPECIFIED
	}
}

func rootCoordinatorLeaseCommandKindFromProto(kind metapb.RootCoordinatorLeaseCommandKind) rootproto.CoordinatorLeaseCommandKind {
	switch kind {
	case metapb.RootCoordinatorLeaseCommandKind_ROOT_COORDINATOR_LEASE_COMMAND_KIND_ISSUE:
		return rootproto.CoordinatorLeaseCommandIssue
	case metapb.RootCoordinatorLeaseCommandKind_ROOT_COORDINATOR_LEASE_COMMAND_KIND_RELEASE:
		return rootproto.CoordinatorLeaseCommandRelease
	default:
		return rootproto.CoordinatorLeaseCommandUnknown
	}
}

func rootCoordinatorClosureCommandKindToProto(kind rootproto.CoordinatorClosureCommandKind) metapb.RootCoordinatorClosureCommandKind {
	switch kind {
	case rootproto.CoordinatorClosureCommandSeal:
		return metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_SEAL
	case rootproto.CoordinatorClosureCommandConfirm:
		return metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_CONFIRM
	case rootproto.CoordinatorClosureCommandClose:
		return metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_CLOSE
	case rootproto.CoordinatorClosureCommandReattach:
		return metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_REATTACH
	default:
		return metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_UNSPECIFIED
	}
}

func rootCoordinatorClosureCommandKindFromProto(kind metapb.RootCoordinatorClosureCommandKind) rootproto.CoordinatorClosureCommandKind {
	switch kind {
	case metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_SEAL:
		return rootproto.CoordinatorClosureCommandSeal
	case metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_CONFIRM:
		return rootproto.CoordinatorClosureCommandConfirm
	case metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_CLOSE:
		return rootproto.CoordinatorClosureCommandClose
	case metapb.RootCoordinatorClosureCommandKind_ROOT_COORDINATOR_CLOSURE_COMMAND_KIND_REATTACH:
		return rootproto.CoordinatorClosureCommandReattach
	default:
		return rootproto.CoordinatorClosureCommandUnknown
	}
}

func rootCoordinatorClosureStageFromProto(stage metapb.RootCoordinatorClosureStage) rootproto.CoordinatorClosureStage {
	switch stage {
	case metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_CONFIRMED:
		return rootproto.CoordinatorClosureStageConfirmed
	case metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_CLOSED:
		return rootproto.CoordinatorClosureStageClosed
	case metapb.RootCoordinatorClosureStage_ROOT_COORDINATOR_CLOSURE_STAGE_REATTACHED:
		return rootproto.CoordinatorClosureStageReattached
	default:
		return rootproto.CoordinatorClosureStagePendingConfirm
	}
}

func RootSnapshotToProto(snapshot rootstate.Snapshot, tailOffset uint64) *metapb.RootCheckpoint {
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
		pbEvent.Payload = &metapb.RootEvent_StoreMembership{StoreMembership: &metapb.RootStoreMembership{StoreId: event.StoreMembership.StoreID, Address: event.StoreMembership.Address}}
	case event.AllocatorFence != nil:
		pbEvent.Payload = &metapb.RootEvent_AllocatorFence{AllocatorFence: &metapb.RootAllocatorFence{Minimum: event.AllocatorFence.Minimum}}
	case event.CoordinatorLease != nil:
		pbEvent.Payload = &metapb.RootEvent_CoordinatorLease{CoordinatorLease: rootEventCoordinatorLeaseToProto(event.CoordinatorLease)}
	case event.CoordinatorSeal != nil:
		pbEvent.Payload = &metapb.RootEvent_CoordinatorSeal{CoordinatorSeal: rootEventCoordinatorSealToProto(event.CoordinatorSeal)}
	case event.CoordinatorClosure != nil:
		pbEvent.Payload = &metapb.RootEvent_CoordinatorClosure{CoordinatorClosure: rootEventCoordinatorClosureToProto(event.CoordinatorClosure)}
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
		event.StoreMembership = &rootevent.StoreMembership{StoreID: body.StoreId, Address: body.Address}
	}
	if body := pbEvent.GetAllocatorFence(); body != nil {
		event.AllocatorFence = &rootevent.AllocatorFence{Minimum: body.Minimum}
	}
	if body := pbEvent.GetCoordinatorLease(); body != nil {
		event.CoordinatorLease = rootEventCoordinatorLeaseFromProto(body)
	}
	if body := pbEvent.GetCoordinatorSeal(); body != nil {
		event.CoordinatorSeal = rootEventCoordinatorSealFromProto(body)
	}
	if body := pbEvent.GetCoordinatorClosure(); body != nil {
		event.CoordinatorClosure = rootEventCoordinatorClosureFromProto(body)
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
	case rootevent.KindStoreLeft:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT
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
	case rootevent.KindCoordinatorLease:
		return metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_LEASE
	case rootevent.KindCoordinatorSeal:
		return metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_SEAL
	case rootevent.KindCoordinatorClosure:
		return metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_CLOSURE
	default:
		return metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED
	}
}

func rootEventKindFromProto(kind metapb.RootEventKind) rootevent.Kind {
	switch kind {
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED:
		return rootevent.KindStoreJoined
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT:
		return rootevent.KindStoreLeft
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
	case metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_LEASE:
		return rootevent.KindCoordinatorLease
	case metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_SEAL:
		return rootevent.KindCoordinatorSeal
	case metapb.RootEventKind_ROOT_EVENT_KIND_COORDINATOR_CLOSURE:
		return rootevent.KindCoordinatorClosure
	default:
		return rootevent.KindUnknown
	}
}
