package store

import (
	"errors"
	"fmt"
)

var (
	errNilStore                               = errors.New("raftstore: store is nil")
	errZeroRegionID                           = errors.New("raftstore: region id is zero")
	errZeroPeerID                             = errors.New("raftstore: peer id is zero")
	errNilPeerConfig                          = errors.New("raftstore: peer config is nil")
	errNilRouter                              = errors.New("raftstore: router is nil")
	errNilRaftCommandRequest                  = errors.New("raftstore: nil raft command request")
	errCommandApplyWithoutHandler             = errors.New("raftstore: command apply without handler")
	errNilCommand                             = errors.New("raftstore: command is nil")
	errRegionIDMissing                        = errors.New("raftstore: region id missing")
	errCommandPipelineUnavailable             = errors.New("raftstore: command pipeline unavailable")
	errReadCommandMissingRequests             = errors.New("raftstore: read command missing requests")
	errReadCommandNotReadOnly                 = errors.New("raftstore: read command must be read-only")
	errParentRegionIDZero                     = errors.New("raftstore: parent region id is zero")
	errChildRegionIDZero                      = errors.New("raftstore: child region id is zero")
	errChildRegionStartKeyRequired            = errors.New("raftstore: child region start key required")
	errSplitCommandMissingPayload             = errors.New("raftstore: split command missing payload")
	errMergeCommandMissingPayload             = errors.New("raftstore: merge command missing payload")
	errRegionManagerNil                       = errors.New("raftstore: region manager nil")
	errInvalidPeerChangeTarget                = errors.New("raftstore: invalid peer change target")
	errInvalidRegionIdentifiers               = errors.New("raftstore: invalid region identifiers")
	errRegionMetaNil                          = errors.New("raftstore: region meta is nil")
	errInvalidConfChangeContext               = errors.New("raftstore: invalid conf change context")
	errTransitionRegionIDZero                 = errors.New("raftstore: transition region id is zero")
	errTransitionProposalEmpty                = errors.New("raftstore: transition proposal is empty")
	errRaftMessageMissingRecipient            = errors.New("raftstore: raft message missing recipient")
	errSnapshotPeerBootstrapRequiresSnapshot  = errors.New("raftstore: snapshot peer bootstrap requires non-empty snapshot message")
	errSnapshotPeerBootstrapRequiresPeerBuild = errors.New("raftstore: snapshot peer bootstrap requires peer builder")
	errSnapshotPayloadMissingRegionMeta       = errors.New("raftstore: snapshot payload missing region metadata")
	errInstallRegionSnapshotRequiresSnapshot  = errors.New("raftstore: install region snapshot requires non-empty snapshot")
	errInstallRegionSnapshotRequiresPayload   = errors.New("raftstore: install region snapshot requires snapshot payload")
	errInstallRegionSnapshotRequiresPeerBuild = errors.New("raftstore: install region snapshot requires peer builder")
	errInstallRegionSSTRequiresSnapshot       = errors.New("raftstore: install region sst snapshot requires non-empty snapshot")
	errInstallRegionSSTRequiresRegionMeta     = errors.New("raftstore: install region sst snapshot requires region metadata")
	errInstallRegionSSTRequiresCallback       = errors.New("raftstore: install region sst snapshot requires install callback")
	errInstallRegionSSTRequiresPeerBuild      = errors.New("raftstore: install region sst snapshot requires peer builder")
	errFailpointAfterSnapshotApply            = errors.New("raftstore: failpoint after snapshot apply before publish")
	errRouterRegisterNilPeer                  = errors.New("raftstore: router cannot register nil peer")
)

func IsNilStore(err error) bool     { return errors.Is(err, errNilStore) }
func IsZeroRegionID(err error) bool { return errors.Is(err, errZeroRegionID) }
func IsZeroPeerID(err error) bool   { return errors.Is(err, errZeroPeerID) }

func errPeerNotFound(id uint64) error { return fmt.Errorf("raftstore: peer %d not found", id) }
func errPeerAlreadyRegistered(id uint64) error {
	return fmt.Errorf("raftstore: peer %d already registered", id)
}
func errRegionNotFound(id uint64) error { return fmt.Errorf("raftstore: region %d not found", id) }
func errParentRegionNotFound(id uint64) error {
	return fmt.Errorf("raftstore: parent region %d not found", id)
}
func errTargetRegionNotFound(id uint64) error {
	return fmt.Errorf("raftstore: target region %d not found", id)
}
func errSourceRegionNotFound(id uint64) error {
	return fmt.Errorf("raftstore: source region %d not found", id)
}
func errRegionMetadataNotFound(id uint64) error {
	return fmt.Errorf("raftstore: region %d metadata not found", id)
}
func errInvalidRegionPeerID(regionID, peerID uint64) error {
	return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, peerID)
}
func errPeerBuilderReturnedNilConfig(regionID uint64) error {
	return fmt.Errorf("raftstore: peer builder returned nil config for region %d", regionID)
}
func errRegionDoesNotAssignPeer(regionID, storeID uint64) error {
	return fmt.Errorf("raftstore: region %d does not assign a peer to store %d", regionID, storeID)
}
func errPeerAlreadyHosted(peerID, regionID uint64) error {
	return fmt.Errorf("raftstore: peer %d already hosted for region %d", peerID, regionID)
}
