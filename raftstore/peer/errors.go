package peer

import (
	"errors"
	"fmt"
)

var (
	// errPeerStopped indicates that the raft peer has already shut down.
	errPeerStopped = errors.New("raftstore: peer stopped")
	// errNilConfig indicates that peer construction received a nil config.
	errNilConfig = errors.New("raftstore: config is nil")
	// errNilTransport indicates that peer construction needs a transport implementation.
	errNilTransport = errors.New("raftstore: transport must be provided")
	// errNilApplyFunc indicates that peer construction needs an apply callback.
	errNilApplyFunc = errors.New("raftstore: apply function must be provided")
	// errZeroRaftID indicates that peer construction needs a non-zero raft id.
	errZeroRaftID                                   = errors.New("raftstore: raft config must specify ID")
	errEmptyAdminCommand                            = errors.New("raftstore: empty admin command")
	errZeroTransferTarget                           = errors.New("raftstore: transfer target must be non-zero")
	errSnapshotRequiresStorage                      = errors.New("raftstore: peer snapshot requires storage")
	errSnapshotExportRequiresRegionMeta             = errors.New("raftstore: snapshot export requires region metadata")
	errSnapshotPayloadExportRequiresRegionMeta      = errors.New("raftstore: snapshot payload export requires region metadata")
	errSnapshotPayloadInstallRequiresEmptyPeerState = errors.New("raftstore: snapshot payload install requires empty peer state")
	errSnapshotPayloadInstallRequiresEmptyPeerLog   = errors.New("raftstore: snapshot payload install requires empty peer log")
	errNilPeer                                      = errors.New("raftstore: peer is nil")
)

func IsPeerStopped(err error) bool { return errors.Is(err, errPeerStopped) }
func IsNilPeer(err error) bool     { return errors.Is(err, errNilPeer) }

func errAdminCommandPayloadTooShort() error {
	return fmt.Errorf("raftstore: admin command payload too short")
}
func errExportSnapshotPayload(err error) error {
	return fmt.Errorf("raftstore: export snapshot payload: %w", err)
}
