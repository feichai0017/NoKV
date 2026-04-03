package storage

import (
	"fmt"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/backend/replicated"
	"maps"
	"slices"
	"strings"
)

const ReplicatedRootReplicaCount = 3

// ReplicatedRootConfig describes one fixed three-replica transport-backed
// metadata root node hosted by PD.
type ReplicatedRootConfig struct {
	NodeID        uint64
	TransportAddr string
	PeerAddrs     map[uint64]string
}

// OpenRootReplicatedStore opens one PD storage backend backed by the
// fixed three-replica transport-backed metadata root.
func OpenRootReplicatedStore(cfg ReplicatedRootConfig) (*RootStore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	transport, err := rootreplicated.NewGRPCTransport(cfg.NodeID, cfg.TransportAddr)
	if err != nil {
		return nil, err
	}
	transport.SetPeers(cfg.PeerAddrs)
	driver, err := rootreplicated.NewNetworkDriver(rootreplicated.NetworkConfig{
		ID:        cfg.NodeID,
		PeerIDs:   cfg.ClusterIDs(),
		Transport: transport,
	})
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	root, err := rootreplicated.Open(rootreplicated.Config{Driver: driver})
	if err != nil {
		_ = driver.Close()
		return nil, err
	}
	return OpenRootStore(root)
}

func (cfg ReplicatedRootConfig) Validate() error {
	if cfg.NodeID == 0 {
		return fmt.Errorf("pd/storage: replicated root node id must be > 0")
	}
	if strings.TrimSpace(cfg.TransportAddr) == "" {
		return fmt.Errorf("pd/storage: replicated root mode requires transport address")
	}
	if len(cfg.PeerAddrs) != ReplicatedRootReplicaCount {
		return fmt.Errorf("pd/storage: replicated root mode requires exactly %d peer addresses", ReplicatedRootReplicaCount)
	}
	for id, addr := range cfg.PeerAddrs {
		if id == 0 {
			return fmt.Errorf("pd/storage: replicated root peer ids must be > 0")
		}
		if strings.TrimSpace(addr) == "" {
			return fmt.Errorf("pd/storage: missing replicated root peer address for node %d", id)
		}
	}
	if strings.TrimSpace(cfg.PeerAddrs[cfg.NodeID]) == "" {
		return fmt.Errorf("pd/storage: missing replicated root peer address for local node %d", cfg.NodeID)
	}
	return nil
}

func (cfg ReplicatedRootConfig) ClusterIDs() []uint64 {
	ids := slices.Collect(maps.Keys(cfg.PeerAddrs))
	slices.Sort(ids)
	return ids
}
