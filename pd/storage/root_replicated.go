package storage

import (
	"fmt"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/backend/replicated"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const ReplicatedRootReplicaCount = 3

// ReplicatedRootConfig describes one experimental fixed-cluster replicated
// metadata root node hosted by PD.
type ReplicatedRootConfig struct {
	WorkDir       string
	NodeID        uint64
	ClusterIDs    []uint64
	TransportAddr string
	PeerAddrs     map[uint64]string
}

type replicatedRootEntry struct {
	ids     []uint64
	cluster *rootreplicated.FixedCluster
}

var replicatedRootRegistry struct {
	mu       sync.Mutex
	clusters map[string]replicatedRootEntry
}

// OpenRootReplicatedStore opens one PD storage backend backed by the
// experimental fixed-cluster replicated metadata root.
func OpenRootReplicatedStore(cfg ReplicatedRootConfig) (*RootStore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.UsesTransport() {
		transport, err := rootreplicated.NewGRPCTransport(cfg.NodeID, cfg.TransportAddr)
		if err != nil {
			return nil, err
		}
		transport.SetPeers(cfg.PeerAddrs)
		driver, err := rootreplicated.NewNetworkDriver(rootreplicated.NetworkConfig{
			ID:        cfg.NodeID,
			PeerIDs:   cfg.ClusterIDs,
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
	cluster, err := getOrCreateReplicatedCluster(cfg.WorkDir, cfg.ClusterIDs)
	if err != nil {
		return nil, err
	}
	driver, err := cluster.Driver(cfg.NodeID)
	if err != nil {
		return nil, err
	}
	root, err := rootreplicated.Open(rootreplicated.Config{Driver: driver})
	if err != nil {
		return nil, err
	}
	return OpenRootStore(root)
}

func (cfg ReplicatedRootConfig) UsesTransport() bool {
	return strings.TrimSpace(cfg.TransportAddr) != "" || len(cfg.PeerAddrs) > 0
}

func (cfg ReplicatedRootConfig) Validate() error {
	if strings.TrimSpace(cfg.WorkDir) == "" {
		return fmt.Errorf("pd/storage: workdir is required for replicated root mode")
	}
	if cfg.NodeID == 0 {
		return fmt.Errorf("pd/storage: replicated root node id must be > 0")
	}
	if len(cfg.ClusterIDs) != ReplicatedRootReplicaCount {
		return fmt.Errorf("pd/storage: replicated root mode requires exactly %d cluster ids", ReplicatedRootReplicaCount)
	}
	if cfg.UsesTransport() {
		if strings.TrimSpace(cfg.TransportAddr) == "" {
			return fmt.Errorf("pd/storage: replicated root transport mode requires transport address")
		}
		if len(cfg.PeerAddrs) != ReplicatedRootReplicaCount {
			return fmt.Errorf("pd/storage: replicated root transport mode requires exactly %d peer addresses", ReplicatedRootReplicaCount)
		}
		for _, id := range cfg.ClusterIDs {
			if strings.TrimSpace(cfg.PeerAddrs[id]) == "" {
				return fmt.Errorf("pd/storage: missing replicated root peer address for node %d", id)
			}
		}
	}
	return nil
}

func ParseReplicatedRootClusterIDs(raw string) ([]uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []uint64{1, 2, 3}, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]uint64, 0, len(parts))
	seen := make(map[uint64]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse root cluster id %q: %w", part, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("root cluster ids must be > 0")
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate root cluster id %d", id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("root cluster must contain at least one node id")
	}
	return out, nil
}

func getOrCreateReplicatedCluster(workdir string, clusterIDs []uint64) (*rootreplicated.FixedCluster, error) {
	replicatedRootRegistry.mu.Lock()
	defer replicatedRootRegistry.mu.Unlock()
	if replicatedRootRegistry.clusters == nil {
		replicatedRootRegistry.clusters = make(map[string]replicatedRootEntry)
	}
	if entry, ok := replicatedRootRegistry.clusters[workdir]; ok {
		if !slices.Equal(entry.ids, clusterIDs) {
			return nil, fmt.Errorf("pd/storage: replicated root cluster ids for %q changed from %v to %v", workdir, entry.ids, clusterIDs)
		}
		return entry.cluster, nil
	}
	cluster, err := rootreplicated.NewFixedCluster(clusterIDs...)
	if err != nil {
		return nil, err
	}
	replicatedRootRegistry.clusters[workdir] = replicatedRootEntry{
		ids:     slices.Clone(clusterIDs),
		cluster: cluster,
	}
	return cluster, nil
}
