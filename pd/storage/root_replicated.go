package storage

import (
	"fmt"
	rootreplicated "github.com/feichai0017/NoKV/meta/root/backend/replicated"
	"slices"
	"sync"
)

// ReplicatedRootConfig describes one experimental fixed-cluster replicated
// metadata root node hosted by PD.
type ReplicatedRootConfig struct {
	WorkDir    string
	NodeID     uint64
	ClusterIDs []uint64
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
	if cfg.WorkDir == "" {
		return nil, fmt.Errorf("pd/storage: workdir is required for replicated root mode")
	}
	if cfg.NodeID == 0 {
		return nil, fmt.Errorf("pd/storage: replicated root node id must be > 0")
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
