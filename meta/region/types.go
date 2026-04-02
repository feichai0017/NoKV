package region

// Peer describes one replica peer in a region membership set.
type Peer struct {
	StoreID uint64 `json:"store_id"`
	PeerID  uint64 `json:"peer_id"`
}

// ReplicaState enumerates the lifecycle state of one region replica.
type ReplicaState uint8

const (
	ReplicaStateNew ReplicaState = iota
	ReplicaStateRunning
	ReplicaStateRemoving
	ReplicaStateTombstone
)

// Epoch tracks versioned region topology.
type Epoch struct {
	Version     uint64 `json:"version"`
	ConfVersion uint64 `json:"conf_version"`
}
