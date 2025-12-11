package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// File models the raft topology configuration shared by CLIs and gateways.
type File struct {
	MaxRetries int      `json:"max_retries"`
	TSO        *TSO     `json:"tso"`
	Stores     []Store  `json:"stores"`
	Regions    []Region `json:"regions"`
}

// TSO describes the timestamp service endpoints.
type TSO struct {
	ListenAddr   string `json:"listen_addr"`
	AdvertiseURL string `json:"advertise_url"`
}

// Store represents a single store endpoint.
type Store struct {
	StoreID          uint64 `json:"store_id"`
	Addr             string `json:"addr"`
	ListenAddr       string `json:"listen_addr"`
	DockerAddr       string `json:"docker_addr"`
	DockerListenAddr string `json:"docker_listen_addr"`
}

// Region defines a key range and its peer set.
type Region struct {
	ID            uint64      `json:"id"`
	StartKey      string      `json:"start_key"`
	EndKey        string      `json:"end_key"`
	Epoch         RegionEpoch `json:"epoch"`
	Peers         []Peer      `json:"peers"`
	LeaderStoreID uint64      `json:"leader_store_id"`
}

// RegionEpoch tracks metadata versions.
type RegionEpoch struct {
	Version     uint64 `json:"version"`
	ConfVersion uint64 `json:"conf_version"`
}

// Peer binds a peer ID to a store.
type Peer struct {
	StoreID uint64 `json:"store_id"`
	PeerID  uint64 `json:"peer_id"`
}

// LoadFile parses a config file from disk.
func LoadFile(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg File
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate performs lightweight consistency checks.
func (f *File) Validate() error {
	if f == nil {
		return fmt.Errorf("config: nil file")
	}
	storeIDs := make(map[uint64]struct{}, len(f.Stores))
	for _, st := range f.Stores {
		if st.StoreID == 0 {
			return fmt.Errorf("config: store_id must be > 0")
		}
		if _, dup := storeIDs[st.StoreID]; dup {
			return fmt.Errorf("config: duplicate store_id %d", st.StoreID)
		}
		storeIDs[st.StoreID] = struct{}{}
	}
	for _, region := range f.Regions {
		if region.ID == 0 {
			return fmt.Errorf("config: region id must be > 0")
		}
		if region.LeaderStoreID != 0 {
			if _, ok := storeIDs[region.LeaderStoreID]; !ok {
				return fmt.Errorf("config: region %d leader store %d missing from stores", region.ID, region.LeaderStoreID)
			}
		}
		for _, peer := range region.Peers {
			if peer.StoreID == 0 || peer.PeerID == 0 {
				return fmt.Errorf("config: region %d peer requires store_id and peer_id", region.ID)
			}
			if _, ok := storeIDs[peer.StoreID]; !ok {
				return fmt.Errorf("config: region %d references unknown store %d", region.ID, peer.StoreID)
			}
		}
	}
	return nil
}
