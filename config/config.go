package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// File models the raft topology configuration shared by CLIs and gateways.
type File struct {
	MaxRetries                 int      `json:"max_retries"`
	PD                         *PD      `json:"pd,omitempty"`
	StoreWorkDirTemplate       string   `json:"store_work_dir_template,omitempty"`
	StoreDockerWorkDirTemplate string   `json:"store_docker_work_dir_template,omitempty"`
	Stores                     []Store  `json:"stores"`
	Regions                    []Region `json:"regions"`
}

// PD describes PD-lite endpoints for host and docker scopes.
type PD struct {
	Addr          string `json:"addr"`
	DockerAddr    string `json:"docker_addr,omitempty"`
	WorkDir       string `json:"work_dir,omitempty"`
	DockerWorkDir string `json:"docker_work_dir,omitempty"`
}

// Store represents a single store endpoint.
type Store struct {
	StoreID          uint64 `json:"store_id"`
	Addr             string `json:"addr"`
	ListenAddr       string `json:"listen_addr"`
	DockerAddr       string `json:"docker_addr"`
	DockerListenAddr string `json:"docker_listen_addr"`
	WorkDir          string `json:"work_dir,omitempty"`
	DockerWorkDir    string `json:"docker_work_dir,omitempty"`
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
	if v := strings.TrimSpace(f.StoreWorkDirTemplate); v != "" && !strings.Contains(v, "{id}") {
		return fmt.Errorf("config: store_work_dir_template must contain {id}")
	}
	if v := strings.TrimSpace(f.StoreDockerWorkDirTemplate); v != "" && !strings.Contains(v, "{id}") {
		return fmt.Errorf("config: store_docker_work_dir_template must contain {id}")
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

// ResolvePDAddr resolves the PD endpoint for the provided scope.
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolvePDAddr(scope string) string {
	if f == nil || f.PD == nil {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(scope), "docker") {
		if v := strings.TrimSpace(f.PD.DockerAddr); v != "" {
			return v
		}
	}
	return strings.TrimSpace(f.PD.Addr)
}

// ResolvePDWorkDir resolves the PD work directory for the provided scope.
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolvePDWorkDir(scope string) string {
	if f == nil || f.PD == nil {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(scope), "docker") {
		if v := strings.TrimSpace(f.PD.DockerWorkDir); v != "" {
			return v
		}
	}
	return strings.TrimSpace(f.PD.WorkDir)
}

// ResolveStoreWorkDir resolves the work directory for the given store and scope.
//
// Resolution order:
//  1. Store-scoped override (work_dir/docker_work_dir)
//  2. Global template (store_work_dir_template/store_docker_work_dir_template)
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolveStoreWorkDir(storeID uint64, scope string) string {
	if f == nil || storeID == 0 {
		return ""
	}
	var store *Store
	for i := range f.Stores {
		if f.Stores[i].StoreID == storeID {
			store = &f.Stores[i]
			break
		}
	}
	if store == nil {
		return ""
	}
	scopeNorm := strings.ToLower(strings.TrimSpace(scope))
	if scopeNorm == "docker" {
		if v := strings.TrimSpace(store.DockerWorkDir); v != "" {
			return v
		}
		if v := strings.TrimSpace(store.WorkDir); v != "" {
			return v
		}
		if v := strings.TrimSpace(f.StoreDockerWorkDirTemplate); v != "" {
			return resolveStoreDirTemplate(v, storeID)
		}
		if v := strings.TrimSpace(f.StoreWorkDirTemplate); v != "" {
			return resolveStoreDirTemplate(v, storeID)
		}
		return ""
	}

	if v := strings.TrimSpace(store.WorkDir); v != "" {
		return v
	}
	if v := strings.TrimSpace(f.StoreWorkDirTemplate); v != "" {
		return resolveStoreDirTemplate(v, storeID)
	}
	return ""
}

func resolveStoreDirTemplate(template string, storeID uint64) string {
	template = strings.TrimSpace(template)
	if template == "" {
		return ""
	}
	return strings.ReplaceAll(template, "{id}", fmt.Sprintf("%d", storeID))
}
