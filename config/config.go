package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const (
	scopeHost   = "host"
	scopeDocker = "docker"
)

// File models the raft topology configuration shared by CLIs and gateways.
//
// The file has two kinds of content, with different lifecycles:
//
//   - Address/topology layer (MetaRoot, Coordinator, Stores): live address
//     directory consumed at every CLI invocation. Keep this in sync with what
//     you have actually deployed; otherwise coordinators and gateways cannot
//     dial.
//   - Bootstrap-only layer (Regions): consumed by `scripts/ops/bootstrap.sh`
//     on first startup to seed fresh store workdirs. Once a store has a
//     `CURRENT` manifest, bootstrap skips it and the `Regions` section is
//     ignored. Runtime topology (splits, merges, peer changes) lives in
//     meta-root, not here; use `nokv-config regions` or ccc-audit to inspect
//     current state.
type File struct {
	MaxRetries                 int          `json:"max_retries"`
	MetaRoot                   *MetaRoot    `json:"meta_root,omitempty"`
	Coordinator                *Coordinator `json:"coordinator,omitempty"`
	StoreWorkDirTemplate       string       `json:"store_work_dir_template,omitempty"`
	StoreDockerWorkDirTemplate string       `json:"store_docker_work_dir_template,omitempty"`
	Stores                     []Store      `json:"stores"`
	Regions                    []Region     `json:"regions"`
}

// MetaRoot describes the 3-peer replicated meta-root cluster. NoKV only
// supports the replicated topology, so the peer count is fixed at 3 at
// validation time.
type MetaRoot struct {
	Peers []MetaRootPeer `json:"peers"`
}

// MetaRootPeer binds one meta-root peer's identity to its gRPC service
// address (dialed by coordinator and ccc-audit), raft transport address
// (dialed by sibling meta-root peers), and on-disk workdir. Each field has
// host / docker variants resolved by the --scope flag.
type MetaRootPeer struct {
	NodeID              uint64 `json:"node_id"`
	Addr                string `json:"addr"`
	DockerAddr          string `json:"docker_addr,omitempty"`
	TransportAddr       string `json:"transport_addr"`
	DockerTransportAddr string `json:"docker_transport_addr,omitempty"`
	WorkDir             string `json:"work_dir,omitempty"`
	DockerWorkDir       string `json:"docker_work_dir,omitempty"`
}

// Coordinator describes coordinator endpoints for host and docker scopes.
type Coordinator struct {
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

func (f *File) resolveStore(storeID uint64) *Store {
	if f == nil || storeID == 0 {
		return nil
	}
	for i := range f.Stores {
		if f.Stores[i].StoreID == storeID {
			return &f.Stores[i]
		}
	}
	return nil
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
	if err := f.validateMetaRoot(); err != nil {
		return err
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

// validateMetaRoot ensures the meta_root section, if present, describes a
// well-formed 3-peer replicated cluster. Callers that do not need meta-root
// resolution (like standalone tools) can leave the section unset.
func (f *File) validateMetaRoot() error {
	if f == nil || f.MetaRoot == nil {
		return nil
	}
	peers := f.MetaRoot.Peers
	if len(peers) != 3 {
		return fmt.Errorf("config: meta_root.peers must have exactly 3 entries, got %d", len(peers))
	}
	seen := make(map[uint64]struct{}, 3)
	for _, p := range peers {
		if p.NodeID == 0 {
			return fmt.Errorf("config: meta_root peer node_id must be > 0")
		}
		if _, dup := seen[p.NodeID]; dup {
			return fmt.Errorf("config: duplicate meta_root peer node_id %d", p.NodeID)
		}
		seen[p.NodeID] = struct{}{}
		if strings.TrimSpace(p.Addr) == "" {
			return fmt.Errorf("config: meta_root peer %d addr is required", p.NodeID)
		}
		if strings.TrimSpace(p.TransportAddr) == "" {
			return fmt.Errorf("config: meta_root peer %d transport_addr is required", p.NodeID)
		}
	}
	return nil
}

// MetaRootPeers returns the meta-root peers sorted by node_id. Empty if no
// meta_root section is configured.
func (f *File) MetaRootPeers() []MetaRootPeer {
	if f == nil || f.MetaRoot == nil {
		return nil
	}
	out := make([]MetaRootPeer, len(f.MetaRoot.Peers))
	copy(out, f.MetaRoot.Peers)
	// Sort by NodeID so callers get a stable order.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].NodeID < out[j-1].NodeID; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// ResolveMetaRootPeer returns the configured peer for nodeID, or nil.
func (f *File) ResolveMetaRootPeer(nodeID uint64) *MetaRootPeer {
	if f == nil || f.MetaRoot == nil || nodeID == 0 {
		return nil
	}
	for i := range f.MetaRoot.Peers {
		if f.MetaRoot.Peers[i].NodeID == nodeID {
			return &f.MetaRoot.Peers[i]
		}
	}
	return nil
}

// ResolveMetaRootTransportAddr returns the raft transport address (the
// address sibling meta-root peers dial for raft traffic) for nodeID and
// scope.
func (f *File) ResolveMetaRootTransportAddr(nodeID uint64, scope string) string {
	peer := f.ResolveMetaRootPeer(nodeID)
	if peer == nil {
		return ""
	}
	return resolveScopedValue(peer.TransportAddr, peer.DockerTransportAddr, scope)
}

// ResolveMetaRootServiceAddr returns the gRPC service address (the address
// coordinator and external tooling dial) for nodeID and scope.
func (f *File) ResolveMetaRootServiceAddr(nodeID uint64, scope string) string {
	peer := f.ResolveMetaRootPeer(nodeID)
	if peer == nil {
		return ""
	}
	return resolveScopedValue(peer.Addr, peer.DockerAddr, scope)
}

// ResolveMetaRootWorkDir returns the workdir path for a meta-root peer.
func (f *File) ResolveMetaRootWorkDir(nodeID uint64, scope string) string {
	peer := f.ResolveMetaRootPeer(nodeID)
	if peer == nil {
		return ""
	}
	return resolveScopedValue(peer.WorkDir, peer.DockerWorkDir, scope)
}

// MetaRootTransportPeers returns nodeID → transport address map for the
// given scope. Used to fill the meta-root CLI's repeated --peer flag.
func (f *File) MetaRootTransportPeers(scope string) map[uint64]string {
	if f == nil || f.MetaRoot == nil {
		return nil
	}
	out := make(map[uint64]string, len(f.MetaRoot.Peers))
	for _, p := range f.MetaRoot.Peers {
		addr := resolveScopedValue(p.TransportAddr, p.DockerTransportAddr, scope)
		if addr != "" {
			out[p.NodeID] = addr
		}
	}
	return out
}

// MetaRootServicePeers returns nodeID → gRPC service address map for the
// given scope. Used to fill the coordinator CLI's repeated --root-peer flag.
func (f *File) MetaRootServicePeers(scope string) map[uint64]string {
	if f == nil || f.MetaRoot == nil {
		return nil
	}
	out := make(map[uint64]string, len(f.MetaRoot.Peers))
	for _, p := range f.MetaRoot.Peers {
		addr := resolveScopedValue(p.Addr, p.DockerAddr, scope)
		if addr != "" {
			out[p.NodeID] = addr
		}
	}
	return out
}

// ResolveCoordinatorAddr resolves the coordinator endpoint for the provided scope.
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolveCoordinatorAddr(scope string) string {
	if f == nil || f.Coordinator == nil {
		return ""
	}
	return resolveScopedValue(f.Coordinator.Addr, f.Coordinator.DockerAddr, scope)
}

// ResolveCoordinatorWorkDir resolves the coordinator work directory for the provided scope.
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolveCoordinatorWorkDir(scope string) string {
	if f == nil || f.Coordinator == nil {
		return ""
	}
	return resolveScopedValue(f.Coordinator.WorkDir, f.Coordinator.DockerWorkDir, scope)
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
	store := f.resolveStore(storeID)
	if store == nil {
		return ""
	}
	if normalizedScope(scope) == scopeDocker {
		if v := resolveScopedValue(store.WorkDir, store.DockerWorkDir, scopeDocker); v != "" {
			return v
		}
		if v := resolveScopedValue(f.StoreWorkDirTemplate, f.StoreDockerWorkDirTemplate, scopeDocker); v != "" {
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

// ResolveStoreListenAddr resolves the listen address for the given store and scope.
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolveStoreListenAddr(storeID uint64, scope string) string {
	store := f.resolveStore(storeID)
	if store == nil {
		return ""
	}
	return resolveScopedValue(store.ListenAddr, store.DockerListenAddr, scope)
}

// ResolveStoreAddr resolves the transport/client-facing address for the given store and scope.
//
// Supported scopes are "host" (default) and "docker". Unknown scopes fallback
// to host semantics.
func (f *File) ResolveStoreAddr(storeID uint64, scope string) string {
	store := f.resolveStore(storeID)
	if store == nil {
		return ""
	}
	return resolveScopedValue(store.Addr, store.DockerAddr, scope)
}

func resolveStoreDirTemplate(template string, storeID uint64) string {
	template = strings.TrimSpace(template)
	if template == "" {
		return ""
	}
	return strings.ReplaceAll(template, "{id}", fmt.Sprintf("%d", storeID))
}

func normalizedScope(scope string) string {
	if strings.EqualFold(strings.TrimSpace(scope), scopeDocker) {
		return scopeDocker
	}
	return scopeHost
}

func resolveScopedValue(host, docker, scope string) string {
	if normalizedScope(scope) == scopeDocker {
		if v := strings.TrimSpace(docker); v != "" {
			return v
		}
	}
	return strings.TrimSpace(host)
}
