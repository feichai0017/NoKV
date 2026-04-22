package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "raft_config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadFileAndValidateOK(t *testing.T) {
	json := `{
  "max_retries": 3,
  "stores": [
    {"store_id": 1, "addr": "127.0.0.1:1"},
    {"store_id": 2, "addr": "127.0.0.1:2"}
  ],
  "regions": [
    {"id": 1, "start_key": "", "end_key": "m", "epoch": {"version":1,"conf_version":1}, "peers": [{"store_id":1,"peer_id":101},{"store_id":2,"peer_id":201}], "leader_store_id": 1}
  ]
}`
	path := writeTempConfig(t, json)

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.MaxRetries != 3 {
		t.Fatalf("max retries: want 3 got %d", cfg.MaxRetries)
	}
	if len(cfg.Stores) != 2 || len(cfg.Regions) != 1 {
		t.Fatalf("unexpected counts: stores=%d regions=%d", len(cfg.Stores), len(cfg.Regions))
	}
}

func TestValidateFailsOnDuplicateStore(t *testing.T) {
	json := `{
  "stores": [
    {"store_id": 1, "addr": "a"},
    {"store_id": 1, "addr": "b"}
  ],
  "regions": []
}`
	path := writeTempConfig(t, json)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for duplicate store_id")
	}
}

func TestValidateFailsOnUnknownStoreInRegion(t *testing.T) {
	json := `{
  "stores": [{"store_id": 1, "addr": "a"}],
  "regions": [
    {"id": 1, "start_key": "", "end_key": "", "epoch": {"version":1,"conf_version":1}, "peers": [{"store_id": 2, "peer_id": 201}], "leader_store_id": 2}
  ]
}`
	path := writeTempConfig(t, json)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for unknown store in region")
	}
}

func TestLoadFileMissing(t *testing.T) {
	if _, err := LoadFile(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoadFileInvalidJSON(t *testing.T) {
	path := writeTempConfig(t, "{not-json")
	if _, err := LoadFile(path); err == nil {
		t.Fatalf("expected json error")
	}
}

func TestValidateNilFile(t *testing.T) {
	var cfg *File
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for nil config")
	}
}

func TestValidateStoreIDZero(t *testing.T) {
	cfg := &File{
		Stores: []Store{{StoreID: 0, Addr: "x"}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for store_id zero")
	}
}

func TestValidateRegionIDZero(t *testing.T) {
	cfg := &File{
		Stores: []Store{{StoreID: 1, Addr: "x"}},
		Regions: []Region{{
			ID:            0,
			Peers:         []Peer{{StoreID: 1, PeerID: 2}},
			LeaderStoreID: 1,
		}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for region id zero")
	}
}

func TestValidateLeaderStoreMissing(t *testing.T) {
	cfg := &File{
		Stores: []Store{{StoreID: 1, Addr: "x"}},
		Regions: []Region{{
			ID:            1,
			LeaderStoreID: 2,
		}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for missing leader store")
	}
}

func TestValidatePeerMissingIDs(t *testing.T) {
	cfg := &File{
		Stores: []Store{{StoreID: 1, Addr: "x"}},
		Regions: []Region{{
			ID:    1,
			Peers: []Peer{{StoreID: 0, PeerID: 0}},
		}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for invalid peer ids")
	}
}

func TestValidatePeerUnknownStore(t *testing.T) {
	cfg := &File{
		Stores: []Store{{StoreID: 1, Addr: "x"}},
		Regions: []Region{{
			ID:    1,
			Peers: []Peer{{StoreID: 2, PeerID: 10}},
		}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for unknown peer store")
	}
}

func TestValidateStoreWorkDirTemplateRequiresID(t *testing.T) {
	cfg := &File{
		StoreWorkDirTemplate: "/var/lib/nokv-store",
		Stores:               []Store{{StoreID: 1, Addr: "x"}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected template validation error")
	}

	cfg.StoreWorkDirTemplate = "/var/lib/nokv-store-{id}"
	cfg.StoreDockerWorkDirTemplate = "/var/lib/nokv-docker-store"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected docker template validation error")
	}
}

func TestResolveCoordinatorAddr(t *testing.T) {
	cfg := &File{
		Coordinator: &Coordinator{
			Addr:       "127.0.0.1:2379",
			DockerAddr: "nokv-pd:2379",
		},
	}
	if got := cfg.ResolveCoordinatorAddr("host"); got != "127.0.0.1:2379" {
		t.Fatalf("host coordinator addr mismatch: got %q", got)
	}
	if got := cfg.ResolveCoordinatorAddr("docker"); got != "nokv-pd:2379" {
		t.Fatalf("docker coordinator addr mismatch: got %q", got)
	}
}

func TestResolveCoordinatorAddrFallbackAndNil(t *testing.T) {
	var nilCfg *File
	if got := nilCfg.ResolveCoordinatorAddr("host"); got != "" {
		t.Fatalf("expected empty address for nil cfg, got %q", got)
	}

	cfg := &File{Coordinator: &Coordinator{Addr: "127.0.0.1:2379"}}
	if got := cfg.ResolveCoordinatorAddr("docker"); got != "127.0.0.1:2379" {
		t.Fatalf("expected docker fallback to host addr, got %q", got)
	}
	if got := cfg.ResolveCoordinatorAddr("weird"); got != "127.0.0.1:2379" {
		t.Fatalf("expected unknown scope fallback to host addr, got %q", got)
	}
}

func TestResolveCoordinatorWorkDir(t *testing.T) {
	cfg := &File{
		Coordinator: &Coordinator{
			WorkDir:       "/var/lib/nokv-coordinator",
			DockerWorkDir: "/var/lib/nokv-coordinator-docker",
		},
	}
	if got := cfg.ResolveCoordinatorWorkDir("host"); got != "/var/lib/nokv-coordinator" {
		t.Fatalf("host coordinator work dir mismatch: got %q", got)
	}
	if got := cfg.ResolveCoordinatorWorkDir("docker"); got != "/var/lib/nokv-coordinator-docker" {
		t.Fatalf("docker coordinator work dir mismatch: got %q", got)
	}
}

func TestResolveCoordinatorWorkDirFallbackAndNil(t *testing.T) {
	var nilCfg *File
	if got := nilCfg.ResolveCoordinatorWorkDir("host"); got != "" {
		t.Fatalf("expected empty work dir for nil cfg, got %q", got)
	}

	cfg := &File{Coordinator: &Coordinator{WorkDir: "/var/lib/nokv-coordinator"}}
	if got := cfg.ResolveCoordinatorWorkDir("docker"); got != "/var/lib/nokv-coordinator" {
		t.Fatalf("expected docker fallback to host work dir, got %q", got)
	}
	if got := cfg.ResolveCoordinatorWorkDir("weird"); got != "/var/lib/nokv-coordinator" {
		t.Fatalf("expected unknown scope fallback to host work dir, got %q", got)
	}
}

func TestResolveStoreWorkDir(t *testing.T) {
	cfg := &File{
		StoreWorkDirTemplate:       "./artifacts/cluster/store-{id}",
		StoreDockerWorkDirTemplate: "/var/lib/nokv/store-{id}",
		Stores: []Store{
			{StoreID: 1, Addr: "a"},
			{
				StoreID:       2,
				Addr:          "b",
				WorkDir:       "/data/store2",
				DockerWorkDir: "/docker/store2",
			},
			{
				StoreID: 3,
				Addr:    "c",
				WorkDir: "/data/store3",
			},
		},
	}
	if got := cfg.ResolveStoreWorkDir(1, "host"); got != "./artifacts/cluster/store-1" {
		t.Fatalf("host template mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreWorkDir(1, "docker"); got != "/var/lib/nokv/store-1" {
		t.Fatalf("docker template mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreWorkDir(2, "host"); got != "/data/store2" {
		t.Fatalf("host override mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreWorkDir(2, "docker"); got != "/docker/store2" {
		t.Fatalf("docker override mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreWorkDir(3, "docker"); got != "/data/store3" {
		t.Fatalf("docker fallback to host override mismatch: got %q", got)
	}
}

func TestMetaRootValidationAndHelpers(t *testing.T) {
	cfg := &File{
		MetaRoot: &MetaRoot{
			Peers: []MetaRootPeer{
				{
					NodeID:              2,
					Addr:                "127.0.0.1:2381",
					DockerAddr:          "nokv-meta-root-2:2380",
					TransportAddr:       "127.0.0.1:2481",
					DockerTransportAddr: "nokv-meta-root-2:2480",
					WorkDir:             "/tmp/root2",
					DockerWorkDir:       "/data/root2",
				},
				{
					NodeID:              1,
					Addr:                "127.0.0.1:2380",
					DockerAddr:          "nokv-meta-root-1:2380",
					TransportAddr:       "127.0.0.1:2480",
					DockerTransportAddr: "nokv-meta-root-1:2480",
					WorkDir:             "/tmp/root1",
					DockerWorkDir:       "/data/root1",
				},
				{
					NodeID:              3,
					Addr:                "127.0.0.1:2382",
					DockerAddr:          "nokv-meta-root-3:2380",
					TransportAddr:       "127.0.0.1:2482",
					DockerTransportAddr: "nokv-meta-root-3:2480",
					WorkDir:             "/tmp/root3",
					DockerWorkDir:       "/data/root3",
				},
			},
		},
		Stores: []Store{{StoreID: 1, Addr: "x"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate meta root: %v", err)
	}

	peers := cfg.MetaRootPeers()
	if len(peers) != 3 || peers[0].NodeID != 1 || peers[2].NodeID != 3 {
		t.Fatalf("meta-root peers should be sorted by node id: %+v", peers)
	}

	peer := cfg.ResolveMetaRootPeer(2)
	if peer == nil || peer.NodeID != 2 {
		t.Fatalf("expected peer 2, got %+v", peer)
	}
	if got := cfg.ResolveMetaRootPeer(99); got != nil {
		t.Fatalf("expected missing peer, got %+v", got)
	}
	if got := cfg.ResolveMetaRootTransportAddr(2, "docker"); got != "nokv-meta-root-2:2480" {
		t.Fatalf("docker transport addr mismatch: %q", got)
	}
	if got := cfg.ResolveMetaRootTransportAddr(2, "host"); got != "127.0.0.1:2481" {
		t.Fatalf("host transport addr mismatch: %q", got)
	}
	if got := cfg.ResolveMetaRootServiceAddr(1, "docker"); got != "nokv-meta-root-1:2380" {
		t.Fatalf("docker service addr mismatch: %q", got)
	}
	if got := cfg.ResolveMetaRootWorkDir(3, "docker"); got != "/data/root3" {
		t.Fatalf("docker work dir mismatch: %q", got)
	}

	transportPeers := cfg.MetaRootTransportPeers("docker")
	if len(transportPeers) != 3 || transportPeers[1] != "nokv-meta-root-1:2480" {
		t.Fatalf("unexpected transport peers: %+v", transportPeers)
	}
	servicePeers := cfg.MetaRootServicePeers("host")
	if len(servicePeers) != 3 || servicePeers[3] != "127.0.0.1:2382" {
		t.Fatalf("unexpected service peers: %+v", servicePeers)
	}
}

func TestValidateMetaRootFailures(t *testing.T) {
	cases := []File{
		{MetaRoot: &MetaRoot{Peers: []MetaRootPeer{{NodeID: 1, Addr: "a", TransportAddr: "ta"}}}},
		{MetaRoot: &MetaRoot{Peers: []MetaRootPeer{{NodeID: 1, Addr: "a", TransportAddr: "ta"}, {NodeID: 1, Addr: "b", TransportAddr: "tb"}, {NodeID: 3, Addr: "c", TransportAddr: "tc"}}}},
		{MetaRoot: &MetaRoot{Peers: []MetaRootPeer{{NodeID: 0, Addr: "a", TransportAddr: "ta"}, {NodeID: 2, Addr: "b", TransportAddr: "tb"}, {NodeID: 3, Addr: "c", TransportAddr: "tc"}}}},
		{MetaRoot: &MetaRoot{Peers: []MetaRootPeer{{NodeID: 1, TransportAddr: "ta"}, {NodeID: 2, Addr: "b", TransportAddr: "tb"}, {NodeID: 3, Addr: "c", TransportAddr: "tc"}}}},
		{MetaRoot: &MetaRoot{Peers: []MetaRootPeer{{NodeID: 1, Addr: "a"}, {NodeID: 2, Addr: "b", TransportAddr: "tb"}, {NodeID: 3, Addr: "c", TransportAddr: "tc"}}}},
	}
	for _, cfg := range cases {
		cfg.Stores = []Store{{StoreID: 1, Addr: "x"}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected meta-root validation failure for %+v", cfg.MetaRoot)
		}
	}
}

func TestResolveStoreWorkDirFallbackAndNil(t *testing.T) {
	var nilCfg *File
	if got := nilCfg.ResolveStoreWorkDir(1, "host"); got != "" {
		t.Fatalf("expected empty workdir for nil cfg, got %q", got)
	}

	cfg := &File{
		Stores: []Store{{StoreID: 1, Addr: "a"}},
	}
	if got := cfg.ResolveStoreWorkDir(1, "host"); got != "" {
		t.Fatalf("expected empty workdir without template/override, got %q", got)
	}
	if got := cfg.ResolveStoreWorkDir(2, "host"); got != "" {
		t.Fatalf("expected empty workdir for unknown store, got %q", got)
	}
}

func TestResolveStoreListenAddr(t *testing.T) {
	cfg := &File{
		Stores: []Store{
			{
				StoreID:          1,
				ListenAddr:       "127.0.0.1:20170",
				DockerListenAddr: "nokv-store-1:20170",
			},
			{
				StoreID:    2,
				ListenAddr: "127.0.0.1:20171",
			},
		},
	}
	if got := cfg.ResolveStoreListenAddr(1, "host"); got != "127.0.0.1:20170" {
		t.Fatalf("host listen addr mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreListenAddr(1, "docker"); got != "nokv-store-1:20170" {
		t.Fatalf("docker listen addr mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreListenAddr(2, "docker"); got != "127.0.0.1:20171" {
		t.Fatalf("docker fallback listen addr mismatch: got %q", got)
	}
}

func TestResolveStoreAddr(t *testing.T) {
	cfg := &File{
		Stores: []Store{
			{
				StoreID:    1,
				Addr:       "127.0.0.1:20170",
				DockerAddr: "nokv-store-1:20170",
			},
			{
				StoreID: 2,
				Addr:    "127.0.0.1:20171",
			},
		},
	}
	if got := cfg.ResolveStoreAddr(1, "host"); got != "127.0.0.1:20170" {
		t.Fatalf("host store addr mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreAddr(1, "docker"); got != "nokv-store-1:20170" {
		t.Fatalf("docker store addr mismatch: got %q", got)
	}
	if got := cfg.ResolveStoreAddr(2, "docker"); got != "127.0.0.1:20171" {
		t.Fatalf("docker fallback store addr mismatch: got %q", got)
	}
}
