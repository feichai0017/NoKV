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

func TestResolvePDAddr(t *testing.T) {
	cfg := &File{
		PD: &PD{
			Addr:       "127.0.0.1:2379",
			DockerAddr: "nokv-pd:2379",
		},
	}
	if got := cfg.ResolvePDAddr("host"); got != "127.0.0.1:2379" {
		t.Fatalf("host pd addr mismatch: got %q", got)
	}
	if got := cfg.ResolvePDAddr("docker"); got != "nokv-pd:2379" {
		t.Fatalf("docker pd addr mismatch: got %q", got)
	}
}

func TestResolvePDAddrFallbackAndNil(t *testing.T) {
	var nilCfg *File
	if got := nilCfg.ResolvePDAddr("host"); got != "" {
		t.Fatalf("expected empty address for nil cfg, got %q", got)
	}

	cfg := &File{PD: &PD{Addr: "127.0.0.1:2379"}}
	if got := cfg.ResolvePDAddr("docker"); got != "127.0.0.1:2379" {
		t.Fatalf("expected docker fallback to host addr, got %q", got)
	}
}

func TestResolvePDWorkDir(t *testing.T) {
	cfg := &File{
		PD: &PD{
			WorkDir:       "/var/lib/nokv-pd",
			DockerWorkDir: "/var/lib/nokv-pd-docker",
		},
	}
	if got := cfg.ResolvePDWorkDir("host"); got != "/var/lib/nokv-pd" {
		t.Fatalf("host pd work dir mismatch: got %q", got)
	}
	if got := cfg.ResolvePDWorkDir("docker"); got != "/var/lib/nokv-pd-docker" {
		t.Fatalf("docker pd work dir mismatch: got %q", got)
	}
}

func TestResolvePDWorkDirFallbackAndNil(t *testing.T) {
	var nilCfg *File
	if got := nilCfg.ResolvePDWorkDir("host"); got != "" {
		t.Fatalf("expected empty work dir for nil cfg, got %q", got)
	}

	cfg := &File{PD: &PD{WorkDir: "/var/lib/nokv-pd"}}
	if got := cfg.ResolvePDWorkDir("docker"); got != "/var/lib/nokv-pd" {
		t.Fatalf("expected docker fallback to host work dir, got %q", got)
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
