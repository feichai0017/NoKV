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
