package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"os"
	"path/filepath"
	"strings"

	"github.com/feichai0017/NoKV/config"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

var exit = os.Exit
var getwd = os.Getwd

func main() {
	if len(os.Args) < 2 {
		printUsage()
		exit(1)
	}

	subcmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch subcmd {
	case "stores":
		err = runStores(args)
	case "regions":
		err = runRegions(args)
	case "coordinator":
		err = runCoordinator(args)
	case "catalog":
		err = runCatalog(args)
	default:
		printUsage()
		exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "nokv-config %s: %v\n", subcmd, err)
		exit(1)
	}
}

func defaultConfigPath() string {
	if cwd, err := getwd(); err == nil {
		return filepath.Join(cwd, "raft_config.example.json")
	}
	return "raft_config.example.json"
}

func runStores(args []string) error {
	fs := flag.NewFlagSet("stores", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(), "path to raft configuration file")
	format := fs.String("format", "simple", "output format: simple|json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}

	switch strings.ToLower(*format) {
	case "json":
		return json.NewEncoder(os.Stdout).Encode(cfg.Stores)
	case "simple":
		for _, st := range cfg.Stores {
			fmt.Printf("%d %s %s %s %s %s %s\n",
				st.StoreID,
				firstNonEmpty(st.ListenAddr, st.Addr),
				st.Addr,
				firstNonEmpty(st.DockerListenAddr, st.ListenAddr, st.Addr),
				firstNonEmpty(st.DockerAddr, st.Addr),
				firstNonEmpty(cfg.ResolveStoreWorkDir(st.StoreID, "host")),
				firstNonEmpty(cfg.ResolveStoreWorkDir(st.StoreID, "docker")),
			)
		}
		return nil
	default:
		return fmt.Errorf("unknown format %q", *format)
	}
}

func runRegions(args []string) error {
	fs := flag.NewFlagSet("regions", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(), "path to raft configuration file")
	format := fs.String("format", "simple", "output format: simple|json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}

	switch strings.ToLower(*format) {
	case "json":
		return json.NewEncoder(os.Stdout).Encode(cfg.Regions)
	case "simple":
		for _, region := range cfg.Regions {
			encodedPeers := make([]string, 0, len(region.Peers))
			for _, peer := range region.Peers {
				encodedPeers = append(encodedPeers, fmt.Sprintf("%d:%d", peer.StoreID, peer.PeerID))
			}
			fmt.Printf("%d %s %s %d %d %s %d\n",
				region.ID,
				encodeKey(region.StartKey),
				encodeKey(region.EndKey),
				region.Epoch.Version,
				region.Epoch.ConfVersion,
				strings.Join(encodedPeers, ","),
				region.LeaderStoreID,
			)
		}
		return nil
	default:
		return fmt.Errorf("unknown format %q", *format)
	}
}

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(), "path to raft configuration file")
	format := fs.String("format", "simple", "output format: simple|json")
	scope := fs.String("scope", "host", "address scope: host|docker")
	field := fs.String("field", "addr", "simple output field: addr|workdir")
	if err := fs.Parse(args); err != nil {
		return err
	}

	scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
	if scopeNorm != "host" && scopeNorm != "docker" {
		return fmt.Errorf("unknown scope %q (expected host|docker)", *scope)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if cfg.Coordinator == nil {
		return fmt.Errorf("coordinator block missing from configuration")
	}

	switch strings.ToLower(*format) {
	case "json":
		return json.NewEncoder(os.Stdout).Encode(cfg.Coordinator)
	case "simple":
		switch strings.ToLower(strings.TrimSpace(*field)) {
		case "addr":
			addr := cfg.ResolveCoordinatorAddr(scopeNorm)
			if addr == "" {
				return fmt.Errorf("coordinator address missing for scope %q", scopeNorm)
			}
			fmt.Println(addr)
		case "workdir":
			workdir := cfg.ResolveCoordinatorWorkDir(scopeNorm)
			if workdir == "" {
				return fmt.Errorf("coordinator workdir missing for scope %q", scopeNorm)
			}
			fmt.Println(workdir)
		default:
			return fmt.Errorf("unknown field %q (expected addr|workdir)", *field)
		}
		return nil
	default:
		return fmt.Errorf("unknown format %q", *format)
	}
}

func loadConfig(path string) (*config.File, error) {
	cfg, err := config.LoadFile(path)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return "-"
}

func encodeKey(value string) string {
	if value == "" {
		return "-"
	}
	if isASCIIPrintable(value) {
		return value
	}
	return "hex:" + hex.EncodeToString([]byte(value))
}

func isASCIIPrintable(value string) bool {
	for _, r := range value {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

func printUsage() {
	fmt.Println(`Usage: nokv-config <command> [flags]

Commands:
  stores   Print store endpoints from the raft configuration
  regions  Print region metadata from the raft configuration
  coordinator Print coordinator endpoint from the raft configuration
  catalog  Write region metadata and optional store membership seed into the raftstore local peer catalog

Flags:
  --config <path>   Path to raft_config JSON (defaults to ./raft_config.example.json)
  --format <fmt>    Output format (simple|json) depending on the command
  --field <name>    For "coordinator --format simple": addr|workdir`)
}

func runCatalog(args []string) error {
	fs := flag.NewFlagSet("catalog", flag.ExitOnError)
	workdir := fs.String("workdir", "", "work directory containing the local peer catalog")
	regionID := fs.Uint64("region-id", 0, "region identifier")
	startKey := fs.String("start-key", "", "region start key (plain or hex:<bytes>)")
	endKey := fs.String("end-key", "", "region end key (primacy, plain or hex:<bytes>)")
	version := fs.Uint64("epoch-version", 1, "region version epoch")
	confVer := fs.Uint64("epoch-conf-version", 0, "region configuration version (defaults to number of peers)")
	stateStr := fs.String("state", "running", "region state (running|tombstone)")
	bootstrapStoreID := fs.Uint64("bootstrap-store-id", 0, "optional store ID to seed as a pending rooted StoreJoined event")
	var peerFlags multiValue
	fs.Var(&peerFlags, "peer", "peer mapping: storeID:peerID (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *workdir == "" {
		return fmt.Errorf("--workdir is required")
	}
	if *regionID == 0 {
		return fmt.Errorf("--region-id must be > 0")
	}
	if len(peerFlags.values) == 0 {
		return fmt.Errorf("at least one --peer is required")
	}

	meta := localmeta.RegionMeta{
		ID:    *regionID,
		State: parseRegionState(*stateStr),
		Epoch: metaregion.Epoch{
			Version:     *version,
			ConfVersion: *confVer,
		},
		StartKey: decodeKey(*startKey),
		EndKey:   decodeKey(*endKey),
	}

	for _, entry := range peerFlags.values {
		storeID, peerID, err := parsePeer(entry)
		if err != nil {
			return fmt.Errorf("parsing --peer %q: %w", entry, err)
		}
		meta.Peers = append(meta.Peers, metaregion.Peer{
			StoreID: storeID,
			PeerID:  peerID,
		})
	}

	if meta.Epoch.ConfVersion == 0 {
		meta.Epoch.ConfVersion = uint64(len(meta.Peers))
	}

	metaStore, err := localmeta.OpenLocalStore(*workdir, nil)
	if err != nil {
		return fmt.Errorf("open local peer catalog at %s: %w", *workdir, err)
	}
	defer func() { _ = metaStore.Close() }()

	if err := metaStore.SaveRegion(meta); err != nil {
		return fmt.Errorf("persist region: %w", err)
	}
	if *bootstrapStoreID != 0 {
		if err := metaStore.SavePendingRootEvent(localmeta.PendingRootEvent{
			Sequence: 1,
			Event:    rootevent.StoreJoined(*bootstrapStoreID),
		}); err != nil {
			return fmt.Errorf("persist store membership root event: %w", err)
		}
	}
	if _, err := fmt.Fprintf(os.Stdout, "stored region %d in local peer catalog at %s\n", meta.ID, *workdir); err != nil {
		return err
	}
	return nil
}

type multiValue struct {
	values []string
}

func (m *multiValue) String() string {
	return strings.Join(m.values, ",")
}

func (m *multiValue) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("value cannot be empty")
	}
	m.values = append(m.values, value)
	return nil
}

func parsePeer(value string) (uint64, uint64, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected storeID:peerID")
	}
	storeID, err := parseUint(parts[0])
	if err != nil {
		return 0, 0, err
	}
	peerID, err := parseUint(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return storeID, peerID, nil
}

func parseUint(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty value")
	}
	var out uint64
	if _, err := fmt.Sscanf(value, "%d", &out); err != nil {
		return 0, fmt.Errorf("parse %q: %w", value, err)
	}
	return out, nil
}

func parseRegionState(state string) metaregion.ReplicaState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", "running":
		return metaregion.ReplicaStateRunning
	case "tombstone":
		return metaregion.ReplicaStateTombstone
	default:
		return metaregion.ReplicaStateRunning
	}
}

func decodeKey(value string) []byte {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return nil
	}
	if strings.HasPrefix(value, "hex:") {
		data, err := hex.DecodeString(value[4:])
		if err != nil {
			panic(fmt.Sprintf("decode hex key %q: %v", value, err))
		}
		return data
	}
	return []byte(value)
}
