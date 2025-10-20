package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/feichai0017/NoKV/manifest"
)

type peerList []string

func (p *peerList) String() string {
	return strings.Join(*p, ",")
}

func (p *peerList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("peer value cannot be empty")
	}
	*p = append(*p, value)
	return nil
}

func main() {
	var (
		workdir   = flag.String("workdir", "", "manifest directory to update")
		regionID  = flag.Uint64("region-id", 0, "region identifier")
		startKey  = flag.String("start-key", "", "region start key (use hex:<bytes> for binary)")
		endKey    = flag.String("end-key", "", "region end key (exclusive, use hex:<bytes> for binary)")
		version   = flag.Uint64("epoch-version", 1, "region version epoch")
		confVer   = flag.Uint64("epoch-conf-version", 0, "region configuration version (defaults to number of peers)")
		stateStr  = flag.String("state", "running", "region state (running|tombstone)")
		peerFlags peerList
	)

	flag.Var(&peerFlags, "peer", "peer mapping in storeID:peerID form (repeatable)")
	flag.Parse()

	if err := run(*workdir, *regionID, *startKey, *endKey, *version, *confVer, *stateStr, peerFlags); err != nil {
		log.Fatalf("manifestctl: %v", err)
	}
}

func run(workdir string, regionID uint64, startKey, endKey string, version, confVer uint64, stateStr string, peerFlags []string) error {
	if workdir == "" {
		return fmt.Errorf("--workdir is required")
	}
	if regionID == 0 {
		return fmt.Errorf("--region-id must be > 0")
	}
	if len(peerFlags) == 0 {
		return fmt.Errorf("at least one --peer is required")
	}

	meta := manifest.RegionMeta{
		ID:    regionID,
		State: parseRegionState(stateStr),
		Epoch: manifest.RegionEpoch{
			Version:     version,
			ConfVersion: confVer,
		},
		StartKey: decodeKey(startKey),
		EndKey:   decodeKey(endKey),
	}

	for _, entry := range peerFlags {
		storeID, peerID, err := parsePeer(entry)
		if err != nil {
			return fmt.Errorf("parsing --peer %q: %w", entry, err)
		}
		meta.Peers = append(meta.Peers, manifest.PeerMeta{
			StoreID: storeID,
			PeerID:  peerID,
		})
	}

	if meta.Epoch.ConfVersion == 0 {
		meta.Epoch.ConfVersion = uint64(len(meta.Peers))
	}

	mgr, err := manifest.Open(workdir)
	if err != nil {
		return fmt.Errorf("open manifest at %s: %w", workdir, err)
	}
	defer func() { _ = mgr.Close() }()

	if err := mgr.LogRegionUpdate(meta); err != nil {
		return fmt.Errorf("log region: %w", err)
	}
	fmt.Fprintf(os.Stdout, "logged region %d to %s\n", meta.ID, workdir)
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
	_, err := fmt.Sscanf(value, "%d", &out)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", value, err)
	}
	return out, nil
}

func parseRegionState(state string) manifest.RegionState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", "running":
		return manifest.RegionStateRunning
	case "tombstone":
		return manifest.RegionStateTombstone
	default:
		log.Printf("unknown state %q, defaulting to running", state)
		return manifest.RegionStateRunning
	}
}

func decodeKey(value string) []byte {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if strings.HasPrefix(value, "hex:") {
		data, err := hex.DecodeString(value[4:])
		if err != nil {
			log.Fatalf("decode hex key %q: %v", value, err)
		}
		return data
	}
	return []byte(value)
}
