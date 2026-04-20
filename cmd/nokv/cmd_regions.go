package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

func runRegionsCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("regions", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}

	metaStore, err := localmeta.OpenLocalStore(*workDir, nil)
	if err != nil {
		return err
	}
	defer func() { _ = metaStore.Close() }()

	snapshot := metaStore.Snapshot()
	regions := make([]localmeta.RegionMeta, 0, len(snapshot))
	for _, meta := range snapshot {
		regions = append(regions, meta)
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i].ID < regions[j].ID })

	if *asJSON {
		out := map[string]any{
			"regions": regions,
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	if len(regions) == 0 {
		_, _ = fmt.Fprintln(w, "Regions: (none)")
		return nil
	}

	_, _ = fmt.Fprintln(w, "Regions:")
	for _, meta := range regions {
		_, _ = fmt.Fprintf(w, "  - id=%d state=%s epoch={ver:%d conf:%d} range=[%q,%q) peers=%s\n",
			meta.ID, formatRegionState(meta.State), meta.Epoch.Version, meta.Epoch.ConfVersion,
			meta.StartKey, meta.EndKey, formatPeers(meta.Peers))
	}
	return nil
}

func formatRegionState(state metaregion.ReplicaState) string {
	switch state {
	case metaregion.ReplicaStateNew:
		return "new"
	case metaregion.ReplicaStateRunning:
		return "running"
	case metaregion.ReplicaStateRemoving:
		return "removing"
	case metaregion.ReplicaStateTombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown(%d)", state)
	}
}

func formatPeers(peers []metaregion.Peer) string {
	if len(peers) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(peers))
	for _, p := range peers {
		parts = append(parts, fmt.Sprintf("{store:%d peer:%d}", p.StoreID, p.PeerID))
	}
	return fmt.Sprintf("[%s]", strings.Join(parts, " "))
}
