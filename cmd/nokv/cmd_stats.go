package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
)

func runStatsCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory (offline snapshot)")
	expvarURL := fs.String("expvar", "", "HTTP endpoint exposing /debug/vars (overrides workdir)")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	noMetrics := fs.Bool("no-region-metrics", false, "do not attach region metrics recorder (requires --workdir)")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	var snap NoKV.StatsSnapshot
	var err error
	switch {
	case *expvarURL != "":
		snap, err = fetchExpvarSnapshot(*expvarURL)
	case *workDir != "":
		snap, err = localStatsSnapshot(*workDir, !*noMetrics)
	default:
		return fmt.Errorf("either --workdir or --expvar must be specified")
	}
	if err != nil {
		return err
	}
	return renderStats(w, snap, *asJSON)
}

func renderStats(w io.Writer, snap NoKV.StatsSnapshot, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(snap)
	}

	_, _ = fmt.Fprintf(w, "Entries               %d\n", snap.Entries)
	_, _ = fmt.Fprintf(w, "Flush.Pending          %d\n", snap.Flush.Pending)
	_, _ = fmt.Fprintf(w, "Compaction.Backlog     %d\n", snap.Compaction.Backlog)
	_, _ = fmt.Fprintf(w, "Compaction.MaxScore    %.2f\n", snap.Compaction.MaxScore)
	_, _ = fmt.Fprintf(w, "Flush.Wait.LastMs      %.2f\n", snap.Flush.LastWaitMs)
	_, _ = fmt.Fprintf(w, "Flush.Wait.MaxMs       %.2f\n", snap.Flush.MaxWaitMs)
	_, _ = fmt.Fprintf(w, "Flush.Build.LastMs     %.2f\n", snap.Flush.LastBuildMs)
	_, _ = fmt.Fprintf(w, "Flush.Build.MaxMs      %.2f\n", snap.Flush.MaxBuildMs)
	_, _ = fmt.Fprintf(w, "Flush.Release.LastMs   %.2f\n", snap.Flush.LastReleaseMs)
	_, _ = fmt.Fprintf(w, "Flush.Release.MaxMs    %.2f\n", snap.Flush.MaxReleaseMs)
	_, _ = fmt.Fprintf(w, "Compaction.LastMs      %.2f\n", snap.Compaction.LastDurationMs)
	_, _ = fmt.Fprintf(w, "Compaction.MaxMs       %.2f\n", snap.Compaction.MaxDurationMs)
	_, _ = fmt.Fprintf(w, "Compaction.Runs        %d\n", snap.Compaction.Runs)
	_, _ = fmt.Fprintf(w, "ValueLog.Segments      %d\n", snap.ValueLog.Segments)
	_, _ = fmt.Fprintf(w, "ValueLog.PendingDelete %d\n", snap.ValueLog.PendingDeletes)
	_, _ = fmt.Fprintf(w, "ValueLog.DiscardQueue  %d\n", snap.ValueLog.DiscardQueue)
	if snap.ValueLog.GC.GCRuns > 0 || snap.ValueLog.GC.GCScheduled > 0 {
		_, _ = fmt.Fprintf(w, "ValueLog.GC            runs=%d scheduled=%d active=%d removed=%d skipped=%d throttled=%d rejected=%d parallel=%d\n",
			snap.ValueLog.GC.GCRuns,
			snap.ValueLog.GC.GCScheduled,
			snap.ValueLog.GC.GCActive,
			snap.ValueLog.GC.SegmentsRemoved,
			snap.ValueLog.GC.GCSkipped,
			snap.ValueLog.GC.GCThrottled,
			snap.ValueLog.GC.GCRejected,
			snap.ValueLog.GC.GCParallelism,
		)
	}
	if len(snap.ValueLog.Heads) > 0 {
		buckets := make([]uint32, 0, len(snap.ValueLog.Heads))
		for bucket := range snap.ValueLog.Heads {
			buckets = append(buckets, bucket)
		}
		slices.Sort(buckets)
		for _, bucket := range buckets {
			head := snap.ValueLog.Heads[bucket]
			if head.IsZero() {
				continue
			}
			_, _ = fmt.Fprintf(w, "ValueLog.Head[%d]       fid=%d offset=%d len=%d\n",
				bucket, head.Fid, head.Offset, head.Len)
		}
	}
	_, _ = fmt.Fprintf(w, "Write.HotKeyThrottled  %d\n", snap.Write.HotKeyLimited)
	if snap.Hot.WriteRing != nil {
		hs := snap.Hot.WriteRing
		_, _ = fmt.Fprintf(w, "Thermos.Buckets        %d\n", hs.Buckets)
		_, _ = fmt.Fprintf(w, "Thermos.Nodes          %d (load=%.2f)\n", hs.Nodes, hs.LoadFactor)
		_, _ = fmt.Fprintf(w, "Thermos.Touches        %d (clamps=%d inserts=%d removes=%d)\n",
			hs.Touches, hs.Clamps, hs.Inserts, hs.Removes)
		if hs.WindowSlots > 0 && hs.WindowSlotDuration > 0 {
			_, _ = fmt.Fprintf(w, "Thermos.Window         slots=%d dur=%s\n",
				hs.WindowSlots, hs.WindowSlotDuration.String())
		}
		if hs.DecayInterval > 0 && hs.DecayShift > 0 {
			_, _ = fmt.Fprintf(w, "Thermos.Decay          every=%s shift=%d\n",
				hs.DecayInterval.String(), hs.DecayShift)
		}
	}
	_, _ = fmt.Fprintf(w, "Compaction.ValueWeight %.2f", snap.Compaction.ValueWeight)
	if snap.Compaction.ValueWeightSuggested > snap.Compaction.ValueWeight {
		_, _ = fmt.Fprintf(w, " (suggested %.2f)", snap.Compaction.ValueWeightSuggested)
	}
	_, _ = fmt.Fprintln(w)
	if snap.LSM.ValueDensityMax > 0 {
		_, _ = fmt.Fprintf(w, "LSM.ValueDensityMax    %.2f\n", snap.LSM.ValueDensityMax)
	}
	if snap.LSM.ValueDensityAlert {
		_, _ = fmt.Fprintln(w, "LSM.ValueDensityAlert  true")
	}
	_, _ = fmt.Fprintf(w, "WAL.ActiveSegment      %d (segments=%d removed=%d)\n", snap.WAL.ActiveSegment, snap.WAL.SegmentCount, snap.WAL.SegmentsRemoved)
	_, _ = fmt.Fprintf(w, "WAL.ActiveSize         %d bytes\n", snap.WAL.ActiveSize)
	if snap.WAL.RecordCounts.Total() > 0 {
		r := snap.WAL.RecordCounts
		_, _ = fmt.Fprintf(w, "WAL.Records            entries=%d raft_entries=%d raft_states=%d raft_snapshots=%d other=%d\n",
			r.Entries, r.RaftEntries, r.RaftStates, r.RaftSnapshots, r.Other)
	}
	_, _ = fmt.Fprintf(w, "WAL.RaftSegments       %d (removable=%d)\n", snap.WAL.SegmentsWithRaftRecords, snap.WAL.RemovableRaftSegments)
	if snap.WAL.TypedRecordRatio > 0 || snap.WAL.TypedRecordWarning {
		_, _ = fmt.Fprintf(w, "WAL.TypedRatio         %.2f\n", snap.WAL.TypedRecordRatio)
	}
	if snap.WAL.TypedRecordWarning && snap.WAL.TypedRecordReason != "" {
		_, _ = fmt.Fprintf(w, "WAL.Warning            %s\n", snap.WAL.TypedRecordReason)
	}
	if snap.WAL.AutoGCRuns > 0 || snap.WAL.AutoGCRemoved > 0 || snap.WAL.AutoGCLastUnix > 0 {
		last := "never"
		if snap.WAL.AutoGCLastUnix > 0 {
			last = time.Unix(snap.WAL.AutoGCLastUnix, 0).Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(w, "WAL.AutoGC             runs=%d removed=%d last=%s\n", snap.WAL.AutoGCRuns, snap.WAL.AutoGCRemoved, last)
	}
	if snap.Raft.GroupCount > 0 {
		_, _ = fmt.Fprintf(w, "Raft.Groups            %d lagging=%d maxLagSegments=%d\n",
			snap.Raft.GroupCount, snap.Raft.LaggingGroups, snap.Raft.MaxLagSegments)
		_, _ = fmt.Fprintf(w, "Raft.SegmentRange      min=%d max=%d\n", snap.Raft.MinLogSegment, snap.Raft.MaxLogSegment)
		if snap.Raft.LagWarnThreshold > 0 {
			_, _ = fmt.Fprintf(w, "Raft.LagThreshold      %d segments\n", snap.Raft.LagWarnThreshold)
		}
		if snap.Raft.LagWarning {
			_, _ = fmt.Fprintf(w, "Raft.Warning           lagging=%d maxLag=%d (threshold=%d)\n",
				snap.Raft.LaggingGroups, snap.Raft.MaxLagSegments, snap.Raft.LagWarnThreshold)
		}
	}
	_, _ = fmt.Fprintf(w, "Regions.Total          %d (new=%d running=%d removing=%d tombstone=%d other=%d)\n",
		snap.Region.Total, snap.Region.New, snap.Region.Running, snap.Region.Removing, snap.Region.Tombstone, snap.Region.Other)
	if snap.LSM.ValueBytesTotal > 0 {
		_, _ = fmt.Fprintf(w, "LSM.ValueBytesTotal   %d\n", snap.LSM.ValueBytesTotal)
	}
	if len(snap.LSM.Levels) > 0 {
		_, _ = fmt.Fprintln(w, "LSM.Levels:")
		for _, lvl := range snap.LSM.Levels {
			_, _ = fmt.Fprintf(w, "  - L%d tables=%d size=%dB value=%dB stale=%dB",
				lvl.Level, lvl.TableCount, lvl.SizeBytes, lvl.ValueBytes, lvl.StaleBytes)
			if lvl.IngestTables > 0 {
				_, _ = fmt.Fprintf(w, " ingestTables=%d ingestSize=%dB ingestValue=%dB",
					lvl.IngestTables, lvl.IngestSizeBytes, lvl.IngestValueBytes)
			}
			_, _ = fmt.Fprintln(w)
		}
	}
	if len(snap.Hot.WriteKeys) > 0 {
		_, _ = fmt.Fprintln(w, "WriteHotKeys:")
		for _, hk := range snap.Hot.WriteKeys {
			_, _ = fmt.Fprintf(w, "  - key=%q count=%d\n", hk.Key, hk.Count)
		}
	}
	if snap.Transport.SendAttempts > 0 || snap.Transport.DialsTotal > 0 {
		_, _ = fmt.Fprintf(w, "Transport.GRPC         sends=%d success=%d fail=%d retries=%d blocked=%d watchdog=%v\n",
			snap.Transport.SendAttempts,
			snap.Transport.SendSuccesses,
			snap.Transport.SendFailures,
			snap.Transport.Retries,
			snap.Transport.BlockedPeers,
			snap.Transport.WatchdogActive,
		)
	}
	if snap.Redis.CommandsTotal > 0 || snap.Redis.ConnectionsAccepted > 0 {
		_, _ = fmt.Fprintf(w, "Redis.Gateway          commands=%d errors=%d active_conn=%d accepted_conn=%d\n",
			snap.Redis.CommandsTotal,
			snap.Redis.ErrorsTotal,
			snap.Redis.ConnectionsActive,
			snap.Redis.ConnectionsAccepted,
		)
	}
	return nil
}

func localStatsSnapshot(workDir string, attachMetrics bool) (NoKV.StatsSnapshot, error) {
	if workDir == "" {
		return NoKV.StatsSnapshot{}, fmt.Errorf("workdir is required")
	}
	metaStore, err := localmeta.OpenLocalStore(workDir, nil)
	if err != nil {
		return NoKV.StatsSnapshot{}, err
	}
	defer func() { _ = metaStore.Close() }()
	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = workDir
	opts.RaftPointerSnapshot = metaStore.RaftPointerSnapshot
	opts.AllowedModes = []raftmode.Mode{
		raftmode.ModeStandalone,
		raftmode.ModePreparing,
		raftmode.ModeSeeded,
		raftmode.ModeCluster,
	}
	// Auto-detect whether the workdir was written with the vlog
	// Authoritative consumer enabled. Stats reporting must reflect
	// existing vlog state even though EnableValueLog defaults to false
	// after the slab-substrate redesign.
	if _, err := os.Stat(filepath.Join(workDir, "vlog")); err == nil {
		opts.EnableValueLog = true
	}
	db, err := NoKV.Open(opts)
	if err != nil {
		return NoKV.StatsSnapshot{}, fmt.Errorf("open db for offline stats: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()
	if attachMetrics {
		if metrics := firstRegionMetrics(); metrics != nil {
			db.SetRegionMetrics(metrics)
		}
	}
	return db.Info().Snapshot(), nil
}

func fetchExpvarSnapshot(url string) (NoKV.StatsSnapshot, error) {
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	if !strings.Contains(url, "/debug/vars") {
		if strings.HasSuffix(url, "/") {
			url += "debug/vars"
		} else {
			url += "/debug/vars"
		}
	}
	resp, err := http.Get(url) // #nosec G107 - CLI utility, user-provided URL.
	if err != nil {
		return NoKV.StatsSnapshot{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return NoKV.StatsSnapshot{}, fmt.Errorf("expvar request failed: %s", resp.Status)
	}
	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return NoKV.StatsSnapshot{}, err
	}
	return parseExpvarSnapshot(data), nil
}

func parseExpvarSnapshot(data map[string]any) NoKV.StatsSnapshot {
	var snap NoKV.StatsSnapshot
	if raw, ok := data["NoKV.Stats"]; ok {
		if blob, err := json.Marshal(raw); err == nil {
			if err := json.Unmarshal(blob, &snap); err == nil {
				return snap
			}
		}
	}
	// Allow callers to pass the stats payload directly.
	if blob, err := json.Marshal(data); err == nil {
		_ = json.Unmarshal(blob, &snap)
	}
	return snap
}

func firstRegionMetrics() *metrics.RegionMetrics {
	for _, st := range runtimeStoreSnapshot() {
		if st == nil {
			continue
		}
		if metrics := st.RegionMetrics(); metrics != nil {
			return metrics
		}
	}
	return nil
}
