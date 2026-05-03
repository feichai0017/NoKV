package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/metrics"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	raftmode "github.com/feichai0017/NoKV/runtime/mode"
	"github.com/feichai0017/NoKV/runtime/stats"
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

	var snap stats.StatsSnapshot
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

func renderStats(w io.Writer, snap stats.StatsSnapshot, asJSON bool) error {
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
	if snap.MVCCGC.Enabled || snap.MVCCGC.Runs > 0 || snap.MVCCGC.DroppableWrites > 0 || snap.MVCCGC.ActiveLocks > 0 {
		_, _ = fmt.Fprintf(w, "MVCCGC.Plan            enabled=%v runs=%d lastMs=%.2f\n",
			snap.MVCCGC.Enabled, snap.MVCCGC.Runs, snap.MVCCGC.LastDurationMs)
		if snap.MVCCGC.SkippedRuns > 0 {
			_, _ = fmt.Fprintf(w, "MVCCGC.PlanSkipped     safePoint=0 runs=%d\n", snap.MVCCGC.SkippedRuns)
		}
		if snap.MVCCGC.LastError != "" {
			_, _ = fmt.Fprintf(w, "MVCCGC.Warning         %s\n", snap.MVCCGC.LastError)
		}
		_, _ = fmt.Fprintf(w, "MVCCGC.TxnFloor        activeLocks=%d oldest=%d max=%d\n",
			snap.MVCCGC.ActiveLocks, snap.MVCCGC.OldestStartTs, snap.MVCCGC.MaxStartTs)
		_, _ = fmt.Fprintf(w, "MVCCGC.Candidates      keys=%d droppableKeys=%d writes=%d droppableWrites=%d\n",
			snap.MVCCGC.ScannedKeys,
			snap.MVCCGC.DroppableKeys,
			snap.MVCCGC.WriteVersions,
			snap.MVCCGC.DroppableWrites,
		)
		if snap.MVCCGC.SafePointClampedKeys > 0 || snap.MVCCGC.MaxVersionsPerKey > 0 {
			_, _ = fmt.Fprintf(w, "MVCCGC.Policy          clampedKeys=%d maxVersionsPerKey=%d safePointMin=%d safePointMax=%d\n",
				snap.MVCCGC.SafePointClampedKeys,
				snap.MVCCGC.MaxVersionsPerKey,
				snap.MVCCGC.MinEffectiveSafePoint,
				snap.MVCCGC.MaxEffectiveSafePoint,
			)
		}
	}
	if snap.MVCCGC.MaintenanceEnabled ||
		snap.MVCCGC.MaintenanceRuns > 0 ||
		snap.MVCCGC.ResolvedLocks > 0 ||
		snap.MVCCGC.AppliedWriteDeletes > 0 ||
		snap.MVCCGC.AppliedDefaultDeletes > 0 ||
		snap.MVCCGC.OrphanDefaults > 0 {
		_, _ = fmt.Fprintf(w, "MVCCGC.Maintenance     enabled=%v runs=%d lastMs=%.2f\n",
			snap.MVCCGC.MaintenanceEnabled,
			snap.MVCCGC.MaintenanceRuns,
			snap.MVCCGC.MaintenanceLastDurationMs,
		)
		if snap.MVCCGC.MaintenanceLastError != "" {
			_, _ = fmt.Fprintf(w, "MVCCGC.MaintWarning    %s\n", snap.MVCCGC.MaintenanceLastError)
		}
		if snap.MVCCGC.MaintenanceResolveError != "" {
			_, _ = fmt.Fprintf(w, "MVCCGC.ResolveWarning  %s\n", snap.MVCCGC.MaintenanceResolveError)
		}
		if snap.MVCCGC.MaintenanceApplyError != "" {
			_, _ = fmt.Fprintf(w, "MVCCGC.ApplyWarning    %s\n", snap.MVCCGC.MaintenanceApplyError)
		}
		if snap.MVCCGC.MaintenanceOrphanError != "" {
			_, _ = fmt.Fprintf(w, "MVCCGC.OrphanWarning   %s\n", snap.MVCCGC.MaintenanceOrphanError)
		}
		if snap.MVCCGC.MaintenanceSafePointSkipped {
			_, _ = fmt.Fprintln(w, "MVCCGC.Apply           skipped safePoint=0")
		}
		if snap.MVCCGC.ScannedLocks > 0 ||
			snap.MVCCGC.ExpiredLocks > 0 ||
			snap.MVCCGC.ResolvedLocks > 0 ||
			snap.MVCCGC.DeletedLockMarkers > 0 {
			_, _ = fmt.Fprintf(w, "MVCCGC.ResolveLocks    scanned=%d expired=%d resolved=%d committed=%d rolledBack=%d deleted=%d\n",
				snap.MVCCGC.ScannedLocks,
				snap.MVCCGC.ExpiredLocks,
				snap.MVCCGC.ResolvedLocks,
				snap.MVCCGC.CommittedLocks,
				snap.MVCCGC.RolledBackLocks,
				snap.MVCCGC.DeletedLockMarkers,
			)
		}
		if snap.MVCCGC.AppliedWriteDeletes > 0 || snap.MVCCGC.AppliedDefaultDeletes > 0 {
			_, _ = fmt.Fprintf(w, "MVCCGC.Apply           writeDeletes=%d defaultDeletes=%d\n",
				snap.MVCCGC.AppliedWriteDeletes,
				snap.MVCCGC.AppliedDefaultDeletes,
			)
		}
		if snap.MVCCGC.OrphanDefaults > 0 || snap.MVCCGC.AppliedOrphanDefaults > 0 {
			_, _ = fmt.Fprintf(w, "MVCCGC.OrphanDefaults  found=%d appliedDeletes=%d\n",
				snap.MVCCGC.OrphanDefaults,
				snap.MVCCGC.AppliedOrphanDefaults,
			)
		}
	}
	if snap.LSM.ValueBytesTotal > 0 {
		_, _ = fmt.Fprintf(w, "LSM.ValueBytesTotal   %d\n", snap.LSM.ValueBytesTotal)
	}
	if len(snap.LSM.Levels) > 0 {
		_, _ = fmt.Fprintln(w, "LSM.Levels:")
		for _, lvl := range snap.LSM.Levels {
			_, _ = fmt.Fprintf(w, "  - L%d tables=%d size=%dB value=%dB stale=%dB",
				lvl.Level, lvl.TableCount, lvl.SizeBytes, lvl.ValueBytes, lvl.StaleBytes)
			if lvl.LandingTables > 0 {
				_, _ = fmt.Fprintf(w, " landingTables=%d landingSize=%dB landingValue=%dB",
					lvl.LandingTables, lvl.LandingSizeBytes, lvl.LandingValueBytes)
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
	return nil
}

func localStatsSnapshot(workDir string, attachMetrics bool) (stats.StatsSnapshot, error) {
	if workDir == "" {
		return stats.StatsSnapshot{}, fmt.Errorf("workdir is required")
	}
	metaStore, err := localmeta.OpenLocalStore(workDir, nil)
	if err != nil {
		return stats.StatsSnapshot{}, err
	}
	defer func() { _ = metaStore.Close() }()
	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = workDir
	opts.RaftPointerSnapshot = raftstorestats.RaftLogPointers(metaStore.RaftPointerSnapshot)
	opts.AllowedModes = []raftmode.Mode{
		raftmode.ModeStandalone,
		raftmode.ModePreparing,
		raftmode.ModeSeeded,
		raftmode.ModeCluster,
	}
	db, err := NoKV.Open(opts)
	if err != nil {
		return stats.StatsSnapshot{}, fmt.Errorf("open db for offline stats: %w", err)
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

func fetchExpvarSnapshot(url string) (stats.StatsSnapshot, error) {
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
		return stats.StatsSnapshot{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return stats.StatsSnapshot{}, fmt.Errorf("expvar request failed: %s", resp.Status)
	}
	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return stats.StatsSnapshot{}, err
	}
	return parseExpvarSnapshot(data), nil
}

func parseExpvarSnapshot(data map[string]any) stats.StatsSnapshot {
	var snap stats.StatsSnapshot
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
