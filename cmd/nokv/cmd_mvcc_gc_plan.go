package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/fsmeta"
	rootclient "github.com/feichai0017/NoKV/meta/root/client"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
)

var loadMVCCGCRootRetention = loadMVCCGCRootRetentionFromAddr

type mountFloorFlags map[string]uint64

func (f *mountFloorFlags) Set(raw string) error {
	mount, value, ok := strings.Cut(raw, "=")
	if !ok || mount == "" || value == "" {
		return fmt.Errorf("mount floor must be mount=read_version")
	}
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("parse mount floor %q: %w", raw, err)
	}
	if version == 0 {
		return fmt.Errorf("mount floor must be greater than zero")
	}
	if *f == nil {
		*f = make(map[string]uint64)
	}
	(*f)[mount] = version
	return nil
}

func (f *mountFloorFlags) String() string {
	if f == nil || *f == nil {
		return ""
	}
	parts := make([]string, 0, len(*f))
	for mount, version := range *f {
		parts = append(parts, fmt.Sprintf("%s=%d", mount, version))
	}
	return strings.Join(parts, ",")
}

type mvccGCCommandOptions struct {
	workDir            string
	requestedSafePoint uint64
	txnFloor           uint64
	txnFloorFromLocks  bool
	globalFloor        uint64
	mountFloors        mountFloorFlags
	metaRootAddr       string
	metaRootTimeout    time.Duration
	batchEntries       int
	maxKeys            uint64
	timeBudget         time.Duration
	asJSON             bool
}

type mvccGCResolveLocksOptions struct {
	workDir    string
	currentTs  uint64
	batchLocks int
	asJSON     bool
}

type mvccGCOrphanDefaultsOptions struct {
	workDir      string
	batchEntries int
	asJSON       bool
}

var (
	mvccGCPlanModes = []raftmode.Mode{
		raftmode.ModeStandalone,
		raftmode.ModePreparing,
		raftmode.ModeSeeded,
		raftmode.ModeCluster,
	}
	mvccGCApplyModes = []raftmode.Mode{
		raftmode.ModeStandalone,
	}
)

func parseMVCCGCCommandOptions(name string, args []string, includeBatchEntries bool) (mvccGCCommandOptions, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	var opt mvccGCCommandOptions
	fs.StringVar(&opt.workDir, "workdir", "", "database work directory")
	fs.Uint64Var(&opt.requestedSafePoint, "safe-point", 0, "requested MVCC GC safe point")
	fs.Uint64Var(&opt.txnFloor, "txn-floor", 0, "oldest active transaction version")
	fs.BoolVar(&opt.txnFloorFromLocks, "txn-floor-from-locks", false, "scan CFLock and use the oldest active lock as transaction floor")
	fs.Uint64Var(&opt.globalFloor, "global-floor", 0, "global snapshot retention floor")
	fs.StringVar(&opt.metaRootAddr, "meta-root-addr", "", "metadata-root gRPC address used to load active snapshot retention floors")
	fs.DurationVar(&opt.metaRootTimeout, "meta-root-timeout", 5*time.Second, "metadata-root RPC timeout")
	fs.BoolVar(&opt.asJSON, "json", false, "output JSON instead of plain text")
	fs.Var(&opt.mountFloors, "mount-floor", "mount-specific snapshot retention floor mount=read_version (repeatable)")
	if includeBatchEntries {
		fs.IntVar(&opt.batchEntries, "batch-entries", 0, "maximum tombstones per apply batch")
		fs.Uint64Var(&opt.maxKeys, "max-keys", 0, "maximum user keys scanned by one apply pass")
		fs.DurationVar(&opt.timeBudget, "time-budget", 0, "maximum wall-clock duration for one apply pass")
	}
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return opt, err
	}
	if opt.workDir == "" {
		return opt, fmt.Errorf("workdir is required")
	}
	if opt.requestedSafePoint == 0 {
		return opt, fmt.Errorf("safe-point is required and must be greater than zero")
	}
	if opt.batchEntries < 0 {
		return opt, fmt.Errorf("batch-entries must be non-negative")
	}
	if opt.timeBudget < 0 {
		return opt, fmt.Errorf("time-budget must be non-negative")
	}
	if opt.metaRootTimeout <= 0 {
		return opt, fmt.Errorf("meta-root-timeout must be positive")
	}
	return opt, nil
}

func parseMVCCGCResolveLocksOptions(args []string) (mvccGCResolveLocksOptions, error) {
	fs := flag.NewFlagSet("mvcc-gc resolve-locks", flag.ContinueOnError)
	var opt mvccGCResolveLocksOptions
	fs.StringVar(&opt.workDir, "workdir", "", "database work directory")
	fs.Uint64Var(&opt.currentTs, "current-ts", 0, "current timestamp used for lock TTL checks")
	fs.IntVar(&opt.batchLocks, "batch-locks", 0, "maximum expired locks per resolve batch")
	fs.BoolVar(&opt.asJSON, "json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return opt, err
	}
	if opt.workDir == "" {
		return opt, fmt.Errorf("workdir is required")
	}
	if opt.currentTs == 0 {
		return opt, fmt.Errorf("current-ts is required and must be greater than zero")
	}
	if opt.batchLocks < 0 {
		return opt, fmt.Errorf("batch-locks must be non-negative")
	}
	return opt, nil
}

func parseMVCCGCOrphanDefaultsOptions(args []string) (mvccGCOrphanDefaultsOptions, error) {
	fs := flag.NewFlagSet("mvcc-gc orphan-defaults", flag.ContinueOnError)
	var opt mvccGCOrphanDefaultsOptions
	fs.StringVar(&opt.workDir, "workdir", "", "database work directory")
	fs.IntVar(&opt.batchEntries, "batch-entries", 0, "maximum default tombstones per apply batch")
	fs.BoolVar(&opt.asJSON, "json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return opt, err
	}
	if opt.workDir == "" {
		return opt, fmt.Errorf("workdir is required")
	}
	if opt.batchEntries < 0 {
		return opt, fmt.Errorf("batch-entries must be non-negative")
	}
	return opt, nil
}

func (o mvccGCCommandOptions) policy(ctx context.Context, db txnstore.Store) (storemvcc.SafePointPolicy, error) {
	retention := rootstate.SnapshotRetentionIndex{
		GlobalFloor: o.globalFloor,
		MountFloors: cloneMountFloors(map[string]uint64(o.mountFloors)),
	}
	txnFloor := o.txnFloor
	if o.txnFloorFromLocks {
		floor, err := storemvcc.PlanTxnFloor(ctx, db)
		if err != nil {
			return storemvcc.SafePointPolicy{}, err
		}
		txnFloor = minNonZero(txnFloor, floor.OldestStartTs)
	}
	if strings.TrimSpace(o.metaRootAddr) != "" {
		rootCtx, cancel := context.WithTimeout(ctx, o.metaRootTimeout)
		defer cancel()
		rootRetention, err := loadMVCCGCRootRetention(rootCtx, strings.TrimSpace(o.metaRootAddr))
		if err != nil {
			return storemvcc.SafePointPolicy{}, err
		}
		retention = mergeSnapshotRetention(retention, rootRetention)
	}
	return storemvcc.SafePointPolicy{
		RequestedSafePoint: o.requestedSafePoint,
		TxnFloor:           txnFloor,
		SnapshotRetention:  retention,
		Mount:              fsmeta.StringMountResolver,
	}, nil
}

func runMVCCGCPlanCmd(w io.Writer, args []string) error {
	opt, err := parseMVCCGCCommandOptions("mvcc-gc plan", args, false)
	if err != nil {
		return err
	}

	db, err := openMVCCGCStore(opt.workDir, mvccGCPlanModes)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	policy, err := opt.policy(ctx, db)
	if err != nil {
		return err
	}
	stats, err := storemvcc.Plan(ctx, db, policy)
	if err != nil {
		return err
	}
	return renderMVCCGCPlan(w, stats, opt.asJSON)
}

func runMVCCGCCmd(w io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("mvcc-gc requires subcommand plan, apply, resolve-locks, or orphan-defaults")
	}
	switch args[0] {
	case "plan":
		return runMVCCGCPlanCmd(w, args[1:])
	case "apply":
		return runMVCCGCApplyCmd(w, args[1:])
	case "resolve-locks":
		return runMVCCGCResolveLocksCmd(w, args[1:])
	case "orphan-defaults":
		return runMVCCGCOrphanDefaultsCmd(w, args[1:])
	default:
		return fmt.Errorf("unknown mvcc-gc subcommand %q", args[0])
	}
}

func runMVCCGCApplyCmd(w io.Writer, args []string) error {
	opt, err := parseMVCCGCCommandOptions("mvcc-gc apply", args, true)
	if err != nil {
		return err
	}

	db, err := openMVCCGCStore(opt.workDir, mvccGCApplyModes)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if opt.timeBudget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opt.timeBudget)
		defer cancel()
	}
	policy, err := opt.policy(ctx, db)
	if err != nil {
		return err
	}
	stats, err := storemvcc.Apply(ctx, db, policy, storemvcc.ApplyOptions{
		BatchEntries: opt.batchEntries,
		MaxKeys:      opt.maxKeys,
	})
	if err != nil {
		return err
	}
	return renderMVCCGCApply(w, stats, opt.asJSON)
}

func runMVCCGCResolveLocksCmd(w io.Writer, args []string) error {
	opt, err := parseMVCCGCResolveLocksOptions(args)
	if err != nil {
		return err
	}
	db, err := openMVCCGCStore(opt.workDir, mvccGCApplyModes)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	stats, err := storemvcc.ResolveExpiredLocks(context.Background(), db, storemvcc.ResolveLocksOptions{
		CurrentTs:  opt.currentTs,
		BatchLocks: opt.batchLocks,
	})
	if err != nil {
		return err
	}
	return renderMVCCGCResolveLocks(w, stats, opt.asJSON)
}

func runMVCCGCOrphanDefaultsCmd(w io.Writer, args []string) error {
	opt, err := parseMVCCGCOrphanDefaultsOptions(args)
	if err != nil {
		return err
	}
	db, err := openMVCCGCStore(opt.workDir, mvccGCApplyModes)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	stats, err := storemvcc.ApplyOrphanDefaults(context.Background(), db, storemvcc.OrphanDefaultOptions{BatchEntries: opt.batchEntries})
	if err != nil {
		return err
	}
	return renderMVCCGCOrphanDefaults(w, stats, opt.asJSON)
}

func openMVCCGCStore(workDir string, allowedModes []raftmode.Mode) (*NoKV.DB, error) {
	metaStore, err := localmeta.OpenLocalStore(workDir, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = metaStore.Close() }()

	opts := NoKV.NewDefaultOptions()
	opts.WorkDir = workDir
	opts.RaftPointerSnapshot = metaStore.RaftPointerSnapshot
	opts.AllowedModes = allowedModes
	db, err := NoKV.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open db for MVCC GC: %w", err)
	}
	return db, nil
}

func loadMVCCGCRootRetentionFromAddr(ctx context.Context, addr string) (rootstate.SnapshotRetentionIndex, error) {
	client, err := rootclient.Dial(ctx, addr)
	if err != nil {
		return rootstate.SnapshotRetentionIndex{}, err
	}
	defer func() { _ = client.Close() }()
	snapshot, err := client.Snapshot()
	if err != nil {
		return rootstate.SnapshotRetentionIndex{}, err
	}
	return snapshot.SnapshotRetentionIndex(), nil
}

func cloneMountFloors(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for mount, floor := range in {
		if floor != 0 {
			out[mount] = floor
		}
	}
	return out
}

func mergeSnapshotRetention(a, b rootstate.SnapshotRetentionIndex) rootstate.SnapshotRetentionIndex {
	out := rootstate.SnapshotRetentionIndex{
		GlobalFloor: minNonZero(a.GlobalFloor, b.GlobalFloor),
		MountFloors: cloneMountFloors(a.MountFloors),
	}
	for mount, floor := range b.MountFloors {
		if floor == 0 {
			continue
		}
		out.MountFloors[mount] = minNonZero(out.MountFloors[mount], floor)
	}
	return out
}

func minNonZero(a, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if b == 0 || a < b {
		return a
	}
	return b
}

func renderMVCCGCPlan(w io.Writer, stats storemvcc.PlanStats, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	renderMVCCGCPlanPlain(w, stats)
	return nil
}

func renderMVCCGCApply(w io.Writer, stats storemvcc.ApplyStats, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	renderMVCCGCPlanPlain(w, stats.PlanStats)
	_, _ = fmt.Fprintf(w, "MVCCGC.AppliedWriteDeletes   %d\n", stats.AppliedWriteDeletes)
	_, _ = fmt.Fprintf(w, "MVCCGC.AppliedDefaultDeletes %d\n", stats.AppliedDefaultDeletes)
	return nil
}

func renderMVCCGCResolveLocks(w io.Writer, stats storemvcc.ResolveLocksStats, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	_, _ = fmt.Fprintf(w, "MVCCGC.ScannedLocks       %d\n", stats.ScannedLocks)
	_, _ = fmt.Fprintf(w, "MVCCGC.ExpiredLocks       %d\n", stats.ExpiredLocks)
	_, _ = fmt.Fprintf(w, "MVCCGC.RetainedLocks      %d\n", stats.RetainedLocks)
	_, _ = fmt.Fprintf(w, "MVCCGC.ResolvedLocks      %d\n", stats.ResolvedLocks)
	_, _ = fmt.Fprintf(w, "MVCCGC.CommittedLocks     %d\n", stats.CommittedLocks)
	_, _ = fmt.Fprintf(w, "MVCCGC.RolledBackLocks    %d\n", stats.RolledBackLocks)
	_, _ = fmt.Fprintf(w, "MVCCGC.DeletedLockMarkers %d\n", stats.DeletedLockMarkers)
	return nil
}

func renderMVCCGCOrphanDefaults(w io.Writer, stats storemvcc.OrphanDefaultStats, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	_, _ = fmt.Fprintf(w, "MVCCGC.ScannedDefaults       %d\n", stats.ScannedDefaults)
	_, _ = fmt.Fprintf(w, "MVCCGC.OrphanDefaults        %d\n", stats.OrphanDefaults)
	_, _ = fmt.Fprintf(w, "MVCCGC.RetainedDefaults      %d\n", stats.RetainedDefaults)
	_, _ = fmt.Fprintf(w, "MVCCGC.AppliedDefaultDeletes %d\n", stats.AppliedDefaultDeletes)
	_, _ = fmt.Fprintf(w, "MVCCGC.DeletedDefaultMarkers %d\n", stats.DeletedDefaultMarkers)
	return nil
}

func renderMVCCGCPlanPlain(w io.Writer, stats storemvcc.PlanStats) {
	_, _ = fmt.Fprintf(w, "MVCCGC.ScannedKeys          %d\n", stats.ScannedKeys)
	_, _ = fmt.Fprintf(w, "MVCCGC.DroppableKeys        %d\n", stats.DroppableKeys)
	_, _ = fmt.Fprintf(w, "MVCCGC.WriteVersions        %d\n", stats.WriteVersions)
	_, _ = fmt.Fprintf(w, "MVCCGC.RetainedWrites       %d\n", stats.RetainedWrites)
	_, _ = fmt.Fprintf(w, "MVCCGC.DroppableWrites      %d\n", stats.DroppableWrites)
	_, _ = fmt.Fprintf(w, "MVCCGC.AnchorWrites         %d\n", stats.AnchorWrites)
	_, _ = fmt.Fprintf(w, "MVCCGC.RetainedDefaultRefs  %d\n", stats.RetainedDefaultRefs)
	_, _ = fmt.Fprintf(w, "MVCCGC.DeletedWriteMarkers  %d\n", stats.DeletedWriteMarkers)
	_, _ = fmt.Fprintf(w, "MVCCGC.SafePointClampedKeys %d\n", stats.SafePointClampedKeys)
	_, _ = fmt.Fprintf(w, "MVCCGC.MaxVersionsPerKey    %d\n", stats.MaxVersionsPerKey)
	_, _ = fmt.Fprintf(w, "MVCCGC.EffectiveSafePoint   min=%d max=%d\n",
		stats.MinEffectiveSafePoint, stats.MaxEffectiveSafePoint)
}
