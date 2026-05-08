package workload

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
)

const Mixed = "mixed"

// MixedClient is the native fsmeta surface needed by the mixed API workload.
// The workload uses fsmeta semantics directly; a plain KV shim would miss the
// service-side contracts around watch, snapshot, sessions, quota, and rename.
type MixedClient interface {
	WatchClient
	UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error)
	GetReadVersion(ctx context.Context, req fsmeta.ReadVersionRequest) (uint64, error)
	SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error)
	RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error
	GetQuotaUsage(ctx context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error)
	Rename(ctx context.Context, req fsmeta.RenameRequest) error
	Link(ctx context.Context, req fsmeta.LinkRequest) error
	Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error
	OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error)
	HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error)
	CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error
	ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error)
}

type MixedConfig struct {
	Mount           fsmeta.MountID
	RunID           string
	Clients         int
	Groups          int
	EntriesPerGroup int
	// ArtifactsPerRun has an effective floor of 4 because the mixed workload
	// needs prompt.md, plan.json, state.bin, and checkpoint.tmp to exercise
	// update, session, and unlink API paths in every run.
	ArtifactsPerRun int
	PageLimit       uint32
	SessionTTL      time.Duration
	StaleSessionTTL time.Duration
}

type mixedDirs struct {
	project     fsmeta.InodeID
	groups      fsmeta.InodeID
	runs        fsmeta.InodeID
	datasets    fsmeta.InodeID
	checkpoints fsmeta.InodeID
	scratch     fsmeta.InodeID
}

type mixedTask struct {
	group int
	run   int
}

// RunMixed exercises the full fsmeta API through a realistic combined metadata
// lifecycle: workspace bootstrap, staged publication, artifact writes,
// shared-file links, writer sessions, snapshots, watch delivery, quota reads,
// and stale-session cleanup.
func RunMixed(ctx context.Context, cli Client, cfg MixedConfig) (Result, error) {
	native, ok := cli.(MixedClient)
	if !ok {
		return Result{}, fmt.Errorf("mixed requires native fsmeta full client")
	}
	cfg = normalizeMixedConfig(cfg)
	started := time.Now()
	rec := newRecorder()

	dirs, err := createMixedDirs(ctx, native, cfg, rec)
	if err != nil {
		return Result{}, err
	}
	if err := createNamespaceGroupDirectories(ctx, native, cfg, dirs.groups, rec); err != nil {
		return Result{}, err
	}

	tasks := make([]mixedTask, 0, cfg.Groups*cfg.EntriesPerGroup)
	for group := 0; group < cfg.Groups; group++ {
		for run := 0; run < cfg.EntriesPerGroup; run++ {
			tasks = append(tasks, mixedTask{group: group, run: run})
		}
	}
	datasets, err := createDatasetSources(ctx, native, cfg, dirs.datasets, tasks, rec)
	if err != nil {
		return Result{}, err
	}

	prefix, err := fsmeta.EncodeDentryPrefix(cfg.Mount, dirs.runs)
	if err != nil {
		return Result{}, err
	}
	stream, err := native.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: uint32(cfg.Groups*cfg.EntriesPerGroup + 1),
	})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = stream.Close() }()

	warmupKey, err := fsmeta.EncodeDentryKey(cfg.Mount, dirs.runs, "watch-warmup")
	if err != nil {
		return Result{}, err
	}
	if _, err := native.Create(ctx, fsmeta.CreateRequest{
		Mount:  cfg.Mount,
		Parent: dirs.runs,
		Name:   "watch-warmup",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	}); err != nil {
		return Result{}, err
	}
	if err := waitForWatchKey(ctx, stream, warmupKey); err != nil {
		return Result{}, err
	}

	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	starts := newWatchStarts()
	watchDone := make(chan error, 1)
	go collectWatchEvents(watchCtx, stream, starts, cfg.Groups*cfg.EntriesPerGroup, rec, watchDone)

	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Clients; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(next.Add(1)) - 1
				if idx >= len(tasks) {
					return
				}
				runMixedTask(ctx, native, cfg, dirs, datasets, tasks[idx], starts, rec)
			}
		}()
	}
	wg.Wait()
	if hasRecordedErrors(rec.snapshot()) {
		_ = stream.Close()
		cancelWatch()
	} else if err := <-watchDone; err != nil {
		rec.record("watch_notify", 0, err)
	}
	runStaleSessionCleanup(ctx, native, cfg, dirs.scratch, rec)

	return finishResult(Mixed, cfg.RunID, started, rec.snapshot())
}

func createMixedDirs(ctx context.Context, cli MixedClient, cfg MixedConfig, rec *recorder) (mixedDirs, error) {
	projectName := fmt.Sprintf("mixed-workload-%s", cfg.RunID)
	project, err := createDir(ctx, cli, cfg.Mount, fsmeta.RootInode, projectName, "mkdir_project", rec)
	if err != nil {
		return mixedDirs{}, err
	}
	out := mixedDirs{project: project}
	entries := []struct {
		name string
		dst  *fsmeta.InodeID
	}{
		{name: "groups", dst: &out.groups},
		{name: "runs", dst: &out.runs},
		{name: "datasets", dst: &out.datasets},
		{name: "checkpoints", dst: &out.checkpoints},
		{name: "scratch", dst: &out.scratch},
	}
	for _, entry := range entries {
		inode, err := createDir(ctx, cli, cfg.Mount, project, entry.name, "mkdir_workspace_dir", rec)
		if err != nil {
			return mixedDirs{}, err
		}
		*entry.dst = inode
	}
	return out, nil
}

func createDir(ctx context.Context, cli MixedClient, mount fsmeta.MountID, parent fsmeta.InodeID, name, op string, rec *recorder) (fsmeta.InodeID, error) {
	var inode fsmeta.InodeID
	err := recordCall(rec, op, func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  mount,
			Parent: parent,
			Name:   name,
			Attrs: fsmeta.CreateAttrs{
				Type: fsmeta.InodeTypeDirectory,
				Mode: 0o755,
			},
		})
		if err == nil {
			inode = result.Inode.Inode
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	if inode == 0 {
		return 0, fmt.Errorf("%s did not create inode for %q", op, name)
	}
	return inode, nil
}

func createDatasetSources(ctx context.Context, cli MixedClient, cfg MixedConfig, parent fsmeta.InodeID, tasks []mixedTask, rec *recorder) (map[mixedTask]fsmeta.CreateResult, error) {
	// The mixed profile is an API-breadth benchmark. Giving each task its own
	// source inode keeps Link covered without turning CI into a single-inode
	// link_count contention benchmark; dedicated hotspot workloads own that.
	out := make(map[mixedTask]fsmeta.CreateResult, len(tasks))
	for _, task := range tasks {
		task := task
		var dataset fsmeta.CreateResult
		err := recordCall(rec, "create_dataset", func() error {
			result, err := cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: parent,
				Name:   fmt.Sprintf("group-%02d-run-%04d.parquet", task.group, task.run),
				Attrs: fsmeta.CreateAttrs{
					Type: fsmeta.InodeTypeFile,
					Mode: 0o644,
					Size: 64 << 20,
				},
			})
			if err == nil {
				dataset = result
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		if dataset.Inode.Inode == 0 {
			return nil, fmt.Errorf("create_dataset did not create dataset inode")
		}
		out[task] = dataset
	}
	return out, nil
}

func createNamespaceGroupDirectories(ctx context.Context, cli MixedClient, cfg MixedConfig, parent fsmeta.InodeID, rec *recorder) error {
	for group := 0; group < cfg.Groups; group++ {
		_, err := createDir(ctx, cli, cfg.Mount, parent, fmt.Sprintf("group-%02d", group), "mkdir_group", rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func runMixedTask(ctx context.Context, cli MixedClient, cfg MixedConfig, dirs mixedDirs, datasets map[mixedTask]fsmeta.CreateResult, task mixedTask, starts *watchStarts, rec *recorder) {
	stageName := fmt.Sprintf("group-%02d-run-%04d.stage", task.group, task.run)
	finalName := fmt.Sprintf("group-%02d-run-%04d", task.group, task.run)

	var runDir fsmeta.InodeID
	rec.recordCall("mkdir_run_stage", func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: dirs.scratch,
			Name:   stageName,
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory, Mode: 0o755},
		})
		if err == nil {
			runDir = result.Inode.Inode
		}
		return err
	})
	if runDir == 0 {
		return
	}

	key, err := fsmeta.EncodeDentryKey(cfg.Mount, dirs.runs, finalName)
	if err != nil {
		rec.record("rename_run_publish", 0, err)
		return
	}
	if err := recordCall(rec, "rename_run_publish", func() error {
		starts.put(key, time.Now())
		return cli.Rename(ctx, fsmeta.RenameRequest{
			Mount:      cfg.Mount,
			FromParent: dirs.scratch,
			FromName:   stageName,
			ToParent:   dirs.runs,
			ToName:     finalName,
		})
	}); err != nil {
		return
	}

	rec.recordCall("lookup_run", func() error {
		_, err := cli.Lookup(ctx, fsmeta.LookupRequest{Mount: cfg.Mount, Parent: dirs.runs, Name: finalName})
		return err
	})
	artifact := createRunArtifacts(ctx, cli, cfg, runDir, task, rec)
	if artifact.Inode != 0 {
		runWriterSessionLifecycle(ctx, cli, cfg, artifact, task, rec)
	}
	runCheckpointPublish(ctx, cli, cfg, dirs, task, rec)
	dataset, ok := datasets[task]
	rec.recordCall("link_dataset", func() error {
		if !ok {
			return fmt.Errorf("missing dataset source for group=%d run=%d", task.group, task.run)
		}
		return cli.Link(ctx, fsmeta.LinkRequest{
			Mount:      cfg.Mount,
			FromParent: dataset.Dentry.Parent,
			FromName:   dataset.Dentry.Name,
			ToParent:   runDir,
			ToName:     "dataset.parquet",
		})
	})
	rec.recordCall("unlink_temp", func() error {
		return cli.Unlink(ctx, fsmeta.UnlinkRequest{Mount: cfg.Mount, Parent: runDir, Name: "checkpoint.tmp"})
	})
	rec.recordCall("readdir", func() error {
		_, err := cli.ReadDir(ctx, fsmeta.ReadDirRequest{Mount: cfg.Mount, Parent: runDir, Limit: cfg.PageLimit})
		return err
	})
	rec.recordCall("readdirplus", func() error {
		_, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{Mount: cfg.Mount, Parent: runDir, Limit: cfg.PageLimit})
		return err
	})
	runSnapshotLifecycle(ctx, cli, cfg, runDir, rec)
	rec.recordCall("get_quota_usage", func() error {
		_, err := cli.GetQuotaUsage(ctx, fsmeta.QuotaUsageRequest{Mount: cfg.Mount, Scope: dirs.project})
		return err
	})
}

func createRunArtifacts(ctx context.Context, cli MixedClient, cfg MixedConfig, runDir fsmeta.InodeID, task mixedTask, rec *recorder) fsmeta.DentryRecord {
	var state fsmeta.DentryRecord
	names := []string{"prompt.md", "plan.json", "state.bin", "checkpoint.tmp"}
	for i := 4; i < cfg.ArtifactsPerRun; i++ {
		names = append(names, fmt.Sprintf("artifact-%02d.bin", i-4))
	}
	for _, name := range names {
		name := name
		rec.recordCall("create_artifact", func() error {
			result, err := cli.Create(ctx, fsmeta.CreateRequest{
				Mount:  cfg.Mount,
				Parent: runDir,
				Name:   name,
				Attrs: fsmeta.CreateAttrs{
					Type: fsmeta.InodeTypeFile,
					Mode: 0o644,
					Size: uint64(1024 + task.run),
				},
			})
			if err == nil && name == "state.bin" {
				state = result.Dentry
			}
			return err
		})
	}
	if state.Inode != 0 {
		rec.recordCall("update_inode", func() error {
			_, err := cli.UpdateInode(ctx, fsmeta.UpdateInodeRequest{
				Mount:            cfg.Mount,
				Parent:           runDir,
				Inode:            state.Inode,
				Name:             state.Name,
				SetSize:          true,
				Size:             uint64(4096 + task.group*cfg.EntriesPerGroup + task.run),
				SetUpdatedUnixNs: true,
				UpdatedUnixNs:    time.Now().UnixNano(),
			})
			return err
		})
	}
	return state
}

func runCheckpointPublish(ctx context.Context, cli MixedClient, cfg MixedConfig, dirs mixedDirs, task mixedTask, rec *recorder) {
	stageName := fmt.Sprintf("group-%02d-run-%04d-checkpoint.tmp", task.group, task.run)
	finalName := fmt.Sprintf("group-%02d-run-%04d-checkpoint", task.group, task.run)
	var checkpoint fsmeta.DentryRecord
	rec.recordCall("create_checkpoint_stage", func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: dirs.scratch,
			Name:   stageName,
			Attrs: fsmeta.CreateAttrs{
				Type: fsmeta.InodeTypeFile,
				Mode: 0o644,
				Size: uint64(16 << 20),
			},
		})
		if err == nil {
			checkpoint = result.Dentry
		}
		return err
	})
	if checkpoint.Inode != 0 {
		rec.recordCall("update_checkpoint_inode", func() error {
			_, err := cli.UpdateInode(ctx, fsmeta.UpdateInodeRequest{
				Mount:            cfg.Mount,
				Parent:           dirs.scratch,
				Inode:            checkpoint.Inode,
				Name:             checkpoint.Name,
				SetSize:          true,
				Size:             uint64(32<<20 + task.run),
				SetUpdatedUnixNs: true,
				UpdatedUnixNs:    time.Now().UnixNano(),
			})
			return err
		})
	}
	if err := recordCall(rec, "publish_checkpoint", func() error {
		return cli.Rename(ctx, fsmeta.RenameRequest{
			Mount:      cfg.Mount,
			FromParent: dirs.scratch,
			FromName:   stageName,
			ToParent:   dirs.checkpoints,
			ToName:     finalName,
		})
	}); err != nil {
		return
	}
	rec.recordCall("lookup_checkpoint", func() error {
		_, err := cli.Lookup(ctx, fsmeta.LookupRequest{Mount: cfg.Mount, Parent: dirs.checkpoints, Name: finalName})
		return err
	})
}

func runWriterSessionLifecycle(ctx context.Context, cli MixedClient, cfg MixedConfig, entry fsmeta.DentryRecord, task mixedTask, rec *recorder) {
	sessionPrefix := fsmeta.SessionID(fmt.Sprintf("group-%s-%02d-%04d", cfg.RunID, task.group, task.run))
	var session fsmeta.SessionID
	openAttempt := 0
	if err := recordCall(rec, "open_write_session", func() error {
		openAttempt++
		candidate := sessionPrefix
		if openAttempt > 1 {
			// OpenWriteSession is a lease acquisition, not an idempotent create.
			// If an earlier attempt left an ambiguous primary lock, a real client
			// can abandon that lease id and request a fresh one.
			candidate = fsmeta.SessionID(fmt.Sprintf("%s-retry-%02d", sessionPrefix, openAttempt))
		}
		_, err := cli.OpenWriteSession(ctx, fsmeta.OpenWriteSessionRequest{
			Mount:   cfg.Mount,
			Inode:   entry.Inode,
			Session: candidate,
			TTL:     cfg.SessionTTL,
		})
		if err == nil {
			session = candidate
		}
		return err
	}); err != nil {
		return
	}
	if err := recordCall(rec, "heartbeat_write_session", func() error {
		_, err := cli.HeartbeatWriteSession(ctx, fsmeta.HeartbeatWriteSessionRequest{
			Mount:   cfg.Mount,
			Inode:   entry.Inode,
			Session: session,
			TTL:     2 * cfg.SessionTTL,
		})
		return err
	}); err != nil {
		return
	}
	rec.recordCall("close_write_session", func() error {
		return cli.CloseWriteSession(ctx, fsmeta.CloseWriteSessionRequest{Mount: cfg.Mount, Inode: entry.Inode, Session: session})
	})
}

func runSnapshotLifecycle(ctx context.Context, cli MixedClient, cfg MixedConfig, root fsmeta.InodeID, rec *recorder) {
	var readVersion uint64
	if err := recordCall(rec, "get_read_version", func() error {
		var err error
		readVersion, err = cli.GetReadVersion(ctx, fsmeta.ReadVersionRequest{Mount: cfg.Mount})
		return err
	}); err != nil {
		return
	}
	rec.recordCall("snapshot_readdirplus", func() error {
		if readVersion == 0 {
			return fmt.Errorf("zero read version from GetReadVersion")
		}
		_, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
			Mount:           cfg.Mount,
			Parent:          root,
			Limit:           cfg.PageLimit,
			SnapshotVersion: readVersion,
		})
		return err
	})
}

func runStaleSessionCleanup(ctx context.Context, cli MixedClient, cfg MixedConfig, parent fsmeta.InodeID, rec *recorder) {
	var stale fsmeta.CreateResult
	rec.recordCall("create_stale_session_target", func() error {
		result, err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  cfg.Mount,
			Parent: parent,
			Name:   "stale-session-target.bin",
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
		})
		if err == nil {
			stale = result
		}
		return err
	})
	if stale.Inode.Inode == 0 {
		return
	}
	session := fsmeta.SessionID("stale-" + cfg.RunID)
	rec.recordCall("open_stale_write_session", func() error {
		_, err := cli.OpenWriteSession(ctx, fsmeta.OpenWriteSessionRequest{
			Mount:   cfg.Mount,
			Inode:   stale.Inode.Inode,
			Session: session,
			TTL:     cfg.StaleSessionTTL,
		})
		return err
	})
	wait := cfg.StaleSessionTTL + 10*time.Millisecond
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		rec.record("expire_write_sessions", 0, ctx.Err())
		return
	}
	rec.recordCall("expire_write_sessions", func() error {
		_, err := cli.ExpireWriteSessions(ctx, fsmeta.ExpireWriteSessionsRequest{
			Mount: cfg.Mount,
			Limit: fsmeta.DefaultSessionExpireLimit,
		})
		return err
	})
}

func normalizeMixedConfig(cfg MixedConfig) MixedConfig {
	if cfg.Mount == "" {
		cfg.Mount = "fsmeta-workload"
	}
	if cfg.RunID == "" {
		cfg.RunID = NewRunID()
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 4
	}
	if cfg.Groups <= 0 {
		cfg.Groups = 4
	}
	if cfg.EntriesPerGroup <= 0 {
		cfg.EntriesPerGroup = 8
	}
	if cfg.ArtifactsPerRun < 4 {
		cfg.ArtifactsPerRun = 4
	}
	if cfg.PageLimit == 0 {
		cfg.PageLimit = 64
	}
	if cfg.PageLimit > fsmeta.MaxReadDirLimit {
		cfg.PageLimit = fsmeta.MaxReadDirLimit
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 2 * time.Second
	}
	if cfg.StaleSessionTTL <= 0 {
		cfg.StaleSessionTTL = cfg.SessionTTL
	}
	return cfg
}

func recordCall(rec *recorder, operation string, fn func() error) error {
	duration, err := timeCall(fn)
	rec.record(operation, duration, err)
	return err
}

func hasRecordedErrors(samples []Sample) bool {
	for _, sample := range samples {
		if sample.Error != "" {
			return true
		}
	}
	return false
}
