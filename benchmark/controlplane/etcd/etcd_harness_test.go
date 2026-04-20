package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	benchHelperEnv        = "NOKV_CONTROL_PLANE_ETCD_BENCH_HELPER"
	benchHelperMemberMode = "etcd-member-server"
)

func TestControlPlaneEtcdBenchHelperProcess(t *testing.T) {
	if os.Getenv(benchHelperEnv) != "1" {
		t.Skip("etcd benchmark helper only")
	}

	args := helperProcessArgs()
	if len(args) == 0 {
		os.Exit(2)
	}
	switch args[0] {
	case benchHelperMemberMode:
		runEtcdMemberHelperProcess(args[1:])
	default:
		os.Exit(2)
	}
}

type etcdClusterHarness struct {
	members map[string]*etcdClusterMember
}

type etcdClusterMember struct {
	name      string
	clientURL string
	peerURL   string
	workdir   string
	proc      *exec.Cmd
	client    *clientv3.Client
	paused    bool
}

func openEtcdClusterHarness(t *testing.T, names []string) *etcdClusterHarness {
	t.Helper()
	require.NotEmpty(t, names)

	type memberSpec struct {
		name      string
		clientURL string
		peerURL   string
		workdir   string
	}
	specs := make([]memberSpec, 0, len(names))
	clusterEntries := make([]string, 0, len(names))
	for _, name := range names {
		clientURL := mustFreeURL(t, "http")
		peerURL := mustFreeURL(t, "http")
		specs = append(specs, memberSpec{
			name:      name,
			clientURL: clientURL.String(),
			peerURL:   peerURL.String(),
			workdir:   t.TempDir(),
		})
		clusterEntries = append(clusterEntries, fmt.Sprintf("%s=%s", name, peerURL.String()))
	}
	initialCluster := strings.Join(clusterEntries, ",")

	h := &etcdClusterHarness{members: make(map[string]*etcdClusterMember, len(specs))}
	for _, spec := range specs {
		proc := startBenchHelperProcess(t, benchHelperMemberMode, spec.name, spec.clientURL, spec.peerURL, spec.workdir, initialCluster)
		member := &etcdClusterMember{
			name:      spec.name,
			clientURL: spec.clientURL,
			peerURL:   spec.peerURL,
			workdir:   spec.workdir,
			proc:      proc,
		}
		h.members[spec.name] = member
		t.Cleanup(func() { stopBenchHelperProcess(proc) })
	}

	for _, member := range h.members {
		waitForEtcdReady(t, member.clientURL)
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{member.clientURL},
			DialTimeout: 5 * time.Second,
			Logger:      zap.NewNop(),
		})
		require.NoError(t, err)
		member.client = client
		t.Cleanup(func() { require.NoError(t, client.Close()) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := h.currentLeaderName(ctx)
	require.NoError(t, err)
	return h
}

func (h *etcdClusterHarness) currentLeaderName(ctx context.Context) (string, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for name, member := range h.members {
			if member.paused {
				continue
			}
			resp, err := member.client.Status(ctx, member.clientURL)
			if err != nil || resp == nil || resp.Header == nil {
				continue
			}
			if resp.Header.MemberId == resp.Leader {
				return name, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("timed out waiting for current leader")
}

func (h *etcdClusterHarness) pauseMember(name string) error {
	member, ok := h.members[name]
	if !ok {
		return fmt.Errorf("unknown member %q", name)
	}
	if member.paused {
		return nil
	}
	if member.proc == nil || member.proc.Process == nil {
		return fmt.Errorf("member %q has no process", name)
	}
	if err := member.proc.Process.Signal(syscall.SIGSTOP); err != nil {
		return err
	}
	member.paused = true
	return nil
}

func (h *etcdClusterHarness) resumeMember(name string) error {
	member, ok := h.members[name]
	if !ok {
		return fmt.Errorf("unknown member %q", name)
	}
	if !member.paused {
		return nil
	}
	if member.proc == nil || member.proc.Process == nil {
		return fmt.Errorf("member %q has no process", name)
	}
	if err := member.proc.Process.Signal(syscall.SIGCONT); err != nil {
		return err
	}
	member.paused = false
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (h *etcdClusterHarness) waitForLeaderChange(ctx context.Context, oldLeader string) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		leader, err := h.currentLeaderName(ctx)
		if err == nil && leader != oldLeader {
			return leader, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func mustDecodeEtcdReplyTrace[T any](t *testing.T, raw []T, format coordaudit.ReplyTraceFormat) []coordaudit.ReplyTraceRecord {
	t.Helper()
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	records, err := coordaudit.DecodeReplyTrace(data, format)
	require.NoError(t, err)
	return records
}

func mustDecodeEtcdReadIndexTrace(t *testing.T, raw []etcdReadIndexTraceRecord) []coordaudit.ReplyTraceRecord {
	return mustDecodeEtcdReplyTrace(t, raw, coordaudit.ReplyTraceFormatEtcdReadIndex)
}

func mustDecodeEtcdLeaseRenewTrace(t *testing.T, raw []etcdLeaseRenewTraceRecord) []coordaudit.ReplyTraceRecord {
	return mustDecodeEtcdReplyTrace(t, raw, coordaudit.ReplyTraceFormatEtcdLeaseRenew)
}

type etcdWALEntrySummary struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Type      string `json:"type"`
	DataBytes int    `json:"data_bytes"`
}

type etcdWALSummary struct {
	Member            string                `json:"member"`
	WALDir            string                `json:"wal_dir"`
	SnapshotCount     int                   `json:"snapshot_count"`
	LastSnapshotIndex uint64                `json:"last_snapshot_index"`
	LastSnapshotTerm  uint64                `json:"last_snapshot_term"`
	HardStateTerm     uint64                `json:"hard_state_term"`
	HardStateVote     uint64                `json:"hard_state_vote"`
	HardStateCommit   uint64                `json:"hard_state_commit"`
	EntryCount        int                   `json:"entry_count"`
	LastEntryIndex    uint64                `json:"last_entry_index"`
	LastEntryTerm     uint64                `json:"last_entry_term"`
	LastEntries       []etcdWALEntrySummary `json:"last_entries"`
}

func loadEtcdWALSummary(member, workdir string) (etcdWALSummary, error) {
	walDir := filepath.Join(workdir, "member", "wal")
	summary := etcdWALSummary{
		Member: member,
		WALDir: walDir,
	}
	logger := zap.NewNop()

	snapshots, err := wal.ValidSnapshotEntries(logger, walDir)
	if err != nil {
		return summary, fmt.Errorf("read wal snapshots for %s: %w", member, err)
	}
	summary.SnapshotCount = len(snapshots)
	if len(snapshots) > 0 {
		last := snapshots[len(snapshots)-1]
		summary.LastSnapshotIndex = last.Index
		summary.LastSnapshotTerm = last.Term
	}

	reader, err := wal.OpenForRead(logger, walDir, walpb.Snapshot{})
	if err != nil {
		return summary, fmt.Errorf("open wal for %s: %w", member, err)
	}
	defer func() { _ = reader.Close() }()

	_, state, entries, err := reader.ReadAll()
	if err != nil {
		return summary, fmt.Errorf("read wal for %s: %w", member, err)
	}
	summary.HardStateTerm = state.Term
	summary.HardStateVote = state.Vote
	summary.HardStateCommit = state.Commit
	summary.EntryCount = len(entries)
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		summary.LastEntryIndex = last.Index
		summary.LastEntryTerm = last.Term
	}
	summary.LastEntries = summarizeWALEntries(entries, 3)
	return summary, nil
}

func summarizeWALEntries(entries []raftpb.Entry, keep int) []etcdWALEntrySummary {
	if len(entries) == 0 || keep <= 0 {
		return nil
	}
	start := max(0, len(entries)-keep)
	summary := make([]etcdWALEntrySummary, 0, len(entries)-start)
	for _, entry := range entries[start:] {
		summary = append(summary, etcdWALEntrySummary{
			Index:     entry.Index,
			Term:      entry.Term,
			Type:      walEntryTypeName(entry.Type),
			DataBytes: len(entry.Data),
		})
	}
	return summary
}

func walEntryTypeName(entryType raftpb.EntryType) string {
	switch entryType {
	case raftpb.EntryNormal:
		return "normal"
	case raftpb.EntryConfChange:
		return "conf_change"
	case raftpb.EntryConfChangeV2:
		return "conf_change_v2"
	default:
		return fmt.Sprintf("type_%d", entryType)
	}
}

func (h *etcdClusterHarness) loadWALSummaries() ([]etcdWALSummary, error) {
	names := make([]string, 0, len(h.members))
	for name := range h.members {
		names = append(names, name)
	}
	slices.Sort(names)
	summaries := make([]etcdWALSummary, 0, len(names))
	for _, name := range names {
		summary, err := loadEtcdWALSummary(name, h.members[name].workdir)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func helperProcessArgs() []string {
	for i, arg := range os.Args {
		if arg == "--" && i+1 < len(os.Args) {
			return os.Args[i+1:]
		}
	}
	return nil
}

func startBenchHelperProcess(tb testing.TB, mode string, args ...string) *exec.Cmd {
	tb.Helper()
	cmdArgs := []string{"-test.run=TestControlPlaneEtcdBenchHelperProcess", "--", mode}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), benchHelperEnv+"=1")
	require.NoError(tb, cmd.Start())
	return cmd
}

func stopBenchHelperProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func runEtcdMemberHelperProcess(args []string) {
	if len(args) != 5 {
		os.Exit(2)
	}
	name, clientAddr, peerAddr, workdir, initialCluster := args[0], args[1], args[2], args[3], args[4]
	clientURL, err := url.Parse(clientAddr)
	if err != nil {
		os.Exit(1)
	}
	peerURL, err := url.Parse(peerAddr)
	if err != nil {
		os.Exit(1)
	}
	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = workdir
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"/dev/null"}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = initialCluster

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		os.Exit(1)
	}
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		server.Close()
		os.Exit(1)
	}
	waitForBenchTermination(server.Close)
}

func waitForBenchTermination(stop func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	<-signals
	stop()
}

func waitForTCPOpen(tb testing.TB, addr string) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for tcp listener at %s", addr)
}

func waitForEtcdReady(tb testing.TB, endpoint string) {
	tb.Helper()
	u, err := url.Parse(endpoint)
	require.NoError(tb, err)
	waitForTCPOpen(tb, u.Host)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: 200 * time.Millisecond,
			Logger:      zap.NewNop(),
		})
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, getErr := client.Get(ctx, "/")
			cancel()
			_ = client.Close()
			if getErr == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for etcd helper at %s", endpoint)
}

func mustFreeAddr(tb testing.TB) string {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	addr := listener.Addr().String()
	require.NoError(tb, listener.Close())
	return addr
}

func mustFreeURL(tb testing.TB, scheme string) url.URL {
	tb.Helper()
	addr := mustFreeAddr(tb)
	u, err := url.Parse(fmt.Sprintf("%s://%s", scheme, addr))
	require.NoError(tb, err)
	return *u
}

func waitForGRPCReady(tb testing.TB, addr string) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for gRPC helper at %s", addr)
}
