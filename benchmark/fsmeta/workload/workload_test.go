package workload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

type fakeClient struct {
	mu       sync.RWMutex
	dentries map[string]fsmeta.DentryRecord
	next     fsmeta.InodeID
}

func newFakeClient() *fakeClient {
	return &fakeClient{dentries: make(map[string]fsmeta.DentryRecord), next: 100}
}

func (c *fakeClient) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inode := req.Attrs.InodeRecord(c.next)
	c.next++
	dentry := fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inode.Inode,
		Type:   inode.Type,
	}
	c.dentries[dentryID(req.Parent, req.Name)] = fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inode.Inode,
		Type:   inode.Type,
	}
	return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
}

func (c *fakeClient) Lookup(_ context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return entry, nil
}

func (c *fakeClient) ReadDir(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	return c.readDir(req), nil
}

func (c *fakeClient) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	entries := c.readDir(req)
	out := make([]fsmeta.DentryAttrPair, 0, len(entries))
	for _, entry := range entries {
		out = append(out, fsmeta.DentryAttrPair{
			Dentry: entry,
			Inode:  fsmeta.InodeRecord{Inode: entry.Inode, Type: entry.Type, LinkCount: 1},
		})
	}
	return out, nil
}

func (c *fakeClient) readDir(req fsmeta.ReadDirRequest) []fsmeta.DentryRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]fsmeta.DentryRecord, 0)
	for _, entry := range c.dentries {
		if entry.Parent == req.Parent {
			out = append(out, entry)
		}
	}
	return out
}

func TestRunCheckpointStorm(t *testing.T) {
	result, err := RunCheckpointStorm(context.Background(), newFakeClient(), CheckpointStormConfig{
		Mount:             "vol",
		RunID:             "test",
		Clients:           2,
		Directories:       2,
		FilesPerDirectory: 3,
	})
	require.NoError(t, err)
	require.Equal(t, CheckpointStorm, result.Name)
	result.Driver = DriverNativeFSMetadata
	require.Equal(t, 8, result.Ops)
	require.Zero(t, result.Errors)
	rows := SummaryRows(result)
	require.NotEmpty(t, rows)
	require.Equal(t, DriverNativeFSMetadata, rows[0].Driver)
}

func TestRunHotspotFanIn(t *testing.T) {
	result, err := RunHotspotFanIn(context.Background(), newFakeClient(), HotspotFanInConfig{
		Mount:          "vol",
		RunID:          "test",
		Clients:        2,
		Files:          3,
		ReadsPerClient: 4,
		ReadDirPlus:    true,
	})
	require.NoError(t, err)
	require.Equal(t, HotspotFanIn, result.Name)
	require.Equal(t, 12, result.Ops)
	require.Zero(t, result.Errors)
}

func TestRunWatchSubtree(t *testing.T) {
	result, err := RunWatchSubtree(context.Background(), newFakeWatchClient(), WatchSubtreeConfig{
		Mount:              "vol",
		RunID:              "test",
		Clients:            2,
		Files:              3,
		BackPressureWindow: 8,
	})
	require.NoError(t, err)
	require.Equal(t, WatchSubtree, result.Name)
	require.Equal(t, 7, result.Ops)
	require.Zero(t, result.Errors)
	rows := SummaryRows(result)
	var sawNotify bool
	for _, row := range rows {
		if row.Operation == "watch_notify" {
			sawNotify = true
			require.Equal(t, 3, row.Count)
		}
	}
	require.True(t, sawNotify)
}

func TestRunNegativeLookup(t *testing.T) {
	result, err := RunNegativeLookup(context.Background(), newFakeClient(), NegativeLookupConfig{
		Mount:          "vol",
		RunID:          "test",
		Clients:        2,
		Keys:           3,
		ReadsPerClient: 4,
		Parent:         fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.Equal(t, NegativeLookup, result.Name)
	require.Equal(t, 8, result.Ops)
	require.Zero(t, result.Errors)
	rows := SummaryRows(result)
	require.Len(t, rows, 1)
	require.Equal(t, "lookup_missing", rows[0].Operation)
}

func TestRunDurableSnapshot(t *testing.T) {
	cli := newFakeSnapshotClient()
	result, err := RunDurableSnapshot(context.Background(), cli, DurableSnapshotConfig{
		Mount:     "vol",
		RunID:     "test",
		Files:     3,
		Snapshots: 2,
		PageLimit: 8,
	})
	require.NoError(t, err)
	require.Equal(t, DurableSnapshot, result.Name)
	require.Zero(t, result.Errors)
	rows := SummaryRows(result)
	ops := make(map[string]int, len(rows))
	for _, row := range rows {
		ops[row.Operation] = row.Count
	}
	require.Equal(t, 1, ops["mkdir"])
	require.Equal(t, 3, ops["seed_create"])
	require.Equal(t, 2, ops["snapshot_subtree"])
	require.Equal(t, 2, ops["snapshot_readdirplus"])
	require.Equal(t, 2, ops["retire_snapshot_subtree"])
	cli.mu.RLock()
	snapshots := len(cli.snapshots)
	retired := make(map[uint64]fsmeta.SnapshotSubtreeToken, len(cli.retired))
	for version, token := range cli.retired {
		retired[version] = token
	}
	readVersions := append([]uint64(nil), cli.readVersions...)
	cli.mu.RUnlock()
	require.Zero(t, snapshots)
	require.Len(t, retired, 2)
	require.Len(t, readVersions, 2)
	for _, version := range readVersions {
		require.NotZero(t, version)
		require.Contains(t, retired, version)
	}
}

func TestWriteSummaryCSVIncludesDriver(t *testing.T) {
	var buf bytes.Buffer
	err := WriteSummaryCSV(&buf, []SummaryRow{{
		Workload:  CheckpointStorm,
		Driver:    DriverNativeFSMetadata,
		RunID:     "run-1",
		Operation: "create_checkpoint",
		Count:     1,
	}})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "workload,driver,run_id,operation")
	require.Contains(t, buf.String(), "throughput_ops_sec,active_ops_per_sec,active_duration_sec")
	require.Contains(t, buf.String(), "checkpoint-storm,native-fsmeta,run-1,create_checkpoint")
}

func TestTimeCallRetriesStableRetryableOperationError(t *testing.T) {
	attempts := 0
	_, err := timeCall(func() error {
		attempts++
		if attempts < 3 {
			return nokverrors.RPCStatusError(nokverrors.KindLockConflict, codes.Aborted, "live lock", nil)
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestTimeCallDoesNotRetryPermanentOrCanceledError(t *testing.T) {
	attempts := 0
	_, err := timeCall(func() error {
		attempts++
		return fsmeta.ErrNotFound
	})

	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	require.Equal(t, 1, attempts)

	attempts = 0
	_, err = timeCall(func() error {
		attempts++
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

func dentryID(parent fsmeta.InodeID, name string) string {
	return fmt.Sprintf("%d/%s", parent, name)
}

type fakeWatchClient struct {
	*fakeClient
	stream *fakeWatchStream
}

func newFakeWatchClient() *fakeWatchClient {
	return &fakeWatchClient{
		fakeClient: newFakeClient(),
		stream:     newFakeWatchStream(16),
	}
}

func testMountIdentity(mount fsmeta.MountID) fsmeta.MountIdentity {
	return fsmeta.MountIdentity{MountID: mount, MountKeyID: 1}
}

func (c *fakeWatchClient) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	result, err := c.fakeClient.Create(ctx, req)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	if c.stream == nil {
		return result, nil
	}
	key, err := fsmeta.EncodeDentryKey(testMountIdentity(req.Mount), req.Parent, req.Name)
	if err != nil {
		return fsmeta.CreateResult{}, err
	}
	c.stream.mu.Lock()
	defer c.stream.mu.Unlock()
	if len(c.stream.prefix) > 0 && !bytes.HasPrefix(key, c.stream.prefix) {
		return result, nil
	}
	if c.stream.closed {
		return result, nil
	}
	c.stream.events <- fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: uint64(len(c.stream.events) + 1)},
		CommitVersion: uint64(len(c.stream.events) + 1),
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           key,
	}
	return result, nil
}

func (c *fakeWatchClient) WatchSubtree(_ context.Context, req fsmeta.WatchRequest) (fsmetaclient.WatchSubscription, error) {
	c.stream.mu.Lock()
	defer c.stream.mu.Unlock()
	c.stream.prefix = append([]byte(nil), req.KeyPrefix...)
	return c.stream, nil
}

type fakeSnapshotClient struct {
	*fakeClient
	nextVersion  uint64
	snapshots    map[uint64]fsmeta.SnapshotSubtreeToken
	retired      map[uint64]fsmeta.SnapshotSubtreeToken
	readVersions []uint64
}

func newFakeSnapshotClient() *fakeSnapshotClient {
	return &fakeSnapshotClient{
		fakeClient:  newFakeClient(),
		nextVersion: 1,
		snapshots:   make(map[uint64]fsmeta.SnapshotSubtreeToken),
		retired:     make(map[uint64]fsmeta.SnapshotSubtreeToken),
	}
}

func (c *fakeSnapshotClient) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	c.mu.Lock()
	c.readVersions = append(c.readVersions, req.SnapshotVersion)
	c.mu.Unlock()
	return c.fakeClient.ReadDirPlus(ctx, req)
}

func (c *fakeSnapshotClient) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextVersion++
	token := fsmeta.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: c.nextVersion}
	c.snapshots[token.ReadVersion] = token
	return token, nil
}

func (c *fakeSnapshotClient) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.snapshots, token.ReadVersion)
	c.retired[token.ReadVersion] = token
	return nil
}

type fakeWatchStream struct {
	mu     sync.Mutex
	prefix []byte
	events chan fsmeta.WatchEvent
	closed bool
}

func newFakeWatchStream(size int) *fakeWatchStream {
	return &fakeWatchStream{events: make(chan fsmeta.WatchEvent, size)}
}

func (s *fakeWatchStream) Recv() (fsmeta.WatchEvent, error) {
	evt, ok := <-s.events
	if !ok {
		return fsmeta.WatchEvent{}, io.EOF
	}
	return evt, nil
}

func (s *fakeWatchStream) ReadyCursor() fsmeta.WatchCursor {
	return fsmeta.WatchCursor{}
}

func (s *fakeWatchStream) Ack(fsmeta.WatchCursor) error {
	return nil
}

func (s *fakeWatchStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		close(s.events)
		s.closed = true
	}
	return nil
}
