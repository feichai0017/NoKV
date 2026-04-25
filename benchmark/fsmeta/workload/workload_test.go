package workload

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/stretchr/testify/require"
)

type fakeClient struct {
	dentries map[string]fsmeta.DentryRecord
}

func newFakeClient() *fakeClient {
	return &fakeClient{dentries: make(map[string]fsmeta.DentryRecord)}
}

func (c *fakeClient) Create(_ context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	c.dentries[dentryID(req.Parent, req.Name)] = fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   inode.Type,
	}
	return nil
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
		StartInode:        100,
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
		StartInode:     200,
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
		StartInode:         300,
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

func TestWriteSummaryCSVIncludesDriver(t *testing.T) {
	var buf bytes.Buffer
	err := WriteSummaryCSV(&buf, []SummaryRow{{
		Workload:  CheckpointStorm,
		Driver:    DriverGenericKV,
		RunID:     "run-1",
		Operation: "create_checkpoint",
		Count:     1,
	}})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "workload,driver,run_id,operation")
	require.Contains(t, buf.String(), "checkpoint-storm,generic-kv,run-1,create_checkpoint")
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
		stream:     &fakeWatchStream{events: make(chan fsmeta.WatchEvent, 16)},
	}
}

func (c *fakeWatchClient) Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	if err := c.fakeClient.Create(ctx, req, inode); err != nil {
		return err
	}
	if c.stream == nil {
		return nil
	}
	key, err := fsmeta.EncodeDentryKey(req.Mount, req.Parent, req.Name)
	if err != nil {
		return err
	}
	if len(c.stream.prefix) > 0 && !bytes.HasPrefix(key, c.stream.prefix) {
		return nil
	}
	c.stream.events <- fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: uint64(len(c.stream.events) + 1)},
		CommitVersion: uint64(len(c.stream.events) + 1),
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           key,
	}
	return nil
}

func (c *fakeWatchClient) WatchSubtree(_ context.Context, req fsmeta.WatchRequest) (fsmetaclient.WatchSubscription, error) {
	c.stream.prefix = append([]byte(nil), req.KeyPrefix...)
	return c.stream, nil
}

type fakeWatchStream struct {
	prefix []byte
	events chan fsmeta.WatchEvent
}

func (s *fakeWatchStream) Recv() (fsmeta.WatchEvent, error) {
	return <-s.events, nil
}

func (s *fakeWatchStream) Ack(fsmeta.WatchCursor) error {
	return nil
}

func (s *fakeWatchStream) Close() error {
	return nil
}
