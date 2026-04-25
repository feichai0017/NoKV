package workload

import (
	"context"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
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
	require.Equal(t, 8, result.Ops)
	require.Zero(t, result.Errors)
	require.NotEmpty(t, SummaryRows(result))
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

func dentryID(parent fsmeta.InodeID, name string) string {
	return fmt.Sprintf("%d/%s", parent, name)
}
