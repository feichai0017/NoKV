package workload

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

type fakeMixedClient struct {
	mu        sync.Mutex
	dentries  map[string]fsmeta.DentryRecord
	inodes    map[fsmeta.InodeID]fsmeta.InodeRecord
	sessions  map[fsmeta.SessionID]fsmeta.SessionRecord
	snapshots map[uint64]fsmeta.SnapshotSubtreeToken
	versions  map[uint64]struct{}
	reads     []fsmeta.ReadDirRequest
	next      fsmeta.InodeID
	version   uint64
	stream    *fakeWatchStream
}

func newFakeMixedClient() *fakeMixedClient {
	return &fakeMixedClient{
		dentries:  make(map[string]fsmeta.DentryRecord),
		inodes:    make(map[fsmeta.InodeID]fsmeta.InodeRecord),
		sessions:  make(map[fsmeta.SessionID]fsmeta.SessionRecord),
		snapshots: make(map[uint64]fsmeta.SnapshotSubtreeToken),
		versions:  make(map[uint64]struct{}),
		next:      100,
		version:   1,
		stream:    &fakeWatchStream{events: make(chan fsmeta.WatchEvent, 1024)},
	}
}

func (c *fakeMixedClient) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := dentryID(req.Parent, req.Name)
	if _, ok := c.dentries[id]; ok {
		return fsmeta.CreateResult{}, fsmeta.ErrExists
	}
	inode := req.Attrs.InodeRecord(c.next)
	c.next++
	dentry := fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inode.Inode,
		Type:   inode.Type,
	}
	c.inodes[inode.Inode] = inode
	c.dentries[id] = dentry
	c.emitDentryEventLocked(req.Mount, dentry)
	return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
}

func (c *fakeMixedClient) UpdateInode(_ context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok || entry.Inode != req.Inode {
		return fsmeta.InodeRecord{}, fsmeta.ErrNotFound
	}
	inode, ok := c.inodes[req.Inode]
	if !ok {
		return fsmeta.InodeRecord{}, fsmeta.ErrNotFound
	}
	if req.SetSize {
		inode.Size = req.Size
	}
	if req.SetMode {
		inode.Mode = req.Mode
	}
	if req.SetUpdatedUnixNs {
		inode.UpdatedUnixNs = req.UpdatedUnixNs
	}
	if req.SetOpaqueAttrs {
		inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
	}
	c.inodes[req.Inode] = inode
	return inode, nil
}

func (c *fakeMixedClient) Lookup(_ context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return entry, nil
}

func (c *fakeMixedClient) ReadDir(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req)
	out := make([]fsmeta.DentryRecord, 0)
	for _, entry := range c.dentries {
		if entry.Parent == req.Parent {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (c *fakeMixedClient) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req)
	out := make([]fsmeta.DentryAttrPair, 0)
	for _, entry := range c.dentries {
		if entry.Parent != req.Parent {
			continue
		}
		inode, ok := c.inodes[entry.Inode]
		if !ok {
			return nil, fsmeta.ErrNotFound
		}
		out = append(out, fsmeta.DentryAttrPair{Dentry: entry, Inode: inode})
	}
	return out, nil
}

func (c *fakeMixedClient) WatchSubtree(_ context.Context, req fsmeta.WatchRequest) (fsmetaclient.WatchSubscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stream.prefix = append([]byte(nil), req.KeyPrefix...)
	return c.stream, nil
}

func (c *fakeMixedClient) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	token := fsmeta.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: c.version}
	c.snapshots[token.ReadVersion] = token
	return token, nil
}

func (c *fakeMixedClient) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.snapshots, token.ReadVersion)
	return nil
}

func (c *fakeMixedClient) GetReadVersion(context.Context, fsmeta.ReadVersionRequest) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.versions[c.version] = struct{}{}
	return c.version, nil
}

func (c *fakeMixedClient) GetQuotaUsage(context.Context, fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out fsmeta.UsageRecord
	for _, inode := range c.inodes {
		out.Bytes += inode.Size
		out.Inodes++
	}
	return out, nil
}

func (c *fakeMixedClient) Rename(_ context.Context, req fsmeta.RenameRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fromID := dentryID(req.FromParent, req.FromName)
	toID := dentryID(req.ToParent, req.ToName)
	entry, ok := c.dentries[fromID]
	if !ok {
		return fsmeta.ErrNotFound
	}
	if _, exists := c.dentries[toID]; exists {
		return fsmeta.ErrExists
	}
	delete(c.dentries, fromID)
	entry.Parent = req.ToParent
	entry.Name = req.ToName
	c.dentries[toID] = entry
	c.emitDentryEventLocked(req.Mount, entry)
	return nil
}

func (c *fakeMixedClient) Link(_ context.Context, req fsmeta.LinkRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.FromParent, req.FromName)]
	if !ok {
		return fsmeta.ErrNotFound
	}
	inode, ok := c.inodes[entry.Inode]
	if !ok {
		return fsmeta.ErrNotFound
	}
	if inode.Type == fsmeta.InodeTypeDirectory {
		return fsmeta.ErrInvalidValue
	}
	toID := dentryID(req.ToParent, req.ToName)
	if _, exists := c.dentries[toID]; exists {
		return fsmeta.ErrExists
	}
	inode.LinkCount++
	c.inodes[inode.Inode] = inode
	c.dentries[toID] = fsmeta.DentryRecord{
		Parent: req.ToParent,
		Name:   req.ToName,
		Inode:  entry.Inode,
		Type:   entry.Type,
	}
	return nil
}

func (c *fakeMixedClient) Unlink(_ context.Context, req fsmeta.UnlinkRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := dentryID(req.Parent, req.Name)
	entry, ok := c.dentries[id]
	if !ok {
		return fsmeta.ErrNotFound
	}
	delete(c.dentries, id)
	inode, ok := c.inodes[entry.Inode]
	if !ok {
		return nil
	}
	if inode.LinkCount <= 1 {
		delete(c.inodes, entry.Inode)
		return nil
	}
	inode.LinkCount--
	c.inodes[entry.Inode] = inode
	return nil
}

func (c *fakeMixedClient) OpenWriteSession(_ context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inodes[req.Inode]; !ok {
		return fsmeta.SessionRecord{}, fsmeta.ErrNotFound
	}
	record := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: time.Now().Add(req.TTL).UnixNano()}
	c.sessions[req.Session] = record
	return record, nil
}

func (c *fakeMixedClient) HeartbeatWriteSession(_ context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.sessions[req.Session]
	if !ok || record.Inode != req.Inode {
		return fsmeta.SessionRecord{}, fsmeta.ErrNotFound
	}
	record.ExpiresUnixNs = time.Now().Add(req.TTL).UnixNano()
	c.sessions[req.Session] = record
	return record, nil
}

func (c *fakeMixedClient) CloseWriteSession(_ context.Context, req fsmeta.CloseWriteSessionRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sessions, req.Session)
	return nil
}

func (c *fakeMixedClient) ExpireWriteSessions(_ context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixNano()
	var expired uint64
	limit := req.Limit
	if limit == 0 {
		limit = fsmeta.DefaultSessionExpireLimit
	}
	for id, session := range c.sessions {
		if expired >= uint64(limit) {
			break
		}
		if session.ExpiresUnixNs <= now {
			delete(c.sessions, id)
			expired++
		}
	}
	return fsmeta.ExpireWriteSessionsResult{Expired: expired}, nil
}

type retryOpenMixedClient struct {
	*fakeMixedClient
	attempts      int
	firstSession  fsmeta.SessionID
	secondSession fsmeta.SessionID
	firstTTL      time.Duration
	secondTTL     time.Duration
}

func (c *retryOpenMixedClient) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	c.attempts++
	if c.attempts == 1 {
		c.firstSession = req.Session
		c.firstTTL = req.TTL
		return fsmeta.SessionRecord{}, nokverrors.RPCStatusError(nokverrors.KindLockConflict, codes.Aborted, "live lock", nil)
	}
	c.secondSession = req.Session
	c.secondTTL = req.TTL
	return c.fakeMixedClient.OpenWriteSession(ctx, req)
}

func (c *fakeMixedClient) emitDentryEventLocked(mount fsmeta.MountID, entry fsmeta.DentryRecord) {
	if c.stream == nil {
		return
	}
	key, err := fsmeta.EncodeDentryKey(mount, entry.Parent, entry.Name)
	if err != nil || len(c.stream.prefix) > 0 && !bytes.HasPrefix(key, c.stream.prefix) {
		return
	}
	c.version++
	c.stream.events <- fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: c.version},
		CommitVersion: c.version,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           key,
	}
}

func TestRunMixedCoversFullSurface(t *testing.T) {
	cli := newFakeMixedClient()
	result, err := RunMixed(context.Background(), cli, MixedConfig{
		Mount:           "vol",
		RunID:           "test",
		Clients:         2,
		Groups:          2,
		EntriesPerGroup: 2,
		ArtifactsPerRun: 5,
		PageLimit:       8,
		SessionTTL:      time.Hour,
		StaleSessionTTL: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Equal(t, Mixed, result.Name)
	require.Zero(t, result.Errors)

	rows := SummaryRows(result)
	ops := make(map[string]bool, len(rows))
	for _, row := range rows {
		ops[row.Operation] = true
	}
	for _, op := range []string{
		"mkdir_project",
		"mkdir_workspace_dir",
		"mkdir_group",
		"create_dataset",
		"mkdir_run_stage",
		"rename_run_publish",
		"watch_notify",
		"lookup_run",
		"create_artifact",
		"update_inode",
		"open_write_session",
		"heartbeat_write_session",
		"close_write_session",
		"create_checkpoint_stage",
		"update_checkpoint_inode",
		"publish_checkpoint",
		"lookup_checkpoint",
		"link_dataset",
		"unlink_temp",
		"readdir",
		"readdirplus",
		"get_read_version",
		"snapshot_readdirplus",
		"get_quota_usage",
		"open_stale_write_session",
		"expire_write_sessions",
	} {
		require.True(t, ops[op], fmt.Sprintf("missing operation %s", op))
	}
	cli.mu.Lock()
	versions := make(map[uint64]struct{}, len(cli.versions))
	for version := range cli.versions {
		versions[version] = struct{}{}
	}
	reads := append([]fsmeta.ReadDirRequest(nil), cli.reads...)
	cli.mu.Unlock()
	seen := make(map[uint64]struct{})
	for _, req := range reads {
		if req.SnapshotVersion != 0 {
			seen[req.SnapshotVersion] = struct{}{}
		}
	}
	require.NotEmpty(t, versions)
	for version := range versions {
		require.Contains(t, seen, version)
	}
}

func TestWriterSessionOpenUsesFreshLeaseIDAcrossRetry(t *testing.T) {
	base := newFakeMixedClient()
	created, err := base.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 1,
		Name:   "state.bin",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)
	cli := &retryOpenMixedClient{fakeMixedClient: base}
	rec := newRecorder()

	runWriterSessionLifecycle(context.Background(), cli, MixedConfig{
		Mount:      "vol",
		RunID:      "retry-expiry",
		SessionTTL: time.Second,
	}, created.Dentry, mixedTask{group: 1, run: 2}, rec)

	require.Equal(t, 2, cli.attempts)
	require.NotEqual(t, cli.firstSession, cli.secondSession)
	require.Equal(t, cli.firstTTL, cli.secondTTL)
	require.False(t, hasRecordedErrors(rec.snapshot()))
}

func TestRunMixedRequiresNativeClient(t *testing.T) {
	_, err := RunMixed(context.Background(), newFakeClient(), MixedConfig{
		Mount: "vol",
		RunID: "unsupported",
	})
	require.ErrorContains(t, err, "requires native fsmeta full client")
}
