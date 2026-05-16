// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
)

type fakeMetadataClient struct {
	mu        sync.Mutex
	dentries  map[string]fsmeta.DentryRecord
	inodes    map[fsmeta.InodeID]fsmeta.InodeRecord
	sessions  map[fakeSessionKey]fsmeta.SessionRecord
	snapshots map[uint64]fsmeta.SnapshotSubtreeToken
	reads     []fsmeta.ReadDirRequest
	next      fsmeta.InodeID
	version   uint64
	streams   []*fakeWatchStream
}

type fakeSessionKey struct {
	inode   fsmeta.InodeID
	session fsmeta.SessionID
}

func newFakeMetadataClient() *fakeMetadataClient {
	return &fakeMetadataClient{
		dentries:  make(map[string]fsmeta.DentryRecord),
		inodes:    make(map[fsmeta.InodeID]fsmeta.InodeRecord),
		sessions:  make(map[fakeSessionKey]fsmeta.SessionRecord),
		snapshots: make(map[uint64]fsmeta.SnapshotSubtreeToken),
		next:      100,
		version:   1,
	}
}

func (c *fakeMetadataClient) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
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

func (c *fakeMetadataClient) UpdateInode(_ context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
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

func (c *fakeMetadataClient) Lookup(_ context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return entry, nil
}

func (c *fakeMetadataClient) LookupPlus(_ context.Context, req fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok {
		return fsmeta.DentryAttrPair{}, fsmeta.ErrNotFound
	}
	inode, ok := c.inodes[entry.Inode]
	if !ok {
		return fsmeta.DentryAttrPair{}, fsmeta.ErrNotFound
	}
	return fsmeta.DentryAttrPair{Dentry: entry, Inode: inode}, nil
}

func (c *fakeMetadataClient) ReadDir(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req)
	return c.readDirLocked(req), nil
}

func (c *fakeMetadataClient) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req)
	entries := c.readDirLocked(req)
	out := make([]fsmeta.DentryAttrPair, 0, len(entries))
	for _, entry := range entries {
		inode, ok := c.inodes[entry.Inode]
		if !ok {
			return nil, fsmeta.ErrNotFound
		}
		out = append(out, fsmeta.DentryAttrPair{Dentry: entry, Inode: inode})
	}
	return out, nil
}

func (c *fakeMetadataClient) readDirLocked(req fsmeta.ReadDirRequest) []fsmeta.DentryRecord {
	out := make([]fsmeta.DentryRecord, 0)
	for _, entry := range c.dentries {
		if entry.Parent != req.Parent {
			continue
		}
		if req.StartAfter != "" && entry.Name <= req.StartAfter {
			continue
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if req.Limit > 0 && len(out) > int(req.Limit) {
		out = out[:req.Limit]
	}
	return out
}

func (c *fakeMetadataClient) WatchSubtree(_ context.Context, req fsmeta.WatchRequest) (fsmetaclient.WatchSubscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix, err := fsmeta.WatchPrefixForMount(req, testMountIdentity(req.Mount))
	if err != nil {
		return nil, err
	}
	size := int(req.BackPressureWindow)
	if size <= 0 {
		size = 4096
	}
	stream := newFakeWatchStream(size)
	stream.prefix = prefix
	c.streams = append(c.streams, stream)
	return stream, nil
}

func (c *fakeMetadataClient) GetReadVersion(context.Context, fsmeta.ReadVersionRequest) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	return c.version, nil
}

func (c *fakeMetadataClient) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	token := fsmeta.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: c.version}
	c.snapshots[token.ReadVersion] = token
	return token, nil
}

func (c *fakeMetadataClient) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.snapshots, token.ReadVersion)
	return nil
}

func (c *fakeMetadataClient) Rename(_ context.Context, req fsmeta.RenameRequest) error {
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

func (c *fakeMetadataClient) Unlink(_ context.Context, req fsmeta.UnlinkRequest) error {
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

func (c *fakeMetadataClient) OpenWriteSession(_ context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inodes[req.Inode]; !ok {
		return fsmeta.SessionRecord{}, fsmeta.ErrNotFound
	}
	record := fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: time.Now().Add(req.TTL).UnixNano()}
	c.sessions[fakeSessionKey{inode: req.Inode, session: req.Session}] = record
	return record, nil
}

func (c *fakeMetadataClient) HeartbeatWriteSession(_ context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fakeSessionKey{inode: req.Inode, session: req.Session}
	record, ok := c.sessions[key]
	if !ok || record.Inode != req.Inode {
		return fsmeta.SessionRecord{}, fsmeta.ErrNotFound
	}
	record.ExpiresUnixNs = time.Now().Add(req.TTL).UnixNano()
	c.sessions[key] = record
	return record, nil
}

func (c *fakeMetadataClient) CloseWriteSession(_ context.Context, req fsmeta.CloseWriteSessionRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fakeSessionKey{inode: req.Inode, session: req.Session}
	if _, ok := c.sessions[key]; !ok {
		return fsmeta.ErrNotFound
	}
	delete(c.sessions, key)
	return nil
}

func (c *fakeMetadataClient) ExpireWriteSessions(_ context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixNano()
	limit := req.Limit
	if limit == 0 {
		limit = fsmeta.DefaultSessionExpireLimit
	}
	var expired uint64
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

func (c *fakeMetadataClient) emitDentryEventLocked(mount fsmeta.MountID, entry fsmeta.DentryRecord) {
	if len(c.streams) == 0 {
		return
	}
	key, err := fsmeta.EncodeDentryKey(testMountIdentity(mount), entry.Parent, entry.Name)
	if err != nil {
		return
	}
	c.version++
	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: c.version},
		CommitVersion: c.version,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           key,
	}
	for _, stream := range c.streams {
		stream.mu.Lock()
		if !stream.closed && bytes.HasPrefix(key, stream.prefix) {
			stream.events <- evt
		}
		stream.mu.Unlock()
	}
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

func dentryID(parent fsmeta.InodeID, name string) string {
	return fmt.Sprintf("%d/%s", parent, name)
}

func testMountIdentity(mount fsmeta.MountID) fsmeta.MountIdentity {
	return fsmeta.MountIdentity{MountID: mount, MountKeyID: 1}
}
