package manifest_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
)

func TestManagerCreateAndRecover(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

	edit := manifest.Edit{
		Type: manifest.EditAddFile,
		File: &manifest.FileMeta{
			Level:     0,
			FileID:    1,
			Size:      123,
			Smallest:  []byte("a"),
			Largest:   []byte("z"),
			CreatedAt: 1,
		},
	}
	if err := mgr.LogEdit(edit); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	version := mgr.Current()
	files := version.Levels[0]
	if len(files) != 1 || files[0].FileID != 1 {
		t.Fatalf("unexpected version: %+v", version)
	}
}

func TestManagerRegionEditRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	meta := manifest.RegionMeta{
		ID:       42,
		StartKey: []byte("alpha"),
		EndKey:   []byte("omega"),
		Epoch: manifest.RegionEpoch{
			Version:     3,
			ConfVersion: 7,
		},
		Peers: []manifest.PeerMeta{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: manifest.RegionStateRunning,
	}
	if err := mgr.LogRegionUpdate(meta); err != nil {
		t.Fatalf("log region: %v", err)
	}
	version := mgr.Current()
	if len(version.Regions) != 1 {
		t.Fatalf("expected one region, got %d", len(version.Regions))
	}
	stored, ok := version.Regions[meta.ID]
	if !ok {
		t.Fatalf("region not found in current version")
	}
	if string(stored.StartKey) != "alpha" || string(stored.EndKey) != "omega" {
		t.Fatalf("unexpected key range: %+v", stored)
	}
	if stored.Epoch.Version != meta.Epoch.Version || stored.Epoch.ConfVersion != meta.Epoch.ConfVersion {
		t.Fatalf("epoch mismatch: %+v", stored.Epoch)
	}
	if len(stored.Peers) != len(meta.Peers) {
		t.Fatalf("peer count mismatch: %+v", stored.Peers)
	}
	if err := mgr.LogRegionDelete(meta.ID); err != nil {
		t.Fatalf("log region delete: %v", err)
	}
	version = mgr.Current()
	if len(version.Regions) != 0 {
		t.Fatalf("expected region deletion to remove entry: %+v", version.Regions)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()
	_, ok = mgr.RegionSnapshot()[meta.ID]
	if ok {
		t.Fatalf("expected region to remain deleted after reopen")
	}
}

func TestManagerLogPointer(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

	edit := manifest.Edit{
		Type:      manifest.EditLogPointer,
		LogSeg:    5,
		LogOffset: 1024,
	}
	if err := mgr.LogEdit(edit); err != nil {
		t.Fatalf("log pointer: %v", err)
	}
	version := mgr.Current()
	if version.LogSegment != 5 || version.LogOffset != 1024 {
		t.Fatalf("log pointer mismatch: %+v", version)
	}
}

func TestManagerRaftPointer(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptr := manifest.RaftLogPointer{
		GroupID:        7,
		Segment:        3,
		Offset:         2048,
		AppliedIndex:   42,
		AppliedTerm:    5,
		Committed:      41,
		SnapshotIndex:  64,
		SnapshotTerm:   7,
		TruncatedIndex: 11,
		TruncatedTerm:  2,
	}
	if err := mgr.LogRaftPointer(ptr); err != nil {
		t.Fatalf("log raft pointer: %v", err)
	}
	version := mgr.Current()
	stored, ok := version.RaftPointers[ptr.GroupID]
	if !ok {
		t.Fatalf("expected raft pointer stored in current version")
	}
	if stored.Segment != ptr.Segment || stored.Offset != ptr.Offset {
		t.Fatalf("unexpected raft pointer %+v", stored)
	}
	if stored.TruncatedIndex != ptr.TruncatedIndex || stored.TruncatedTerm != ptr.TruncatedTerm {
		t.Fatalf("truncated pointer not persisted: %+v", stored)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	recovered, ok := mgr.RaftPointer(ptr.GroupID)
	if !ok {
		t.Fatalf("expected raft pointer after reopen")
	}
	if recovered.Segment != ptr.Segment || recovered.Offset != ptr.Offset || recovered.AppliedIndex != ptr.AppliedIndex {
		t.Fatalf("unexpected pointer after reopen: %+v", recovered)
	}
	if recovered.TruncatedIndex != ptr.TruncatedIndex || recovered.TruncatedTerm != ptr.TruncatedTerm {
		t.Fatalf("truncated fields not recovered: %+v", recovered)
	}
}

func TestManagerValueLog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

	if err := mgr.LogValueLogHead(2, 4096); err != nil {
		t.Fatalf("log value log head: %v", err)
	}
	version := mgr.Current()
	meta := mgr.ValueLogHead()
	if !meta.Valid || meta.FileID != 2 || meta.Offset != 4096 {
		t.Fatalf("value log head mismatch: %+v", meta)
	}
	if err := mgr.LogValueLogDelete(2); err != nil {
		t.Fatalf("log value log delete: %v", err)
	}
	version = mgr.Current()
	if meta, ok := version.ValueLogs[2]; !ok {
		t.Fatalf("expected value log entry tracked after deletion")
	} else if meta.Valid {
		t.Fatalf("expected value log entry marked invalid")
	}
	meta = mgr.ValueLogHead()
	if meta.Valid {
		t.Fatalf("expected head cleared after deletion: %+v", meta)
	}
}

func TestManagerValueLogUpdate(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := mgr.LogValueLogHead(3, 2048); err != nil {
		t.Fatalf("log head: %v", err)
	}
	if err := mgr.LogValueLogDelete(3); err != nil {
		t.Fatalf("delete: %v", err)
	}

	meta := manifest.ValueLogMeta{FileID: 3, Offset: 2048, Valid: true}
	if err := mgr.LogValueLogUpdate(meta); err != nil {
		t.Fatalf("update: %v", err)
	}

	current := mgr.Current()
	restored, ok := current.ValueLogs[3]
	if !ok {
		t.Fatalf("expected fid 3 metadata after update")
	}
	if !restored.Valid || restored.Offset != 2048 {
		t.Fatalf("unexpected restored meta: %+v", restored)
	}
	head := mgr.ValueLogHead()
	if head.Valid {
		t.Fatalf("expected head to remain cleared after update: %+v", head)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	current = mgr.Current()
	restored, ok = current.ValueLogs[3]
	if !ok {
		t.Fatalf("expected fid 3 metadata after reopen")
	}
	if !restored.Valid || restored.Offset != 2048 {
		t.Fatalf("unexpected metadata after reopen: %+v", restored)
	}
}

func TestManagerRegionMetadata(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	meta := manifest.RegionMeta{
		ID:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: manifest.RegionEpoch{
			Version:     3,
			ConfVersion: 5,
		},
		Peers: []manifest.PeerMeta{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 21},
		},
		State: manifest.RegionStateRunning,
	}
	if err := mgr.LogRegionUpdate(meta); err != nil {
		t.Fatalf("log region update: %v", err)
	}
	snap := mgr.RegionSnapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 region, got %d", len(snap))
	}
	got := snap[meta.ID]
	if got.State != manifest.RegionStateRunning || got.Epoch.Version != meta.Epoch.Version {
		t.Fatalf("unexpected region snapshot: %+v", got)
	}
	if len(got.Peers) != len(meta.Peers) || got.Peers[0].StoreID != meta.Peers[0].StoreID {
		t.Fatalf("unexpected peers: %+v", got.Peers)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	snap = mgr.RegionSnapshot()
	if len(snap) != 1 {
		t.Fatalf("expected region after reopen, got %d", len(snap))
	}

	if err := mgr.LogRegionDelete(meta.ID); err != nil {
		t.Fatalf("log region delete: %v", err)
	}
	snap = mgr.RegionSnapshot()
	if len(snap) != 0 {
		t.Fatalf("expected region to be deleted, snapshot=%+v", snap)
	}
}

func TestManagerLogRaftTruncate(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const groupID = uint64(9)
	initial := manifest.RaftLogPointer{
		GroupID:      groupID,
		Segment:      4,
		Offset:       8192,
		AppliedIndex: 100,
		AppliedTerm:  9,
	}
	if err := mgr.LogRaftPointer(initial); err != nil {
		t.Fatalf("log raft pointer: %v", err)
	}
	const segmentID = uint32(6)
	const offset = uint64(512)
	if err := mgr.LogRaftTruncate(groupID, 80, 8, segmentID, offset); err != nil {
		t.Fatalf("log raft truncate: %v", err)
	}
	ptr, ok := mgr.RaftPointer(groupID)
	if !ok {
		t.Fatalf("expected raft pointer stored")
	}
	if ptr.TruncatedIndex != 80 || ptr.TruncatedTerm != 8 {
		t.Fatalf("unexpected truncation fields: %+v", ptr)
	}
	if ptr.SegmentIndex != uint64(segmentID) {
		t.Fatalf("unexpected segment index: %+v", ptr)
	}
	if ptr.TruncatedOffset != offset {
		t.Fatalf("unexpected truncated offset: %+v", ptr)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	ptr, ok = mgr.RaftPointer(groupID)
	if !ok {
		t.Fatalf("expected raft pointer after reopen")
	}
	if ptr.TruncatedIndex != 80 || ptr.TruncatedTerm != 8 {
		t.Fatalf("truncation fields not persisted: %+v", ptr)
	}
	if ptr.SegmentIndex != uint64(segmentID) {
		t.Fatalf("segment index not persisted: %+v", ptr)
	}
	if ptr.TruncatedOffset != offset {
		t.Fatalf("truncated offset not persisted: %+v", ptr)
	}
}

func TestManagerCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	name, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	path := filepath.Join(dir, string(name))
	mgr.Close()

	if err := os.WriteFile(path, []byte("corrupt"), 0o666); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	if _, err := manifest.Open(dir); err == nil {
		t.Fatalf("expected error for corrupt manifest")
	}
}

func TestManagerValueLogReplaySequence(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := mgr.LogValueLogHead(1, 128); err != nil {
		t.Fatalf("log head 1: %v", err)
	}
	if err := mgr.LogValueLogDelete(1); err != nil {
		t.Fatalf("delete head 1: %v", err)
	}
	if err := mgr.LogValueLogHead(2, 4096); err != nil {
		t.Fatalf("log head 2: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	version := mgr.Current()
	if meta1, ok := version.ValueLogs[1]; !ok {
		t.Fatalf("expected fid 1 metadata after replay")
	} else if meta1.Valid {
		t.Fatalf("expected fid 1 to remain invalid after deletion: %+v", meta1)
	}
	if meta2, ok := version.ValueLogs[2]; !ok {
		t.Fatalf("expected fid 2 metadata after replay")
	} else {
		if !meta2.Valid {
			t.Fatalf("expected fid 2 to be valid: %+v", meta2)
		}
		if meta2.Offset != 4096 {
			t.Fatalf("unexpected fid 2 offset: %d", meta2.Offset)
		}
	}
	head := mgr.ValueLogHead()
	if !head.Valid || head.FileID != 2 || head.Offset != 4096 {
		t.Fatalf("unexpected replay head: %+v", head)
	}
}

func TestManifestVerifyTruncatesPartialEdit(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := mgr.LogEdit(manifest.Edit{Type: manifest.EditAddFile, File: &manifest.FileMeta{FileID: 11}}); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	path := filepath.Join(dir, string(current))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	before := info.Size()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(24)); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte("NoK")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	f.Close()

	if err := manifest.Verify(dir); err != nil {
		t.Fatalf("verify: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat after verify: %v", err)
	}
	if info.Size() != before {
		t.Fatalf("expected manifest truncated to %d, got %d", before, info.Size())
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen after verify: %v", err)
	}
	defer mgr.Close()
}

func TestManagerRewrite(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mgr.Close()

	mgr.SetRewriteThreshold(1)

	if err := mgr.LogEdit(manifest.Edit{
		Type: manifest.EditAddFile,
		File: &manifest.FileMeta{Level: 0, FileID: 10, Size: 1},
	}); err != nil {
		t.Fatalf("log edit: %v", err)
	}
	if err := mgr.LogEdit(manifest.Edit{
		Type:      manifest.EditLogPointer,
		LogSeg:    3,
		LogOffset: 128,
	}); err != nil {
		t.Fatalf("log pointer: %v", err)
	}

	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	currentName := strings.TrimSpace(string(current))
	if currentName == "MANIFEST-000001" || currentName == "" {
		t.Fatalf("expected rewritten manifest, got %q", currentName)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mgr, err = manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr.Close()

	version := mgr.Current()
	if len(version.Levels[0]) != 1 || version.Levels[0][0].FileID != 10 {
		t.Fatalf("expected file 10 after rewrite: %+v", version.Levels[0])
	}
	if version.LogSegment != 3 || version.LogOffset != 128 {
		t.Fatalf("expected log pointer preserved, got seg=%d off=%d", version.LogSegment, version.LogOffset)
	}
}
