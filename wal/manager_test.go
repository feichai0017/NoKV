package wal_test

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/wal"
)

func TestManagerAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("zoom"),
	}
	if _, err := m.Append(entries...); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// reopen and replay
	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = m.Close() }()

	var got [][]byte
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		got = append(got, cp)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("entries mismatch: want %d got %d", len(entries), len(got))
	}
	for i := range entries {
		if string(entries[i]) != string(got[i]) {
			t.Fatalf("entry %d mismatch: want %q got %q", i, entries[i], got[i])
		}
	}
}

func TestManagerRotate(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	payload := []byte("record")
	if _, err := m.Append(payload); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := m.Append(payload); err != nil {
		t.Fatalf("append after rotate: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync after rotate: %v", err)
	}
	files, err := m.ListSegments()
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 segments, got %d (%v)", len(files), files)
	}
}

func TestManagerReplayHandlesTruncate(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	if _, err := m.Append([]byte("alpha"), []byte("beta")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	files, err := m.ListSegments()
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	active := files[len(files)-1]
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Truncate the last two bytes to leave at least one full record.
	info, err := os.Stat(active)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(active, info.Size()-2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = m.Close() }()

	var count int
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected at least one record before truncation")
	}
}

func TestManagerReplayChecksumError(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	if _, err := m.Append([]byte("gamma")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	files, err := m.ListSegments()
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	path := files[0]
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt checksum
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Seek(-2, io.SeekEnd); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if _, err := f.Write([]byte{0x12, 0x34}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = m.Close() }()

	err = m.Replay(func(_ wal.EntryInfo, payload []byte) error { return nil })
	if err == nil {
		t.Fatalf("expected checksum error")
	}
}

func TestManagerCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := m.Append([]byte("x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close again: %v", err)
	}
}

// Helper verifying segments sorted order.
func TestManagerListSegmentsSorted(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir, SegmentSize: 128})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	for i := 0; i < 5; i++ {
		payload := []byte("payload-" + string(rune('a'+i)))
		if _, err := m.Append(payload); err != nil {
			t.Fatalf("append: %v", err)
		}
		if i%2 == 1 {
			if err := m.Rotate(); err != nil {
				t.Fatalf("rotate: %v", err)
			}
		}
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	files, err := m.ListSegments()
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if !sort.StringsAreSorted(files) {
		t.Fatalf("files not sorted: %v", files)
	}
}

func TestVerifyDirTruncatesPartialSegment(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payloads := [][]byte{[]byte("alpha"), []byte("beta")}
	if _, err := m.Append(payloads...); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(16)); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte("bad")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	if err := wal.VerifyDir(dir); err != nil {
		t.Fatalf("verify dir: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after verify: %v", err)
	}
	defer func() { _ = m.Close() }()

	count := 0
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != len(payloads) {
		t.Fatalf("expected %d entries after verify, got %d", len(payloads), count)
	}
}

func TestManagerAppendRecordsTyped(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	rec := wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: []byte("raft-data"),
	}
	infos, err := m.AppendRecords(rec)
	if err != nil {
		t.Fatalf("append records: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 record info, got %d", len(infos))
	}
	if infos[0].Type != wal.RecordTypeRaftEntry {
		t.Fatalf("expected record type raft entry, got %v", infos[0].Type)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var seenType wal.RecordType
	if err := m.Replay(func(info wal.EntryInfo, payload []byte) error {
		seenType = info.Type
		if string(payload) != "raft-data" {
			t.Fatalf("unexpected payload: %q", payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if seenType != wal.RecordTypeRaftEntry {
		t.Fatalf("expected replay type raft entry, got %v", seenType)
	}

	metrics := m.Metrics()
	if metrics == nil {
		t.Fatalf("expected wal metrics to be available")
	}
	if metrics.RecordCounts.RaftEntries != 1 {
		t.Fatalf("expected raft entry count to be 1, got %+v", metrics.RecordCounts)
	}
	if metrics.RecordCounts.Entries != 0 || metrics.RecordCounts.RaftStates != 0 || metrics.RecordCounts.RaftSnapshots != 0 {
		t.Fatalf("unexpected record counts %+v", metrics.RecordCounts)
	}
	if metrics.RecordCounts.Total() != 1 {
		t.Fatalf("expected total record count to equal 1, got %d", metrics.RecordCounts.Total())
	}
	if metrics.SegmentsWithRaftRecords != 1 {
		t.Fatalf("expected raft segment count to be 1, got %d", metrics.SegmentsWithRaftRecords)
	}
	segMetrics := m.SegmentRecordMetrics(infos[0].SegmentID)
	if segMetrics.RaftEntries != 1 {
		t.Fatalf("expected segment raft entry count to be 1, got %+v", segMetrics)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	metrics = m.Metrics()
	if metrics.RecordCounts.RaftEntries != 1 {
		t.Fatalf("expected raft entry count to persist after reopen, got %+v", metrics.RecordCounts)
	}
	if metrics.SegmentsWithRaftRecords != 1 {
		t.Fatalf("expected segment count with raft records to persist after reopen, got %d", metrics.SegmentsWithRaftRecords)
	}
}
