package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultSegmentSize = 64 << 20  // 64 MiB
	minSegmentSize     = 64 << 10  // 64 KiB
	defaultBufferSize  = 256 << 10 // 256 KiB
)

// Config controls WAL manager behaviour.
type Config struct {
	Dir         string
	SegmentSize int64
	FileMode    os.FileMode
	SyncOnWrite bool
	BufferSize  int
}

// EntryInfo describes an entry written to WAL.
type EntryInfo struct {
	SegmentID uint32
	Offset    int64
	Length    uint32
	Type      RecordType
}

// Metrics captures runtime information about WAL manager state.
type Metrics struct {
	ActiveSegment           uint32
	SegmentCount            int
	ActiveSize              int64
	RemovedSegments         uint64
	RecordCounts            RecordMetrics
	SegmentsWithRaftRecords int
}

// RecordMetrics summarises counts per record type.
type RecordMetrics struct {
	Entries       uint64 `json:"entries"`
	RaftEntries   uint64 `json:"raft_entries"`
	RaftStates    uint64 `json:"raft_states"`
	RaftSnapshots uint64 `json:"raft_snapshots"`
	Other         uint64 `json:"other"`
}

// Total returns the sum across all record types.
func (m RecordMetrics) Total() uint64 {
	return m.Entries + m.RaftEntries + m.RaftStates + m.RaftSnapshots + m.Other
}

func (m *RecordMetrics) add(recType RecordType) {
	switch recType {
	case RecordTypeEntry:
		m.Entries++
	case RecordTypeRaftEntry:
		m.RaftEntries++
	case RecordTypeRaftState:
		m.RaftStates++
	case RecordTypeRaftSnapshot:
		m.RaftSnapshots++
	default:
		m.Other++
	}
}

func (m RecordMetrics) RaftRecords() uint64 {
	return m.RaftEntries + m.RaftStates + m.RaftSnapshots
}

// Manager provides append-only WAL segments with replay support.
type Manager struct {
	cfg Config

	mu              sync.Mutex
	active          *os.File
	activeID        uint32
	activeSize      int64
	closed          bool
	segmentSize     int64
	removedSegments uint64
	bufferSize      int
	writer          *bufio.Writer
	recordTotals    RecordMetrics
	segmentTotals   map[uint32]RecordMetrics
}

// RecordType identifies the kind of payload stored in the WAL.
type RecordType uint8

const (
	// RecordTypeEntry represents an LSM mutation (default behaviour).
	RecordTypeEntry RecordType = iota
	// RecordTypeRaftEntry encodes a batch of raft log entries.
	RecordTypeRaftEntry
	// RecordTypeRaftState encodes a raft HardState update.
	RecordTypeRaftState
	// RecordTypeRaftSnapshot encodes a raft snapshot payload.
	RecordTypeRaftSnapshot
)

// Record describes a typed WAL payload.
//
// The WAL record is stored on disk in the following format:
//
//  +--------+-----------+-----------+---------+
//  | Length | Type      | Payload   | CRC32   |
//  | [4]byte| [1]byte   | [N]byte   | [4]byte |
//  +--------+-----------+-----------+---------+
//
// - Length: The length of the Type and Payload fields.
// - Type: The type of the record, as defined by RecordType.
// - Payload: The record's data.
// - CRC32: A CRC32 checksum of the Type and Payload.
type Record struct {
	Type    RecordType
	Payload []byte
}

func DecodeRecord(r io.Reader) (RecordType, []byte, uint32, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, io.EOF
		}
		return 0, nil, 0, err
	}

	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return 0, nil, 0, ErrEmptyRecord
	}

	// Allocate buffer for type byte + payload
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	expected := binary.BigEndian.Uint32(crcBuf[:])
	hasher := kv.CRC32()
	// Calculate CRC over type byte + payload
	if _, err := hasher.Write(buf); err != nil {
		kv.PutCRC32(hasher)
		return 0, nil, 0, err
	}
	sum := hasher.Sum32()
	kv.PutCRC32(hasher)
	if expected != sum {
		return 0, nil, 0, kv.ErrBadChecksum
	}

	recType := RecordType(buf[0])
	payload := buf[1:] // Payload is the rest after the type byte
	
	return recType, payload, length, nil
}

func EncodeRecord(w io.Writer, recType RecordType, payload []byte) (int, error) {
	total := len(payload) + 1 // Type byte + payload length
	length := uint32(total)

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], length)
	if _, err := w.Write(hdr[:]); err != nil {
		return 0, err
	}

	typeByte := byte(recType)
	if _, err := w.Write([]byte{typeByte}); err != nil {
		return 0, err
	}

	if _, err := w.Write(payload); err != nil {
		return 0, err
	}

	hasher := kv.CRC32()
	typeBuf := [1]byte{typeByte}
	if _, err := hasher.Write(typeBuf[:]); err != nil {
		kv.PutCRC32(hasher)
		return 0, err
	}
	if _, err := hasher.Write(payload); err != nil {
		kv.PutCRC32(hasher)
		return 0, err
	}
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], hasher.Sum32())
	kv.PutCRC32(hasher)
	if _, err := w.Write(crcBuf[:]); err != nil {
		return 0, err
	}

	return int(length) + 8, nil // length + 4 bytes for header + 4 bytes for CRC
}

// Open creates or resumes a WAL manager.
func Open(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("wal: directory required")
	}
	if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	segSize := cfg.SegmentSize
	if segSize == 0 {
		segSize = defaultSegmentSize
	}
	if segSize < minSegmentSize {
		segSize = minSegmentSize
	}

	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = defaultBufferSize
	}

	m := &Manager{
		cfg:           cfg,
		segmentSize:   segSize,
		bufferSize:    bufSize,
		segmentTotals: make(map[uint32]RecordMetrics, 16),
	}
	if err := m.openLatestSegment(); err != nil {
		return nil, err
	}
	if err := m.rebuildRecordCounts(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) openLatestSegment() error {
	files, err := filepath.Glob(filepath.Join(m.cfg.Dir, "*.wal"))
	if err != nil {
		return err
	}
	var ids []int
	for _, f := range files {
		var id int
		_, err := fmt.Sscanf(filepath.Base(f), "%05d.wal", &id)
		if err == nil {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	if len(ids) == 0 {
		return m.switchSegmentLocked(1, true)
	}
	last := ids[len(ids)-1]
	return m.switchSegmentLocked(uint32(last), false)
}

func (m *Manager) rebuildRecordCounts() error {
	totals := RecordMetrics{}
	segmentTotals := make(map[uint32]RecordMetrics, 16)
	err := m.Replay(func(info EntryInfo, _ []byte) error {
		metrics := segmentTotals[info.SegmentID]
		metrics.add(info.Type)
		segmentTotals[info.SegmentID] = metrics
		totals.add(info.Type)
		return nil
	})
	if err != nil {
		m.mu.Lock()
		if m.segmentTotals == nil {
			m.segmentTotals = make(map[uint32]RecordMetrics, 16)
		}
		m.recordTotals = RecordMetrics{}
		m.mu.Unlock()
		return nil
	}
	m.mu.Lock()
	m.segmentTotals = segmentTotals
	m.recordTotals = totals
	m.mu.Unlock()
	return nil
}

func (m *Manager) segmentPath(id uint32) string {
	return filepath.Join(m.cfg.Dir, fmt.Sprintf("%05d.wal", id))
}

func (m *Manager) switchSegmentLocked(id uint32, truncate bool) error {
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			return err
		}
	}
	if m.active != nil {
		if err := m.active.Sync(); err != nil {
			return err
		}
		if err := m.active.Close(); err != nil {
			return err
		}
	}

	path := m.segmentPath(id)
	flag := os.O_CREATE | os.O_RDWR
	if truncate {
		flag |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flag, m.cfg.FileMode)
	if err != nil {
		return err
	}

	var size int64
	if !truncate {
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		size = info.Size()
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			f.Close()
			return err
		}
	}
	m.active = f
	m.activeID = id
	if truncate {
		m.activeSize = 0
	} else {
		m.activeSize = size
	}
	m.writer = bufio.NewWriterSize(f, m.bufferSize)
	return nil
}

func (m *Manager) switchSegment(id uint32, truncate bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("wal: manager closed")
	}
	return m.switchSegmentLocked(id, truncate)
}

// Append appends one or more payloads to WAL and returns their locations.
func (m *Manager) Append(payloads ...[]byte) ([]EntryInfo, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	records := make([]Record, len(payloads))
	for i, p := range payloads {
		records[i] = Record{
			Type:    RecordTypeEntry,
			Payload: p,
		}
	}
	return m.AppendRecords(records...)
}

// AppendRecords appends typed records to WAL and returns their locations.
func (m *Manager) AppendRecords(records ...Record) ([]EntryInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("wal: manager closed")
	}
	results := make([]EntryInfo, len(records))
	for i, rec := range records {
		payload := rec.Payload
		totalRecordSize := len(payload) + 1 + 4 + 4 // Type + Payload + Length field + CRC field
		if err := m.ensureCapacity(int64(totalRecordSize)); err != nil {
			return nil, err
		}
		offset := m.activeSize
		
		n, err := EncodeRecord(m.writer, rec.Type, payload)
		if err != nil {
			return nil, err
		}
		
		m.activeSize += int64(n)
		results[i] = EntryInfo{
			SegmentID: m.activeID,
			Offset:    offset,
			Length:    uint32(len(payload) + 1),
			Type:      rec.Type,
		}
		segMetrics := m.segmentTotals[m.activeID]
		segMetrics.add(rec.Type)
		m.segmentTotals[m.activeID] = segMetrics
		m.recordTotals.add(rec.Type)
	}
	if m.cfg.SyncOnWrite {
		if err := m.writer.Flush(); err != nil {
			return nil, err
		}
		if err := m.active.Sync(); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (m *Manager) ensureCapacity(need int64) error {
	if m.activeSize+need <= m.segmentSize {
		return nil
	}
	return m.rotateLocked()
}

// Rotate forces creation of a new WAL segment.
func (m *Manager) Rotate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("wal: manager closed")
	}
	return m.rotateLocked()
}

func (m *Manager) rotateLocked() error {
	nextID := m.activeID + 1
	return m.switchSegmentLocked(nextID, true)
}

// Sync fsyncs the active segment.
func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("wal: manager closed")
	}
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			return err
		}
	}
	return m.active.Sync()
}

// ActiveSegment returns current segment ID.
func (m *Manager) ActiveSegment() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeID
}

// ListSegments lists existing WAL files sorted ascending.
func (m *Manager) ListSegments() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files, err := filepath.Glob(filepath.Join(m.cfg.Dir, "*.wal"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// Replay traverses all WAL segments and feeds entries to callback.
func (m *Manager) Replay(fn func(info EntryInfo, payload []byte) error) error {
	m.mu.Lock()
	files, err := filepath.Glob(filepath.Join(m.cfg.Dir, "*.wal"))
	m.mu.Unlock()
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, path := range files {
		var id int
		if _, err := fmt.Sscanf(filepath.Base(path), "%05d.wal", &id); err != nil {
			continue
		}
		if err := m.replayFile(uint32(id), path, fn); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) replayFile(id uint32, path string, fn func(info EntryInfo, payload []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

    reIter := NewRecordIterator(f, m.bufferSize)
	defer reIter.Close()

	var offset int64
	for reIter.Next() {
        length := reIter.Length()
        recType := reIter.Type()
        payload := append([]byte{}, reIter.Record()...)
		info := EntryInfo{
			SegmentID: id,
			Offset:    offset,
			Length:    length,
			Type:      recType,
		}
		if err := fn(info, payload); err != nil {
			return err
		}
		offset += int64(length) + 8
	}

	switch err := reIter.Err(); err {
	case nil, io.EOF:
		return nil
	case ErrPartialRecord:
		return nil
	case kv.ErrBadChecksum:
		return fmt.Errorf("wal: checksum mismatch segment=%d offset=%d", id, offset)
	default:
		return err
	}
}

// Close closes the manager and active segment.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.active == nil {
		return nil
	}
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			m.active.Close()
			return err
		}
	}
	if err := m.active.Sync(); err != nil {
		m.active.Close()
		return err
	}
	return m.active.Close()
}

// SwitchSegment switches the active WAL segment to the provided ID. When truncate is true,
// the segment is recreated; otherwise it is opened for append.
func (m *Manager) SwitchSegment(id uint32, truncate bool) error {
	return m.switchSegment(id, truncate)
}

// ReplaySegment replays entries from a single WAL segment.
func (m *Manager) ReplaySegment(id uint32, fn func(info EntryInfo, payload []byte) error) error {
	path := m.segmentPath(id)
	if _, err := os.Stat(path); err != nil {
		return err
	}
	return m.replayFile(id, path, fn)
}

// VerifyDir scans WAL segments in the provided directory, truncating any
// partially written records left behind by crashes and validating their
// checksums.
func VerifyDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("wal: directory required")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, path := range files {
		if err := verifySegment(path); err != nil {
			return err
		}
	}
	return nil
}

func verifySegment(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

    reIter := NewRecordIterator(f, defaultBufferSize)
	defer reIter.Close()

	var offset int64
	for reIter.Next() {
		offset += int64(reIter.Length()) + 8
	}

	switch err := reIter.Err(); err {
	case nil, io.EOF:
		return nil
	case ErrPartialRecord:
		return f.Truncate(offset)
	case kv.ErrBadChecksum:
		return fmt.Errorf("wal: checksum mismatch verifying %s at offset %d", filepath.Base(path), offset)
	default:
		return err
	}
}

// RemoveSegment deletes a WAL segment from disk.
func (m *Manager) RemoveSegment(id uint32) error {
	path := m.segmentPath(id)
	if err := os.Remove(path); err != nil {
		return err
	}
	atomic.AddUint64(&m.removedSegments, 1)
	m.mu.Lock()
	if metrics, ok := m.segmentTotals[id]; ok {
		m.recordTotals.Entries -= metrics.Entries
		m.recordTotals.RaftEntries -= metrics.RaftEntries
		m.recordTotals.RaftStates -= metrics.RaftStates
		m.recordTotals.RaftSnapshots -= metrics.RaftSnapshots
		m.recordTotals.Other -= metrics.Other
		delete(m.segmentTotals, id)
	}
	m.mu.Unlock()
	return nil
}

// ActiveSize returns the size in bytes of the current active WAL segment.
func (m *Manager) ActiveSize() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeSize
}

// Metrics returns a snapshot of WAL manager statistics.
func (m *Manager) Metrics() *Metrics {
	if m == nil {
		return nil
	}
	files, err := m.ListSegments()
	count := 0
	if err == nil {
		count = len(files)
	}
	segmentRaft := 0
	m.mu.Lock()
	for _, metrics := range m.segmentTotals {
		if metrics.RaftRecords() > 0 {
			segmentRaft++
		}
	}
	recordTotals := m.recordTotals
	m.mu.Unlock()
	return &Metrics{
		ActiveSegment:           m.ActiveSegment(),
		ActiveSize:              m.ActiveSize(),
		SegmentCount:            count,
		RemovedSegments:         atomic.LoadUint64(&m.removedSegments),
		RecordCounts:            recordTotals,
		SegmentsWithRaftRecords: segmentRaft,
	}
}

// RecordMetrics returns the current record-type counters.
func (m *Manager) RecordMetrics() RecordMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recordTotals
}

// SegmentRecordMetrics returns accumulated metrics for a specific segment.
func (m *Manager) SegmentRecordMetrics(id uint32) RecordMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.segmentTotals[id]
}

// SegmentMetrics returns a copy of per-segment metrics map.
func (m *Manager) SegmentMetrics() map[uint32]RecordMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	copyMap := make(map[uint32]RecordMetrics, len(m.segmentTotals))
	for id, metrics := range m.segmentTotals {
		copyMap[id] = metrics
	}
	return copyMap
}
