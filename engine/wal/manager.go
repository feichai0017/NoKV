// Package wal implements the write-ahead log manager and replay logic.
package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultSegmentSize = 64 << 20 // 64 MiB
	minSegmentSize     = 64 << 10 // 64 KiB
	defaultBufferSize  = 4 << 20  // 4 MiB
	// DefaultBufferSize is the default in-memory WAL writer buffer size.
	DefaultBufferSize = defaultBufferSize
)

// Config controls WAL manager behaviour.
type Config struct {
	Dir         string
	SegmentSize int64
	FileMode    os.FileMode
	BufferSize  int
	MaxSegments int
	FS          vfs.FS
}

// DurabilityPolicy describes when an append may be acknowledged.
type DurabilityPolicy uint8

const (
	// DurabilityBuffered acknowledges after records are copied into the WAL writer buffer.
	DurabilityBuffered DurabilityPolicy = iota + 1
	// DurabilityFlushed acknowledges after flushing records into the OS page cache.
	DurabilityFlushed
	// DurabilityFsync acknowledges after fsyncing the active segment.
	DurabilityFsync
	// DurabilityFsyncBatched acknowledges after a group-commit fsync.
	DurabilityFsyncBatched
)

const (
	defaultFsyncBatchWindow = 50 * time.Microsecond
	defaultFsyncBatchSize   = 128
)

// resolveOpenConfig resolves constructor defaults once at the WAL open boundary.
func (cfg Config) resolveOpenConfig() (Config, error) {
	if cfg.Dir == "" {
		return Config{}, fmt.Errorf("wal: directory required")
	}
	cfg.FS = vfs.Ensure(cfg.FS)
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = defaultSegmentSize
	}
	if cfg.SegmentSize < minSegmentSize {
		cfg.SegmentSize = minSegmentSize
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	return cfg, nil
}

func (p DurabilityPolicy) valid() bool {
	switch p {
	case DurabilityBuffered, DurabilityFlushed, DurabilityFsync, DurabilityFsyncBatched:
		return true
	default:
		return false
	}
}

// EntryInfo describes an entry written to WAL.
type EntryInfo struct {
	SegmentID uint32
	Offset    int64
	Length    uint32
	Type      RecordType
	GroupID   uint64
}

// Metrics captures runtime information about WAL manager state.
type Metrics = metrics.WALMetrics

// RecordMetrics summarises counts per record type.
type RecordMetrics = metrics.WALRecordMetrics

func addRecordMetric(m *RecordMetrics, recType RecordType) {
	if m == nil {
		return
	}
	switch recType {
	case RecordTypeEntry, RecordTypeEntryBatch:
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

// Manager provides append-only WAL segments with replay support.
type Manager struct {
	cfg Config

	mu              sync.Mutex
	active          vfs.File
	activeID        uint32
	activeSize      int64
	closed          bool
	segmentSize     int64
	removedSegments atomic.Uint64
	bufferSize      int
	segmentCount    int
	writer          *bufio.Writer
	recordTotals    RecordMetrics
	segmentTotals   map[uint32]RecordMetrics
	retention       map[string]RetentionFunc
	fsyncCond       *sync.Cond
	fsyncClosed     bool
	fsyncBatchSeq   uint64
	fsyncDurableSeq uint64
	fsyncErr        error
}

// Open creates or resumes a WAL manager.
func Open(cfg Config) (*Manager, error) {
	var err error
	cfg, err = cfg.resolveOpenConfig()
	if err != nil {
		return nil, err
	}
	if err := cfg.FS.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}

	m := &Manager{
		cfg:           cfg,
		segmentSize:   cfg.SegmentSize,
		bufferSize:    cfg.BufferSize,
		segmentTotals: make(map[uint32]RecordMetrics, 16),
	}
	m.fsyncCond = sync.NewCond(&m.mu)
	if err := m.openLatestSegment(); err != nil {
		return nil, err
	}
	if err := m.rebuildRecordCounts(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) openLatestSegment() error {
	files, err := m.cfg.FS.Glob(filepath.Join(m.cfg.Dir, "*.wal"))
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
	m.segmentCount = len(ids)
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
		addRecordMetric(&metrics, info.Type)
		segmentTotals[info.SegmentID] = metrics
		addRecordMetric(&totals, info.Type)
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

// switchSegmentLocked replaces the active segment; caller must hold m.mu.
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
		if err := m.rebuildActiveCatalogLocked(); err != nil {
			return err
		}
		if err := m.active.Close(); err != nil {
			return err
		}
	}

	path := m.segmentPath(id)
	_, statErr := m.cfg.FS.Stat(path)
	segmentExists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	flag := os.O_CREATE | os.O_RDWR
	if truncate {
		flag |= os.O_TRUNC
	}
	f, err := m.cfg.FS.OpenFileHandle(path, flag, m.cfg.FileMode)
	if err != nil {
		return err
	}

	var size int64
	if !truncate {
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		size = info.Size()
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			_ = f.Close()
			return err
		}
	}
	m.active = f
	m.activeID = id
	if !segmentExists {
		m.segmentCount++
	}
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

// AppendEntry appends a single encoded kv entry record to WAL.
func (m *Manager) AppendEntry(durability DurabilityPolicy, entry *kv.Entry) (EntryInfo, error) {
	if entry == nil || len(entry.Key) == 0 {
		return EntryInfo{}, fmt.Errorf("wal: invalid entry")
	}
	// Reuse the shared entryBufPool. AppendRecords synchronously copies the
	// payload into the WAL bufio writer, so the buffer is safe to release
	// once that call returns.
	buf := acquireEntryBuf()
	payload, err := kv.EncodeEntry(buf, entry)
	if err != nil {
		releaseEntryBuf(buf)
		return EntryInfo{}, err
	}
	infos, err := m.AppendRecords(durability, Record{
		Type:    RecordTypeEntry,
		Payload: payload,
	})
	releaseEntryBuf(buf)
	if err != nil {
		return EntryInfo{}, err
	}
	if len(infos) != 1 {
		return EntryInfo{}, fmt.Errorf("wal: expected one info for entry, got %d", len(infos))
	}
	return infos[0], nil
}

// AppendEntryBatch appends a batch of kv entries as one WAL record.
func (m *Manager) AppendEntryBatch(durability DurabilityPolicy, entries []*kv.Entry) (EntryInfo, error) {
	payload, err := EncodeEntryBatch(entries)
	if err != nil {
		return EntryInfo{}, err
	}
	infos, err := m.AppendRecords(durability, Record{
		Type:    RecordTypeEntryBatch,
		Payload: payload,
	})
	if err != nil {
		return EntryInfo{}, err
	}
	if len(infos) != 1 {
		return EntryInfo{}, fmt.Errorf("wal: expected one info for entry batch, got %d", len(infos))
	}
	return infos[0], nil
}

// AppendRecords appends typed records with explicit durability semantics.
func (m *Manager) AppendRecords(durability DurabilityPolicy, records ...Record) ([]EntryInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("wal: manager closed")
	}
	if !durability.valid() {
		return nil, fmt.Errorf("wal: invalid durability policy %d", durability)
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
		addRecordMetric(&segMetrics, rec.Type)
		m.segmentTotals[m.activeID] = segMetrics
		addRecordMetric(&m.recordTotals, rec.Type)
	}
	if err := m.applyDurabilityLocked(durability); err != nil {
		return nil, err
	}
	return results, nil
}

func (m *Manager) applyDurabilityLocked(policy DurabilityPolicy) error {
	switch policy {
	case DurabilityBuffered:
		return nil
	case DurabilityFlushed:
		if m.writer != nil {
			return m.writer.Flush()
		}
		return nil
	case DurabilityFsync:
		if err := m.writer.Flush(); err != nil {
			return err
		}
		if err := m.active.Sync(); err != nil {
			return err
		}
		return nil
	case DurabilityFsyncBatched:
		return m.enqueueFsyncBatchLocked()
	default:
		return fmt.Errorf("wal: invalid durability policy %d", policy)
	}
}

func (m *Manager) enqueueFsyncBatchLocked() error {
	if m.fsyncClosed {
		return fmt.Errorf("wal: manager closed")
	}
	seq := m.fsyncBatchSeq + 1
	m.fsyncBatchSeq = seq
	if m.fsyncBatchSeq-m.fsyncDurableSeq == 1 {
		go m.runFsyncBatch()
	}
	for m.fsyncDurableSeq < seq && !m.fsyncClosed {
		m.fsyncCond.Wait()
	}
	if m.fsyncDurableSeq >= seq {
		return m.fsyncErr
	}
	return fmt.Errorf("wal: manager closed")
}

func (m *Manager) runFsyncBatch() {
	timer := time.NewTimer(defaultFsyncBatchWindow)
	<-timer.C

	for {
		m.mu.Lock()
		target := m.fsyncBatchSeq
		if pending := target - m.fsyncDurableSeq; pending < defaultFsyncBatchSize && !m.fsyncClosed {
			m.mu.Unlock()
			time.Sleep(defaultFsyncBatchWindow)
			m.mu.Lock()
			target = m.fsyncBatchSeq
		}
		if target == m.fsyncDurableSeq || m.fsyncClosed {
			m.mu.Unlock()
			return
		}
		err := m.flushAndSyncLocked()
		m.fsyncErr = err
		m.fsyncDurableSeq = target
		m.fsyncCond.Broadcast()
		more := m.fsyncBatchSeq > m.fsyncDurableSeq && !m.fsyncClosed
		m.mu.Unlock()
		if !more {
			return
		}
	}
}

func (m *Manager) flushAndSyncLocked() error {
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			return err
		}
	}
	if m.active != nil {
		if err := m.active.Sync(); err != nil {
			return err
		}
	}
	return nil
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

// rotateLocked advances to a new segment; caller must hold m.mu.
func (m *Manager) rotateLocked() error {
	if m.cfg.MaxSegments > 0 && m.segmentCount >= m.cfg.MaxSegments {
		return ErrWALBackpressure
	}
	nextID := m.activeID + 1
	return m.switchSegmentLocked(nextID, true)
}

// Sync fsyncs the active segment.
func (m *Manager) Sync() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("wal: manager closed")
	}
	active := m.active
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Unlock()
	if active == nil {
		return nil
	}
	err := active.Sync()
	if err == nil || errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
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
	files, err := m.cfg.FS.Glob(filepath.Join(m.cfg.Dir, "*.wal"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// Replay traverses all WAL segments and feeds entries to callback.
func (m *Manager) Replay(fn func(info EntryInfo, payload []byte) error) error {
	return m.ReplayFiltered(nil, fn)
}

// ReplayFiltered traverses WAL segments and only decodes records accepted by
// filter. Nil filter accepts all records.
func (m *Manager) ReplayFiltered(filter func(EntryInfo) bool, fn func(info EntryInfo, payload []byte) error) error {
	m.mu.Lock()
	files, err := m.cfg.FS.Glob(filepath.Join(m.cfg.Dir, "*.wal"))
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
		if err := m.replayFile(uint32(id), path, filter, fn); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) replayFile(id uint32, path string, filter func(EntryInfo) bool, fn func(info EntryInfo, payload []byte) error) error {
	entries, err := m.loadSegmentCatalog(id)
	if err == nil {
		err = m.replayIndexedFile(path, entries, filter, fn)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errStaleCatalog) {
			return err
		}
	}
	entries, err = m.rebuildSegmentCatalog(id, path)
	if err != nil {
		return err
	}
	return m.replayIndexedFile(path, entries, filter, fn)
}

// Close closes the manager and active segment.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.fsyncClosed = true
	if m.fsyncCond != nil {
		m.fsyncCond.Broadcast()
	}
	if m.active == nil {
		m.closed = true
		return nil
	}
	if m.writer != nil {
		if err := m.writer.Flush(); err != nil {
			closeErr := m.active.Close()
			m.active = nil
			m.writer = nil
			m.closed = true
			return errors.Join(err, closeErr)
		}
	}
	if err := m.active.Sync(); err != nil {
		closeErr := m.active.Close()
		m.active = nil
		m.writer = nil
		m.closed = true
		return errors.Join(err, closeErr)
	}
	if err := m.rebuildActiveCatalogLocked(); err != nil {
		closeErr := m.active.Close()
		m.active = nil
		m.writer = nil
		m.closed = true
		return errors.Join(err, closeErr)
	}
	if err := m.active.Close(); err != nil {
		// Keep state intact so callers can retry Close() on transient close failures.
		return err
	}
	m.active = nil
	m.writer = nil
	m.closed = true
	return nil
}

// SwitchSegment switches the active WAL segment to the provided ID. When truncate is true,
// the segment is recreated; otherwise it is opened for append.
func (m *Manager) SwitchSegment(id uint32, truncate bool) error {
	return m.switchSegment(id, truncate)
}

// ReplaySegment replays entries from a single WAL segment.
func (m *Manager) ReplaySegment(id uint32, fn func(info EntryInfo, payload []byte) error) error {
	path := m.segmentPath(id)
	if _, err := m.cfg.FS.Stat(path); err != nil {
		return err
	}
	return m.replayFile(id, path, nil, fn)
}

// VerifyDir scans WAL segments in the provided directory, truncating any
// partially written records left behind by crashes and validating their
// checksums. Nil fs defaults to OSFS.
func VerifyDir(dir string, fs vfs.FS) error {
	if dir == "" {
		return fmt.Errorf("wal: directory required")
	}
	fs = vfs.Ensure(fs)
	files, err := fs.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, path := range files {
		if err := verifySegment(fs, path); err != nil {
			return err
		}
	}
	return nil
}

// CheckDir scans WAL segments in the provided directory without mutating them.
// Nil fs defaults to OSFS.
func CheckDir(dir string, fs vfs.FS) error {
	if dir == "" {
		return fmt.Errorf("wal: directory required")
	}
	fs = vfs.Ensure(fs)
	files, err := fs.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, path := range files {
		if err := checkSegment(fs, path); err != nil {
			return err
		}
	}
	return nil
}

func verifySegment(fs vfs.FS, path string) error {
	f, err := fs.OpenFileHandle(path, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	reIter := NewRecordIterator(f, defaultBufferSize)
	defer func() { _ = reIter.Close() }()

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

func checkSegment(fs vfs.FS, path string) error {
	f, err := fs.OpenFileHandle(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	reIter := NewRecordIterator(f, defaultBufferSize)
	defer func() { _ = reIter.Close() }()

	var offset int64
	for reIter.Next() {
		offset += int64(reIter.Length()) + 8
	}

	switch err := reIter.Err(); err {
	case nil, io.EOF:
		return nil
	case ErrPartialRecord:
		return fmt.Errorf("wal: partial record verifying %s at offset %d", filepath.Base(path), offset)
	case kv.ErrBadChecksum:
		return fmt.Errorf("wal: checksum mismatch verifying %s at offset %d", filepath.Base(path), offset)
	default:
		return err
	}
}

func (m *Manager) rebuildActiveCatalogLocked() error {
	if m.activeID == 0 || m.active == nil {
		return nil
	}
	if _, err := m.active.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := m.scanAndWriteSegmentCatalog(m.activeID, m.active)
	if _, seekErr := m.active.Seek(0, io.SeekEnd); err == nil {
		err = seekErr
	}
	return err
}

// RemoveSegment deletes a WAL segment from disk.
func (m *Manager) RemoveSegment(id uint32) error {
	if !m.CanRemoveSegment(id) {
		return ErrSegmentRetained
	}
	path := m.segmentPath(id)
	if err := m.cfg.FS.Remove(path); err != nil {
		return err
	}
	if err := m.cfg.FS.Remove(m.catalogPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	m.removedSegments.Add(1)
	m.mu.Lock()
	if m.segmentCount > 0 {
		m.segmentCount--
	}
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
	segmentRaft := 0
	m.mu.Lock()
	count := m.segmentCount
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
		RemovedSegments:         m.removedSegments.Load(),
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
	maps.Copy(copyMap, m.segmentTotals)
	return copyMap
}
