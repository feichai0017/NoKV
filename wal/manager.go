package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

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
	ActiveSegment   uint32
	SegmentCount    int
	RemovedSegments uint64
}

// Manager provides append-only WAL segments with replay support.
type Manager struct {
	cfg Config

	mu              sync.Mutex
	active          *os.File
	activeID        uint32
	activeSize      int64
	closed          bool
	crcTable        *crc32.Table
	segmentSize     int64
	removedSegments uint64
	bufferSize      int
	writer          *bufio.Writer
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
type Record struct {
	Type    RecordType
	Payload []byte
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
		cfg:         cfg,
		crcTable:    utils.CastagnoliCrcTable,
		segmentSize: segSize,
		bufferSize:  bufSize,
	}
	if err := m.openLatestSegment(); err != nil {
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
		encoded := make([]byte, len(payload)+1)
		encoded[0] = byte(rec.Type)
		copy(encoded[1:], payload)
		total := len(encoded)
		if err := m.ensureCapacity(int64(total) + 8); err != nil {
			return nil, err
		}
		offset := m.activeSize
		length := uint32(total)
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], length)
		if _, err := m.writer.Write(hdr[:]); err != nil {
			return nil, err
		}
		if _, err := m.writer.Write(encoded); err != nil {
			return nil, err
		}
		var crcBuf [4]byte
		hasher := crc32.New(m.crcTable)
		if _, err := hasher.Write(encoded); err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(crcBuf[:], hasher.Sum32())
		if _, err := m.writer.Write(crcBuf[:]); err != nil {
			return nil, err
		}
		m.activeSize += int64(length) + 8
		results[i] = EntryInfo{
			SegmentID: m.activeID,
			Offset:    offset,
			Length:    length,
			Type:      rec.Type,
		}
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
	reader := bufio.NewReader(f)
	var offset int64
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		expected := binary.BigEndian.Uint32(crcBuf[:])
		if length == 0 {
			return fmt.Errorf("wal: empty record segment=%d offset=%d", id, offset)
		}
		hasher := crc32.New(m.crcTable)
		if _, err := hasher.Write([]byte{payload[0]}); err != nil {
			return err
		}
		if _, err := hasher.Write(payload[1:]); err != nil {
			return err
		}
		if expected != hasher.Sum32() {
			return fmt.Errorf("wal: checksum mismatch segment=%d offset=%d", id, offset)
		}
		recType := RecordType(payload[0])
		data := payload[1:]
		info := EntryInfo{
			SegmentID: id,
			Offset:    offset,
			Length:    length,
			Type:      recType,
		}
		if err := fn(info, data); err != nil {
			return err
		}
		offset += int64(length) + 8
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
	reader := bufio.NewReader(f)
	var offset int64
	for {
		start := offset
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				return f.Truncate(start)
			}
			return fmt.Errorf("wal verify length %s: %w", filepath.Base(path), err)
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return f.Truncate(start)
			}
			return fmt.Errorf("wal verify payload %s: %w", filepath.Base(path), err)
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return f.Truncate(start)
			}
			return fmt.Errorf("wal verify checksum %s: %w", filepath.Base(path), err)
		}
		expected := binary.BigEndian.Uint32(crcBuf[:])
		if length == 0 {
			return fmt.Errorf("wal: empty record verifying %s at offset %d", filepath.Base(path), start)
		}
		hasher := crc32.New(utils.CastagnoliCrcTable)
		if _, err := hasher.Write([]byte{payload[0]}); err != nil {
			return fmt.Errorf("wal: checksum write type %s: %w", filepath.Base(path), err)
		}
		if _, err := hasher.Write(payload[1:]); err != nil {
			return fmt.Errorf("wal: checksum write payload %s: %w", filepath.Base(path), err)
		}
		if expected != hasher.Sum32() {
			return fmt.Errorf("wal: checksum mismatch verifying %s at offset %d", filepath.Base(path), start)
		}
		offset = start + int64(length) + 8
	}
}

// RemoveSegment deletes a WAL segment from disk.
func (m *Manager) RemoveSegment(id uint32) error {
	path := m.segmentPath(id)
	if err := os.Remove(path); err != nil {
		return err
	}
	atomic.AddUint64(&m.removedSegments, 1)
	return nil
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
	return &Metrics{
		ActiveSegment:   m.ActiveSegment(),
		SegmentCount:    count,
		RemovedSegments: atomic.LoadUint64(&m.removedSegments),
	}
}
