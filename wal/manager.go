package wal

import (
	"bufio"
	"encoding/binary"
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
	defaultSegmentSize = 64 << 20 // 64 MiB
	minSegmentSize     = 64 << 10 // 64 KiB
)

// Config controls WAL manager behaviour.
type Config struct {
	Dir         string
	SegmentSize int64
	FileMode    os.FileMode
	SyncOnWrite bool
}

// EntryInfo describes an entry written to WAL.
type EntryInfo struct {
	SegmentID uint32
	Offset    int64
	Length    uint32
}

// Metrics captures runtime information about WAL manager state.
type Metrics struct {
	ActiveSegment   uint32
	SegmentCount    int
	RemovedSegments uint64
}

// Manager provides append-only WAL segments with replay support.
type Manager struct {
	cfg 			Config

	mu              sync.Mutex
	active          *os.File
	activeID        uint32
	activeSize      int64
	closed          bool
	crcTable        *crc32.Table
	segmentSize     int64
	removedSegments uint64
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

	m := &Manager{
		cfg:         cfg,
		crcTable:    utils.CastagnoliCrcTable,
		segmentSize: segSize,
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("wal: manager closed")
	}
	results := make([]EntryInfo, len(payloads))
	for i, p := range payloads {
		if err := m.ensureCapacity(int64(len(p)) + 8); err != nil {
			return nil, err
		}
		offset := m.activeSize
		length := uint32(len(p))
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], length)
		if _, err := m.active.Write(hdr[:]); err != nil {
			return nil, err
		}
		if _, err := m.active.Write(p); err != nil {
			return nil, err
		}
		var crcBuf [4]byte
		sum := crc32.Checksum(p, m.crcTable)
		binary.BigEndian.PutUint32(crcBuf[:], sum)
		if _, err := m.active.Write(crcBuf[:]); err != nil {
			return nil, err
		}
		m.activeSize += int64(length) + 8
		results[i] = EntryInfo{
			SegmentID: m.activeID,
			Offset:    offset,
			Length:    length,
		}
		if m.cfg.SyncOnWrite {
			if err := m.active.Sync(); err != nil {
				return nil, err
			}
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
		actual := crc32.Checksum(payload, m.crcTable)
		if expected != actual {
			return fmt.Errorf("wal: checksum mismatch segment=%d offset=%d", id, offset)
		}
		info := EntryInfo{
			SegmentID: id,
			Offset:    offset,
			Length:    length,
		}
		if err := fn(info, payload); err != nil {
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
