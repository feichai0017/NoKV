// Package manifest persists SST, WAL checkpoint, vlog, and raft metadata.
package manifest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const currentFileName = "CURRENT"

// Manager controls manifest file operations.
type Manager struct {
	mu         sync.Mutex
	dir        string
	current    string
	manifest   *os.File
	version    Version
	syncWrites bool
}

// Open loads manifest from CURRENT file or creates a new one.
func Open(dir string) (*Manager, error) {
	if dir == "" {
		return nil, fmt.Errorf("manifest dir required")
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	mgr := &Manager{dir: dir, syncWrites: true}
	if err := mgr.loadCurrent(); err != nil {
		if err := mgr.createNew(); err != nil {
			return nil, err
		}
		return mgr, nil
	}
	if err := mgr.replay(); err != nil {
		mgr.Close()
		return nil, err
	}
	return mgr, nil
}

func (m *Manager) loadCurrent() error {
	path := filepath.Join(m.dir, currentFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	m.current = string(data)
	manifestPath := filepath.Join(m.dir, m.current)
	m.manifest, err = os.OpenFile(manifestPath, os.O_RDWR, 0o666)
	return err
}

func (m *Manager) createNew() error {
	fileName := "MANIFEST-000001"
	path := filepath.Join(m.dir, fileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	m.manifest = f
	m.current = fileName
	if err := m.writeCurrent(); err != nil {
		return err
	}
	m.version = Version{
		Levels:       make(map[int][]FileMeta),
		ValueLogs:    make(map[uint32]ValueLogMeta),
		RaftPointers: make(map[uint64]RaftLogPointer),
		Regions:      make(map[uint64]RegionMeta),
	}
	return nil
}

func (m *Manager) writeCurrent() error {
	tmp := filepath.Join(m.dir, "CURRENT.tmp")
	if err := os.WriteFile(tmp, []byte(m.current), 0o666); err != nil {
		return err
	}
	dst := filepath.Join(m.dir, currentFileName)
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	return nil
}

func (m *Manager) replay() error {
	m.version = Version{
		Levels:       make(map[int][]FileMeta),
		ValueLogs:    make(map[uint32]ValueLogMeta),
		RaftPointers: make(map[uint64]RaftLogPointer),
		Regions:      make(map[uint64]RegionMeta),
	}
	reader := bufio.NewReader(m.manifest)
	for {
		edit, err := readEdit(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		m.apply(edit)
	}
	return nil
}

func (m *Manager) apply(edit Edit) {
	switch edit.Type {
	case EditAddFile:
		meta := *edit.File
		m.version.Levels[meta.Level] = append(m.version.Levels[meta.Level], meta)
	case EditDeleteFile:
		meta := edit.File
		files := m.version.Levels[meta.Level]
		for i, fm := range files {
			if fm.FileID == meta.FileID {
				m.version.Levels[meta.Level] = append(files[:i], files[i+1:]...)
				break
			}
		}
	case EditLogPointer:
		m.version.LogSegment = edit.LogSeg
		m.version.LogOffset = edit.LogOffset
	case EditValueLogHead:
		if edit.ValueLog != nil {
			meta := *edit.ValueLog
			meta.Valid = true
			m.version.ValueLogs[meta.FileID] = meta
			m.version.ValueLogHead = meta
		}
	case EditDeleteValueLog:
		if edit.ValueLog != nil {
			meta := m.version.ValueLogs[edit.ValueLog.FileID]
			meta.FileID = edit.ValueLog.FileID
			meta.Offset = 0
			meta.Valid = false
			m.version.ValueLogs[edit.ValueLog.FileID] = meta
			if m.version.ValueLogHead.FileID == edit.ValueLog.FileID {
				m.version.ValueLogHead = ValueLogMeta{}
			}
		}
	case EditUpdateValueLog:
		if edit.ValueLog != nil {
			meta := *edit.ValueLog
			m.version.ValueLogs[meta.FileID] = meta
			if m.version.ValueLogHead.FileID == meta.FileID {
				if meta.Valid {
					m.version.ValueLogHead = meta
				} else {
					m.version.ValueLogHead = ValueLogMeta{}
				}
			}
		}
	case EditRaftPointer:
		if edit.Raft != nil {
			ptr := *edit.Raft
			m.version.RaftPointers[ptr.GroupID] = ptr
		}
	case EditRegion:
		if edit.Region != nil {
			if edit.Region.Delete {
				delete(m.version.Regions, edit.Region.Meta.ID)
			} else {
				meta := edit.Region.Meta
				meta.StartKey = append([]byte(nil), meta.StartKey...)
				meta.EndKey = append([]byte(nil), meta.EndKey...)
				meta.Peers = append([]PeerMeta(nil), meta.Peers...)
				m.version.Regions[meta.ID] = meta
			}
		}
	}
}

// LogEdit appends an edit to manifest and updates current version.
func (m *Manager) LogEdit(edit Edit) error {
	return m.LogEdits(edit)
}

// SetSync configures whether manifest edits are synchronously flushed to disk.
func (m *Manager) SetSync(sync bool) {
	m.mu.Lock()
	m.syncWrites = sync
	m.mu.Unlock()
}

// LogEdits appends a batch of edits to manifest and updates current version.
func (m *Manager) LogEdits(edits ...Edit) error {
	if len(edits) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logEditsLocked(edits)
}

func (m *Manager) logEditsLocked(edits []Edit) error {
	var buf bytes.Buffer
	syncNeeded := false
	for _, edit := range edits {
		if err := writeEdit(&buf, edit); err != nil {
			return err
		}
		if requiresSync(edit) {
			syncNeeded = true
		}
	}
	if _, err := m.manifest.Write(buf.Bytes()); err != nil {
		return err
	}
	if syncNeeded && m.syncWrites {
		if err := m.manifest.Sync(); err != nil {
			return err
		}
	}
	for _, edit := range edits {
		m.apply(edit)
	}
	return nil
}

func requiresSync(edit Edit) bool {
	switch edit.Type {
	case EditAddFile, EditDeleteFile, EditLogPointer, EditValueLogHead, EditDeleteValueLog, EditUpdateValueLog:
		return true
	default:
		return false
	}
}

// Current returns a snapshot of the current version.
func (m *Manager) Current() Version {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := Version{
		LogSegment:   m.version.LogSegment,
		LogOffset:    m.version.LogOffset,
		Levels:       make(map[int][]FileMeta),
		ValueLogs:    make(map[uint32]ValueLogMeta),
		ValueLogHead: m.version.ValueLogHead,
		RaftPointers: make(map[uint64]RaftLogPointer, len(m.version.RaftPointers)),
		Regions:      make(map[uint64]RegionMeta, len(m.version.Regions)),
	}
	for level, files := range m.version.Levels {
		cp.Levels[level] = append([]FileMeta(nil), files[:]...)
	}
	maps.Copy(cp.ValueLogs, m.version.ValueLogs)
	maps.Copy(cp.RaftPointers, m.version.RaftPointers)
	for id, meta := range m.version.Regions {
		metaCopy := meta
		metaCopy.StartKey = append([]byte(nil), meta.StartKey...)
		metaCopy.EndKey = append([]byte(nil), meta.EndKey...)
		metaCopy.Peers = append([]PeerMeta(nil), meta.Peers...)
		cp.Regions[id] = metaCopy
	}
	return cp
}

// Dir returns the manifest directory path. The result is empty when the
// manager is nil, allowing callers to short-circuit optional metrics
// collection.
func (m *Manager) Dir() string {
	if m == nil {
		return ""
	}
	return m.dir
}

// Close closes manifest file.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manifest != nil {
		return m.manifest.Close()
	}
	return nil
}

// LogValueLogHead records the active value log head pointer.
func (m *Manager) LogValueLogHead(fid uint32, offset uint64) error {
	meta := &ValueLogMeta{
		FileID: fid,
		Offset: offset,
		Valid:  true,
	}
	return m.LogEdit(Edit{Type: EditValueLogHead, ValueLog: meta})
}

// LogValueLogDelete records value log segment deletion.
func (m *Manager) LogValueLogDelete(fid uint32) error {
	meta := &ValueLogMeta{
		FileID: fid,
		Valid:  false,
	}
	return m.LogEdit(Edit{Type: EditDeleteValueLog, ValueLog: meta})
}

// LogValueLogUpdate records an updated value log metadata entry.
func (m *Manager) LogValueLogUpdate(meta ValueLogMeta) error {
	cp := meta
	return m.LogEdit(Edit{Type: EditUpdateValueLog, ValueLog: &cp})
}

// ValueLogHead returns the latest persisted value log head.
func (m *Manager) ValueLogHead() ValueLogMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version.ValueLogHead
}

// ValueLogStatus returns a copy of all tracked value log segment metadata.
func (m *Manager) ValueLogStatus() map[uint32]ValueLogMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uint32]ValueLogMeta, len(m.version.ValueLogs))
	maps.Copy(out, m.version.ValueLogs)
	return out
}

// LogRaftPointer persists the WAL checkpoint for a raft group.
func (m *Manager) LogRaftPointer(ptr RaftLogPointer) error {
	cp := ptr
	return m.LogEdit(Edit{Type: EditRaftPointer, Raft: &cp})
}

// LogRegionUpdate records region metadata.
func (m *Manager) LogRegionUpdate(meta RegionMeta) error {
	edit := RegionEdit{Meta: meta}
	return m.LogEdit(Edit{Type: EditRegion, Region: &edit})
}

// LogRegionDelete marks the region as removed.
func (m *Manager) LogRegionDelete(regionID uint64) error {
	edit := RegionEdit{Meta: RegionMeta{ID: regionID}, Delete: true}
	return m.LogEdit(Edit{Type: EditRegion, Region: &edit})
}

// RegionSnapshot returns a copy of region metadata map.
func (m *Manager) RegionSnapshot() map[uint64]RegionMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uint64]RegionMeta, len(m.version.Regions))
	for id, meta := range m.version.Regions {
		metaCopy := meta
		metaCopy.StartKey = append([]byte(nil), meta.StartKey...)
		metaCopy.EndKey = append([]byte(nil), meta.EndKey...)
		metaCopy.Peers = append([]PeerMeta(nil), meta.Peers...)
		out[id] = metaCopy
	}
	return out
}

// LogRaftTruncate records the log truncation point (index/term) for a raft
// group without altering other pointer metadata. When segment is non-zero it
// updates the segment containing the truncated index; offset, when provided,
// marks the byte position within that segment that must be retained. If the
// persisted state already matches the provided values the call is a no-op.
func (m *Manager) LogRaftTruncate(groupID, index, term uint64, segment uint32, offset uint64) error {
	if groupID == 0 {
		return fmt.Errorf("manifest: raft truncate requires group id")
	}
	var ptr RaftLogPointer
	m.mu.Lock()
	existing, ok := m.version.RaftPointers[groupID]
	if ok {
		ptr = existing
	}
	m.mu.Unlock()

	if !ok {
		if index == 0 && term == 0 {
			return nil
		}
		ptr.GroupID = groupID
	}
	if ptr.TruncatedIndex == index && ptr.TruncatedTerm == term {
		if segment == 0 || ptr.SegmentIndex == uint64(segment) {
			if offset == 0 || ptr.TruncatedOffset == offset {
				return nil
			}
		}
		if offset == 0 {
			return nil
		}
	}
	ptr.GroupID = groupID
	ptr.TruncatedIndex = index
	ptr.TruncatedTerm = term
	if segment == 0 && ptr.SegmentIndex != 0 {
		segment = uint32(ptr.SegmentIndex)
	}
	ptr.SegmentIndex = uint64(segment)
	if offset == 0 && ptr.TruncatedOffset != 0 {
		offset = ptr.TruncatedOffset
	}
	ptr.TruncatedOffset = offset
	return m.LogRaftPointer(ptr)
}

// RaftPointer returns the last persisted raft WAL pointer for the given group.
func (m *Manager) RaftPointer(groupID uint64) (RaftLogPointer, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptr, ok := m.version.RaftPointers[groupID]
	return ptr, ok
}

// RaftPointerSnapshot returns a copy of all raft WAL checkpoints.
func (m *Manager) RaftPointerSnapshot() map[uint64]RaftLogPointer {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uint64]RaftLogPointer, len(m.version.RaftPointers))
	maps.Copy(out, m.version.RaftPointers)
	return out
}

// Internal encoding helpers
// Verify ensures manifest and CURRENT pointer are well-formed. It truncates any
// partially written edits left at the end of the manifest file.
func Verify(dir string) error {
	if dir == "" {
		return fmt.Errorf("manifest: directory required")
	}
	tmp := filepath.Join(dir, "CURRENT.tmp")
	if _, err := os.Stat(tmp); err == nil {
		_ = os.Remove(tmp)
	}

	currentPath := filepath.Join(dir, currentFileName)
	data, err := os.ReadFile(currentPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return err
		}
		return fmt.Errorf("manifest: read CURRENT: %w", err)
	}
	name := strings.TrimSpace(string(data))
	if name == "" {
		return fmt.Errorf("manifest: CURRENT empty")
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("manifest: open %s: %w", name, err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var offset int64
	for {
		pos := offset
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF {
				return f.Truncate(pos)
			}
			return fmt.Errorf("manifest: read length at %d: %w", pos, err)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return f.Truncate(pos)
			}
			return fmt.Errorf("manifest: read payload at %d: %w", pos, err)
		}
		if _, err := decodeEdit(payload); err != nil {
			return fmt.Errorf("manifest: invalid edit at %d: %w", pos, err)
		}
		offset = pos + int64(length) + 4
	}
}
