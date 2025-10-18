package manifest

import (
	"bufio"
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

const (
	currentFileName = "CURRENT"
	editMagic       = "NoKV"
)

// FileMeta describes an SST file.
type FileMeta struct {
	Level     int
	FileID    uint64
	Size      uint64
	Smallest  []byte
	Largest   []byte
	CreatedAt uint64
}

// ValueLogMeta describes a value log segment.
type ValueLogMeta struct {
	FileID uint32
	Offset uint64
	Valid  bool
}

// PeerMeta describes a peer replica for a region.
type PeerMeta struct {
	StoreID uint64
	PeerID  uint64
}

// RegionState enumerates region lifecycle states.
type RegionState uint8

const (
	RegionStateNew RegionState = iota
	RegionStateRunning
	RegionStateRemoving
	RegionStateTombstone
)

// RegionEpoch tracks metadata versioning.
type RegionEpoch struct {
	Version     uint64
	ConfVersion uint64
}

// RegionMeta captures region key range and peer membership.
type RegionMeta struct {
	ID       uint64
	StartKey []byte
	EndKey   []byte
	Epoch    RegionEpoch
	Peers    []PeerMeta
	State    RegionState
}

// RegionEdit represents an update or deletion.
type RegionEdit struct {
	Meta   RegionMeta
	Delete bool
}

// RaftLogPointer tracks WAL progress for a raft group.
type RaftLogPointer struct {
	GroupID         uint64
	Segment         uint32
	Offset          uint64
	AppliedIndex    uint64
	AppliedTerm     uint64
	Committed       uint64
	SnapshotIndex   uint64
	SnapshotTerm    uint64
	TruncatedIndex  uint64
	TruncatedTerm   uint64
	SegmentIndex    uint64
	TruncatedOffset uint64
}

// Version represents current manifest state.
type Version struct {
	Levels       map[int][]FileMeta
	LogSegment   uint32
	LogOffset    uint64
	ValueLogs    map[uint32]ValueLogMeta
	ValueLogHead ValueLogMeta
	RaftPointers map[uint64]RaftLogPointer
	Regions      map[uint64]RegionMeta
}

// Edit operation types.
type EditType uint8

const (
	EditAddFile EditType = iota
	EditDeleteFile
	EditLogPointer
	EditValueLogHead
	EditDeleteValueLog
	EditUpdateValueLog
	EditRaftPointer
	EditRegion
)

// Edit describes a single metadata operation.
type Edit struct {
	Type      EditType
	File      *FileMeta
	LogSeg    uint32
	LogOffset uint64
	ValueLog  *ValueLogMeta
	Raft      *RaftLogPointer
	Region    *RegionEdit
}

// Manager controls manifest file operations.
type Manager struct {
	mu       sync.Mutex
	dir      string
	current  string
	manifest *os.File
	version  Version
}

// Open loads manifest from CURRENT file or creates a new one.
func Open(dir string) (*Manager, error) {
	if dir == "" {
		return nil, fmt.Errorf("manifest dir required")
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	mgr := &Manager{dir: dir}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := writeEdit(m.manifest, edit); err != nil {
		return err
	}
	if err := m.manifest.Sync(); err != nil {
		return err
	}
	m.apply(edit)
	return nil
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
func writeEdit(w io.Writer, edit Edit) error {
	buf := make([]byte, 0, 64)
	// magic + type
	buf = append(buf, []byte(editMagic)...)
	buf = append(buf, byte(edit.Type))
	switch edit.Type {
	case EditAddFile, EditDeleteFile:
		meta := edit.File
		buf = binary.AppendUvarint(buf, uint64(meta.Level))
		buf = binary.AppendUvarint(buf, meta.FileID)
		buf = binary.AppendUvarint(buf, meta.Size)
		buf = appendBytes(buf, meta.Smallest)
		buf = appendBytes(buf, meta.Largest)
		buf = binary.AppendUvarint(buf, meta.CreatedAt)
	case EditLogPointer:
		buf = binary.AppendUvarint(buf, uint64(edit.LogSeg))
		buf = binary.AppendUvarint(buf, edit.LogOffset)
	case EditValueLogHead:
		if edit.ValueLog != nil {
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.FileID))
			buf = binary.AppendUvarint(buf, edit.ValueLog.Offset)
		}
	case EditDeleteValueLog:
		if edit.ValueLog != nil {
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.FileID))
		}
	case EditUpdateValueLog:
		if edit.ValueLog != nil {
			buf = binary.AppendUvarint(buf, uint64(edit.ValueLog.FileID))
			buf = binary.AppendUvarint(buf, edit.ValueLog.Offset)
			if edit.ValueLog.Valid {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	case EditRaftPointer:
		if edit.Raft != nil {
			buf = binary.AppendUvarint(buf, edit.Raft.GroupID)
			buf = binary.AppendUvarint(buf, uint64(edit.Raft.Segment))
			buf = binary.AppendUvarint(buf, edit.Raft.Offset)
			buf = binary.AppendUvarint(buf, edit.Raft.AppliedIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.AppliedTerm)
			buf = binary.AppendUvarint(buf, edit.Raft.Committed)
			buf = binary.AppendUvarint(buf, edit.Raft.SnapshotIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.SnapshotTerm)
			buf = binary.AppendUvarint(buf, edit.Raft.TruncatedIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.TruncatedTerm)
			buf = binary.AppendUvarint(buf, edit.Raft.SegmentIndex)
			buf = binary.AppendUvarint(buf, edit.Raft.TruncatedOffset)
		}
	case EditRegion:
		if edit.Region != nil {
			buf = binary.AppendUvarint(buf, edit.Region.Meta.ID)
			if edit.Region.Delete {
				buf = append(buf, 1)
				break
			}
			buf = append(buf, 0)
			buf = appendBytes(buf, edit.Region.Meta.StartKey)
			buf = appendBytes(buf, edit.Region.Meta.EndKey)
			buf = binary.AppendUvarint(buf, edit.Region.Meta.Epoch.Version)
			buf = binary.AppendUvarint(buf, edit.Region.Meta.Epoch.ConfVersion)
			buf = append(buf, byte(edit.Region.Meta.State))
			buf = binary.AppendUvarint(buf, uint64(len(edit.Region.Meta.Peers)))
			for _, peer := range edit.Region.Meta.Peers {
				buf = binary.AppendUvarint(buf, peer.StoreID)
				buf = binary.AppendUvarint(buf, peer.PeerID)
			}
		}
	}
	// length prefix
	length := uint32(len(buf))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := w.Write(buf)
	return err
}

func readEdit(r *bufio.Reader) (Edit, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return Edit{}, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Edit{}, err
	}
	return decodeEdit(data)
}

func decodeEdit(data []byte) (Edit, error) {
	if len(data) < len(editMagic)+1 {
		return Edit{}, fmt.Errorf("manifest entry too short")
	}
	if string(data[:len(editMagic)]) != editMagic {
		return Edit{}, fmt.Errorf("bad manifest magic")
	}
	edit := Edit{Type: EditType(data[len(editMagic)])}
	pos := len(editMagic) + 1
	switch edit.Type {
	case EditAddFile, EditDeleteFile:
		level, n := binary.Uvarint(data[pos:])
		pos += n
		fileID, n := binary.Uvarint(data[pos:])
		pos += n
		size, n := binary.Uvarint(data[pos:])
		pos += n
		smallest, n := readBytes(data[pos:])
		pos += n
		largest, n := readBytes(data[pos:])
		pos += n
		created, n := binary.Uvarint(data[pos:])
		pos += n
		if pos > len(data) {
			return Edit{}, fmt.Errorf("manifest add/delete truncated")
		}
		edit.File = &FileMeta{
			Level:     int(level),
			FileID:    fileID,
			Size:      size,
			Smallest:  smallest,
			Largest:   largest,
			CreatedAt: created,
		}
	case EditLogPointer:
		seg, n := binary.Uvarint(data[pos:])
		pos += n
		off, n := binary.Uvarint(data[pos:])
		pos += n
		if pos > len(data) {
			return Edit{}, fmt.Errorf("manifest log pointer truncated")
		}
		edit.LogSeg = uint32(seg)
		edit.LogOffset = off
	case EditValueLogHead:
		if pos < len(data) {
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			offset, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest value log head truncated")
			}
			edit.ValueLog = &ValueLogMeta{
				FileID: uint32(fid64),
				Offset: offset,
				Valid:  true,
			}
		}
	case EditDeleteValueLog:
		if pos < len(data) {
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest value log delete truncated")
			}
			edit.ValueLog = &ValueLogMeta{
				FileID: uint32(fid64),
			}
		}
	case EditUpdateValueLog:
		if pos < len(data) {
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			offset, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest value log update truncated")
			}
			valid := false
			if pos < len(data) {
				valid = data[pos] == 1
				pos++
			}
			edit.ValueLog = &ValueLogMeta{
				FileID: uint32(fid64),
				Offset: offset,
				Valid:  valid,
			}
		}
	case EditRaftPointer:
		if pos <= len(data) {
			groupID, n := binary.Uvarint(data[pos:])
			pos += n
			seg, n := binary.Uvarint(data[pos:])
			pos += n
			off, n := binary.Uvarint(data[pos:])
			pos += n
			appliedIdx, n := binary.Uvarint(data[pos:])
			pos += n
			appliedTerm, n := binary.Uvarint(data[pos:])
			pos += n
			committed, n := binary.Uvarint(data[pos:])
			pos += n
			snapIdx, n := binary.Uvarint(data[pos:])
			pos += n
			snapTerm, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest raft pointer truncated")
			}
			var truncatedIdx uint64
			var truncatedTerm uint64
			var segmentIndex uint64
			var truncatedOffset uint64
			if pos < len(data) {
				truncatedIdx, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer truncated index overflow")
				}
			}
			if pos < len(data) {
				truncatedTerm, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer truncated term overflow")
				}
			}
			if pos < len(data) {
				segmentIndex, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer segment index overflow")
				}
			}
			if pos < len(data) {
				truncatedOffset, n = binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest raft pointer truncated offset overflow")
				}
			}
			edit.Raft = &RaftLogPointer{
				GroupID:         groupID,
				Segment:         uint32(seg),
				Offset:          off,
				AppliedIndex:    appliedIdx,
				AppliedTerm:     appliedTerm,
				Committed:       committed,
				SnapshotIndex:   snapIdx,
				SnapshotTerm:    snapTerm,
				TruncatedIndex:  truncatedIdx,
				TruncatedTerm:   truncatedTerm,
				SegmentIndex:    segmentIndex,
				TruncatedOffset: truncatedOffset,
			}
		}
	case EditRegion:
		if pos <= len(data) {
			regionID, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest region edit truncated after id")
			}
			var delete bool
			if pos < len(data) {
				delete = data[pos] == 1
				pos++
			}
			if delete {
				edit.Region = &RegionEdit{
					Meta:   RegionMeta{ID: regionID},
					Delete: true,
				}
				break
			}
			start, n := readBytes(data[pos:])
			pos += n
			end, n := readBytes(data[pos:])
			pos += n
			version, n := binary.Uvarint(data[pos:])
			pos += n
			confVer, n := binary.Uvarint(data[pos:])
			pos += n
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest region edit truncated epoch")
			}
			var state RegionState
			if pos < len(data) {
				state = RegionState(data[pos])
				pos++
			}
			peersCount := uint64(0)
			if pos < len(data) {
				peersCount, n = binary.Uvarint(data[pos:])
				pos += n
			}
			if pos > len(data) {
				return Edit{}, fmt.Errorf("manifest region edit truncated peer count")
			}
			peers := make([]PeerMeta, 0, peersCount)
			for i := uint64(0); i < peersCount; i++ {
				storeID, n := binary.Uvarint(data[pos:])
				pos += n
				peerID, n := binary.Uvarint(data[pos:])
				pos += n
				if pos > len(data) {
					return Edit{}, fmt.Errorf("manifest region edit truncated peer meta")
				}
				peers = append(peers, PeerMeta{StoreID: storeID, PeerID: peerID})
			}
			edit.Region = &RegionEdit{
				Meta: RegionMeta{
					ID:       regionID,
					StartKey: append([]byte(nil), start...),
					EndKey:   append([]byte(nil), end...),
					Epoch: RegionEpoch{
						Version:     version,
						ConfVersion: confVer,
					},
					Peers: append([]PeerMeta(nil), peers...),
					State: state,
				},
			}
		}
	}
	return edit, nil
}

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

func appendUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func appendUint32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}

func appendBytes(dst []byte, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func readBytes(data []byte) ([]byte, int) {
	length, n := binary.Uvarint(data)
	pos := n
	end := pos + int(length)
	if n <= 0 || end > len(data) {
		return nil, len(data)
	}
	return data[pos:end], n + int(length)
}
