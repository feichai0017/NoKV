package manifest

import (
	"maps"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// Version represents current manifest state.
type Version struct {
	Levels       map[int][]FileMeta
	LogSegment   uint32
	LogOffset    uint64
	ValueLogs    map[uint32]ValueLogMeta
	ValueLogHead ValueLogMeta
}

// Edit operation types.
type EditType uint8

const (
	EditAddFile EditType = iota
	EditDeleteFile
	EditLogPointer
	EditValueLogHead
	EditDeleteValueLog
)

// Edit describes a single metadata operation.
type Edit struct {
	Type      EditType
	File      *FileMeta
	LogSeg    uint32
	LogOffset uint64
	ValueLog  *ValueLogMeta
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
		Levels:    make(map[int][]FileMeta),
		ValueLogs: make(map[uint32]ValueLogMeta),
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
		Levels:    make(map[int][]FileMeta),
		ValueLogs: make(map[uint32]ValueLogMeta),
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
	}
	for level, files := range m.version.Levels {
		cp.Levels[level] = append([]FileMeta(nil), files[:]...)
	}
	maps.Copy(cp.ValueLogs, m.version.ValueLogs)
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
		edit.LogSeg = uint32(seg)
		edit.LogOffset = off
	case EditValueLogHead:
		if pos < len(data) {
			fid64, n := binary.Uvarint(data[pos:])
			pos += n
			offset, n := binary.Uvarint(data[pos:])
			pos += n
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
			edit.ValueLog = &ValueLogMeta{
				FileID: uint32(fid64),
			}
		}
	}
	return edit, nil
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
