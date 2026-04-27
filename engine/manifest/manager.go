// Package manifest persists storage-engine metadata such as SST layout, WAL
// replay position, and value-log state.
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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/engine/vfs"
)

const (
	currentFileName         = "CURRENT"
	manifestFilePrefix      = "MANIFEST-"
	manifestFileWidth       = 6
	defaultRewriteThreshold = 64 << 20
	manifestFilePermissions = 0o666
	manifestTempCurrentName = "CURRENT.tmp"
)

// Manager controls manifest file operations.
type Manager struct {
	mu         sync.Mutex
	dir        string
	fs         vfs.FS
	current    string
	manifest   vfs.File
	version    Version
	syncWrites bool

	rewriteThreshold int64
	nextFileID       uint64
}

// Open loads manifest from CURRENT file or creates a new one.
// Nil fs defaults to OSFS.
func Open(dir string, fs vfs.FS) (*Manager, error) {
	if dir == "" {
		return nil, fmt.Errorf("manifest dir required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	mgr := &Manager{
		dir:              dir,
		fs:               fs,
		syncWrites:       true,
		rewriteThreshold: defaultRewriteThreshold,
	}
	if err := mgr.loadCurrent(); err != nil {
		if err := mgr.createNew(); err != nil {
			return nil, err
		}
		return mgr, nil
	}
	mgr.initNextFileID()
	if err := mgr.replay(); err != nil {
		_ = mgr.Close()
		return nil, err
	}
	return mgr, nil
}

func (m *Manager) loadCurrent() error {
	path := filepath.Join(m.dir, currentFileName)
	data, err := m.fs.ReadFile(path)
	if err != nil {
		return err
	}
	m.current = string(data)
	manifestPath := filepath.Join(m.dir, m.current)
	m.manifest, err = m.fs.OpenFileHandle(manifestPath, os.O_RDWR, manifestFilePermissions)
	return err
}

func (m *Manager) createNew() error {
	fileName := manifestFileName(1)
	path := filepath.Join(m.dir, fileName)
	f, err := m.fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, manifestFilePermissions)
	if err != nil {
		return err
	}
	m.manifest = f
	m.current = fileName
	m.nextFileID = 2
	if err := m.writeCurrent(); err != nil {
		return err
	}
	m.version = Version{
		Levels:       make(map[int][]FileMeta),
		ValueLogs:    make(map[ValueLogID]ValueLogMeta),
		ValueLogHead: make(map[uint32]ValueLogMeta),
	}
	return nil
}

func (m *Manager) writeCurrent() error {
	tmp := filepath.Join(m.dir, manifestTempCurrentName)
	f, err := m.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, manifestFilePermissions)
	if err != nil {
		return err
	}
	writeErr := writeAll(f, []byte(m.current))
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = m.fs.Remove(tmp)
		return writeErr
	}
	if syncErr != nil {
		_ = m.fs.Remove(tmp)
		return syncErr
	}
	if closeErr != nil {
		_ = m.fs.Remove(tmp)
		return closeErr
	}
	dst := filepath.Join(m.dir, currentFileName)
	if err := m.fs.Rename(tmp, dst); err != nil {
		return err
	}
	return vfs.SyncDir(m.fs, m.dir)
}

func (m *Manager) replay() error {
	m.version = Version{
		Levels:       make(map[int][]FileMeta),
		ValueLogs:    make(map[ValueLogID]ValueLogMeta),
		ValueLogHead: make(map[uint32]ValueLogMeta),
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
	case EditValueLogHead:
		if edit.ValueLog != nil {
			meta := *edit.ValueLog
			meta.Valid = true
			id := ValueLogID{Bucket: meta.Bucket, FileID: meta.FileID}
			m.version.ValueLogs[id] = meta
			if m.version.ValueLogHead == nil {
				m.version.ValueLogHead = make(map[uint32]ValueLogMeta)
			}
			m.version.ValueLogHead[meta.Bucket] = meta
		}
	case EditDeleteValueLog:
		if edit.ValueLog != nil {
			id := ValueLogID{Bucket: edit.ValueLog.Bucket, FileID: edit.ValueLog.FileID}
			meta := m.version.ValueLogs[id]
			meta.Bucket = edit.ValueLog.Bucket
			meta.FileID = edit.ValueLog.FileID
			meta.Offset = 0
			meta.Valid = false
			m.version.ValueLogs[id] = meta
			if head, ok := m.version.ValueLogHead[meta.Bucket]; ok && head.FileID == meta.FileID {
				delete(m.version.ValueLogHead, meta.Bucket)
			}
		}
	case EditUpdateValueLog:
		if edit.ValueLog != nil {
			meta := *edit.ValueLog
			id := ValueLogID{Bucket: meta.Bucket, FileID: meta.FileID}
			m.version.ValueLogs[id] = meta
			if head, ok := m.version.ValueLogHead[meta.Bucket]; ok && head.FileID == meta.FileID {
				if meta.Valid {
					m.version.ValueLogHead[meta.Bucket] = meta
				} else {
					delete(m.version.ValueLogHead, meta.Bucket)
				}
			}
		}
	}
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

// logEditsLocked appends edits and updates the manifest; caller must hold m.mu.
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
	return m.maybeRewriteLocked()
}

func requiresSync(edit Edit) bool {
	switch edit.Type {
	case EditAddFile, EditDeleteFile, EditValueLogHead, EditDeleteValueLog, EditUpdateValueLog:
		return true
	default:
		return false
	}
}

// SetRewriteThreshold configures how large the manifest file can grow before
// a rewrite is attempted. Values <= 0 disable automatic rewrites.
func (m *Manager) SetRewriteThreshold(bytes int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.rewriteThreshold = bytes
	m.mu.Unlock()
}

// Rewrite forces a manifest rewrite using the current version snapshot.
func (m *Manager) Rewrite() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rewriteLocked()
}

// maybeRewriteLocked rewrites the manifest if threshold exceeded; caller must hold m.mu.
func (m *Manager) maybeRewriteLocked() error {
	if m.rewriteThreshold <= 0 || m.manifest == nil {
		return nil
	}
	info, err := m.manifest.Stat()
	if err != nil {
		return err
	}
	if info.Size() < m.rewriteThreshold {
		return nil
	}
	return m.rewriteLocked()
}

// rewriteLocked rewrites the manifest file; caller must hold m.mu.
func (m *Manager) rewriteLocked() error {
	if m.manifest == nil {
		return nil
	}
	fileName, err := m.nextManifestFileLocked()
	if err != nil {
		return err
	}
	path := filepath.Join(m.dir, fileName)
	f, err := m.fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, manifestFilePermissions)
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(f)
	if err := m.writeSnapshot(buf); err != nil {
		_ = f.Close()
		_ = m.fs.Remove(path)
		return err
	}
	if err := buf.Flush(); err != nil {
		_ = f.Close()
		_ = m.fs.Remove(path)
		return err
	}
	if m.syncWrites {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}

	oldName := m.current
	m.current = fileName
	if err := m.writeCurrent(); err != nil {
		m.current = oldName
		return err
	}
	if m.manifest != nil {
		_ = m.manifest.Close()
	}
	m.manifest, err = m.fs.OpenFileHandle(path, os.O_RDWR, manifestFilePermissions)
	if err != nil {
		return err
	}
	if _, err := m.manifest.Seek(0, io.SeekEnd); err != nil {
		_ = m.manifest.Close()
		return err
	}
	if oldName != "" && oldName != fileName {
		_ = m.fs.Remove(filepath.Join(m.dir, oldName))
	}
	return nil
}

func (m *Manager) writeSnapshot(w io.Writer) error {
	version := m.version
	levels := make([]int, 0, len(version.Levels))
	for level := range version.Levels {
		levels = append(levels, level)
	}
	sort.Ints(levels)
	for _, level := range levels {
		files := append([]FileMeta(nil), version.Levels[level]...)
		sort.Slice(files, func(i, j int) bool { return files[i].FileID < files[j].FileID })
		for _, meta := range files {
			metaCopy := meta
			metaCopy.Level = level
			if err := writeEdit(w, Edit{Type: EditAddFile, File: &metaCopy}); err != nil {
				return err
			}
		}
	}

	if len(version.ValueLogs) > 0 {
		ids := make([]ValueLogID, 0, len(version.ValueLogs))
		for id := range version.ValueLogs {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool {
			if ids[i].Bucket == ids[j].Bucket {
				return ids[i].FileID < ids[j].FileID
			}
			return ids[i].Bucket < ids[j].Bucket
		})
		for _, id := range ids {
			meta := version.ValueLogs[id]
			metaCopy := meta
			if meta.Valid {
				if err := writeEdit(w, Edit{Type: EditUpdateValueLog, ValueLog: &metaCopy}); err != nil {
					return err
				}
			} else {
				if err := writeEdit(w, Edit{Type: EditDeleteValueLog, ValueLog: &metaCopy}); err != nil {
					return err
				}
			}
		}
	}
	if len(version.ValueLogHead) > 0 {
		buckets := make([]uint32, 0, len(version.ValueLogHead))
		for bucket := range version.ValueLogHead {
			buckets = append(buckets, bucket)
		}
		slices.Sort(buckets)
		for _, bucket := range buckets {
			head := version.ValueLogHead[bucket]
			if err := writeEdit(w, Edit{Type: EditValueLogHead, ValueLog: &head}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Manager) initNextFileID() {
	m.nextFileID = parseManifestFileID(strings.TrimSpace(m.current)) + 1
	if m.nextFileID == 1 {
		m.nextFileID = 2
	}
}

// nextManifestFileLocked picks the next manifest filename; caller must hold m.mu.
func (m *Manager) nextManifestFileLocked() (string, error) {
	id := m.nextFileID
	if id == 0 {
		id = 1
	}
	for {
		name := manifestFileName(id)
		if _, err := m.fs.Stat(filepath.Join(m.dir, name)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				m.nextFileID = id + 1
				return name, nil
			}
			return "", err
		}
		id++
	}
}

func manifestFileName(id uint64) string {
	return fmt.Sprintf("%s%0*d", manifestFilePrefix, manifestFileWidth, id)
}

func parseManifestFileID(name string) uint64 {
	name = strings.TrimSpace(name)
	if !strings.HasPrefix(name, manifestFilePrefix) {
		return 0
	}
	raw := strings.TrimPrefix(name, manifestFilePrefix)
	if raw == "" {
		return 0
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// Current returns a snapshot of the current version.
func (m *Manager) Current() Version {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := Version{
		Levels:       make(map[int][]FileMeta),
		ValueLogs:    make(map[ValueLogID]ValueLogMeta),
		ValueLogHead: make(map[uint32]ValueLogMeta, len(m.version.ValueLogHead)),
	}
	for level, files := range m.version.Levels {
		cp.Levels[level] = append([]FileMeta(nil), files[:]...)
	}
	maps.Copy(cp.ValueLogs, m.version.ValueLogs)
	maps.Copy(cp.ValueLogHead, m.version.ValueLogHead)
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
	if m.manifest == nil {
		return nil
	}
	if err := m.manifest.Close(); err != nil {
		return err
	}
	m.manifest = nil
	return nil
}

// LogValueLogHead records the active value log head pointer.
func (m *Manager) LogValueLogHead(bucket uint32, fid uint32, offset uint64) error {
	meta := &ValueLogMeta{
		Bucket: bucket,
		FileID: fid,
		Offset: offset,
		Valid:  true,
	}
	return m.LogEdits(Edit{Type: EditValueLogHead, ValueLog: meta})
}

// LogValueLogDelete records value log segment deletion.
func (m *Manager) LogValueLogDelete(bucket uint32, fid uint32) error {
	meta := &ValueLogMeta{
		Bucket: bucket,
		FileID: fid,
		Valid:  false,
	}
	return m.LogEdits(Edit{Type: EditDeleteValueLog, ValueLog: meta})
}

// LogValueLogUpdate records an updated value log metadata entry.
func (m *Manager) LogValueLogUpdate(meta ValueLogMeta) error {
	cp := meta
	return m.LogEdits(Edit{Type: EditUpdateValueLog, ValueLog: &cp})
}

// ValueLogHead returns the latest persisted value log head.
func (m *Manager) ValueLogHead() map[uint32]ValueLogMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uint32]ValueLogMeta, len(m.version.ValueLogHead))
	maps.Copy(out, m.version.ValueLogHead)
	return out
}

// ValueLogStatus returns a copy of all tracked value log segment metadata.
func (m *Manager) ValueLogStatus() map[ValueLogID]ValueLogMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[ValueLogID]ValueLogMeta, len(m.version.ValueLogs))
	maps.Copy(out, m.version.ValueLogs)
	return out
}

// Internal encoding helpers
// Check ensures the manifest CURRENT pointer and edit stream are well-formed
// without mutating on-disk state. Nil fs defaults to OSFS.
func Check(dir string, fs vfs.FS) error {
	if dir == "" {
		return fmt.Errorf("manifest: directory required")
	}
	fs = vfs.Ensure(fs)

	currentPath := filepath.Join(dir, currentFileName)
	data, err := fs.ReadFile(currentPath)
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
	f, err := fs.OpenFileHandle(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("manifest: open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()

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
				return fmt.Errorf("manifest: partial length at %d", pos)
			}
			return fmt.Errorf("manifest: read length at %d: %w", pos, err)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("manifest: partial edit at %d", pos)
			}
			return fmt.Errorf("manifest: read payload at %d: %w", pos, err)
		}
		if _, err := decodeEdit(payload); err != nil {
			return fmt.Errorf("manifest: invalid edit at %d: %w", pos, err)
		}
		offset = pos + int64(length) + 4
	}
}

// Verify ensures manifest and CURRENT pointer are well-formed. It truncates any
// partially written edits left at the end of the manifest file. Nil fs defaults
// to OSFS.
func Verify(dir string, fs vfs.FS) error {
	if dir == "" {
		return fmt.Errorf("manifest: directory required")
	}
	fs = vfs.Ensure(fs)
	tmp := filepath.Join(dir, manifestTempCurrentName)
	if _, err := fs.Stat(tmp); err == nil {
		_ = fs.Remove(tmp)
	}

	currentPath := filepath.Join(dir, currentFileName)
	data, err := fs.ReadFile(currentPath)
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
	f, err := fs.OpenFileHandle(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("manifest: open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()

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
