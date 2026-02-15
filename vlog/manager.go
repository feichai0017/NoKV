// Package vlog implements the value-log segment manager and IO helpers.
package vlog

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
	pkgerrors "github.com/pkg/errors"
)

type Config struct {
	Dir      string
	FileMode os.FileMode
	MaxSize  int64
	Bucket   uint32
	FS       vfs.FS
}

type Manager struct {
	cfg       Config
	bucket    uint32
	filesLock sync.RWMutex
	files     map[uint32]*segment
	index     atomic.Value
	maxFid    uint32
	active    *segment
	activeID  uint32
	offset    uint32
}

type segmentIndex struct {
	files map[uint32]*segment
}

func (m *Manager) SetMaxSize(maxSize int64) {
	if maxSize <= 0 {
		return
	}
	m.filesLock.Lock()
	m.cfg.MaxSize = maxSize
	m.filesLock.Unlock()
}

// loadIndex returns a lock-free snapshot of the segment map.
func (m *Manager) loadIndex() *segmentIndex {
	if idx := m.index.Load(); idx != nil {
		return idx.(*segmentIndex)
	}
	return &segmentIndex{files: make(map[uint32]*segment)}
}

// refreshIndexLocked publishes a copy-on-write snapshot of the segment map.
// Caller must hold m.filesLock.
func (m *Manager) refreshIndexLocked() {
	next := make(map[uint32]*segment, len(m.files))
	maps.Copy(next, m.files)
	m.index.Store(&segmentIndex{files: next})
}

// ensureActiveLocked returns the active segment store; caller must hold m.filesLock.
func (m *Manager) ensureActiveLocked() (*file.LogFile, uint32, error) {
	if m.active != nil {
		return m.active.store, m.activeID, nil
	}
	next := uint32(0)
	if len(m.files) > 0 {
		next = m.maxFid + 1
	}
	if _, err := m.create(next); err != nil {
		return nil, 0, err
	}
	m.active = m.files[m.maxFid]
	m.activeID = m.maxFid
	m.offset = uint32(kv.ValueLogHeaderSize)
	m.refreshIndexLocked()
	return m.active.store, m.activeID, nil
}

func Open(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("vlog manager: dir required")
	}
	cfg.FS = vfs.Ensure(cfg.FS)
	if err := cfg.FS.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = int64(1 << 29)
	}
	mgr := &Manager{
		cfg:    cfg,
		bucket: cfg.Bucket,
		files:  make(map[uint32]*segment),
	}
	if err := mgr.populate(); err != nil {
		return nil, err
	}
	fresh := false
	if len(mgr.files) == 0 {
		if _, err := mgr.create(0); err != nil {
			return nil, err
		}
		mgr.active = mgr.files[0]
		mgr.activeID = 0
		fresh = true
	} else {
		mgr.activeID = mgr.maxFid
		mgr.active = mgr.files[mgr.activeID]
	}
	if mgr.active != nil {
		if fresh {
			mgr.offset = uint32(kv.ValueLogHeaderSize)
		} else if size := mgr.active.store.Size(); size >= 0 {
			mgr.offset = uint32(size)
		}
	}
	mgr.filesLock.Lock()
	mgr.refreshIndexLocked()
	mgr.filesLock.Unlock()
	return mgr, nil
}

func openLogFile(fs vfs.FS, fid uint32, path string, dir string, maxSize int64, readOnly bool) (*file.LogFile, error) {
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	lf := &file.LogFile{}
	if err := lf.Open(&file.Options{
		FID:      uint64(fid),
		FileName: path,
		Dir:      dir,
		Flag:     flag,
		MaxSz:    int(maxSize),
		FS:       fs,
	}); err != nil {
		return nil, err
	}
	return lf, nil
}

func createLogFile(fs vfs.FS, fid uint32, path string, dir string, maxSize int64) (*file.LogFile, error) {
	lf, err := openLogFile(fs, fid, path, dir, maxSize, false)
	if err != nil {
		return nil, err
	}
	if err := lf.Bootstrap(); err != nil {
		_ = lf.Close()
		return nil, err
	}
	return lf, nil
}

func (m *Manager) populate() error {
	files, err := m.cfg.FS.Glob(filepath.Join(m.cfg.Dir, "*.vlog"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	var max uint32
	for _, path := range files {
		var fid uint32
		if _, err := fmt.Sscanf(filepath.Base(path), "%05d.vlog", &fid); err != nil {
			continue
		}
		if fid > max {
			max = fid
		}
	}
	m.maxFid = max
	for _, path := range files {
		var fid uint32
		if _, err := fmt.Sscanf(filepath.Base(path), "%05d.vlog", &fid); err != nil {
			continue
		}
		readonly := fid != max
		store, err := openLogFile(m.cfg.FS, fid, path, m.cfg.Dir, m.cfg.MaxSize, readonly)
		if err != nil {
			return err
		}
		m.files[fid] = newSegment(store, readonly)
	}
	return nil
}

func (m *Manager) create(fid uint32) (*file.LogFile, error) {
	path := filepath.Join(m.cfg.Dir, fmt.Sprintf("%05d.vlog", fid))
	store, err := createLogFile(m.cfg.FS, fid, path, m.cfg.Dir, m.cfg.MaxSize)
	if err != nil {
		return nil, err
	}
	m.files[fid] = newSegment(store, false)
	if fid > m.maxFid {
		m.maxFid = fid
	}
	return store, nil
}

func (m *Manager) Rotate() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	return m.rotateLocked()
}

// rotateLocked rotates the active segment; caller must hold m.filesLock.
func (m *Manager) rotateLocked() error {
	if m.active != nil {
		if err := m.active.store.DoneWriting(m.offset); err != nil {
			return err
		}
		// Previous active becomes read-only to reduce cache/RSS.
		_ = m.active.store.SetReadOnly()
		m.active.seal()
	}
	nextID := m.maxFid + 1
	if _, err := m.create(nextID); err != nil {
		return err
	}
	m.active = m.files[nextID]
	m.activeID = nextID
	m.offset = uint32(kv.ValueLogHeaderSize)
	m.refreshIndexLocked()
	return nil
}

// getStoreForRead returns a store and release callback without acquiring m.filesLock.
// Sealed segments are pinned; active segments use the store read lock.
func (m *Manager) getStoreForRead(fid uint32) (*file.LogFile, func(), error) {
	seg, ok := m.loadIndex().files[fid]
	if !ok {
		return nil, nil, pkgerrors.Errorf("value log file %d not found", fid)
	}
	if seg.isClosing() {
		return nil, nil, pkgerrors.Errorf("value log file %d closing", fid)
	}
	if seg.isSealed() {
		if !seg.pinRead() {
			return nil, nil, pkgerrors.Errorf("value log file %d closing", fid)
		}
		return seg.store, seg.unpinRead, nil
	}
	seg.store.Lock.RLock()
	return seg.store, seg.store.Lock.RUnlock, nil
}

func (m *Manager) getFile(fid uint32) (*file.LogFile, error) {
	seg, ok := m.loadIndex().files[fid]
	if !ok {
		return nil, pkgerrors.Errorf("value log file %d not found", fid)
	}
	return seg.store, nil
}

func (m *Manager) Remove(fid uint32) error {
	m.filesLock.Lock()
	seg, ok := m.files[fid]
	if !ok {
		m.filesLock.Unlock()
		return nil
	}
	delete(m.files, fid)

	var maxID uint32
	for id := range m.files {
		if id > maxID {
			maxID = id
		}
	}
	m.maxFid = maxID

	if fid == m.activeID {
		if len(m.files) == 0 {
			m.active = nil
			m.activeID = 0
			m.offset = 0
		} else {
			m.activeID = maxID
			m.active = m.files[maxID]
			if m.active != nil {
				if size := m.active.store.Size(); size >= 0 {
					m.offset = uint32(size)
				}
			}
		}
	}
	m.refreshIndexLocked()
	m.filesLock.Unlock()

	seg.beginClose()
	if seg.isSealed() {
		seg.waitForNoPins()
	}
	seg.store.Lock.Lock()
	defer seg.store.Lock.Unlock()
	if err := seg.store.Close(); err != nil {
		return err
	}
	return m.cfg.FS.Remove(seg.store.FileName())
}

func (m *Manager) MaxFID() uint32 {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return m.maxFid
}

func (m *Manager) ActiveFID() uint32 {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return m.activeID
}

func (m *Manager) Head() kv.ValuePtr {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return kv.ValuePtr{
		Fid:    m.activeID,
		Offset: m.offset,
		Bucket: m.bucket,
	}
}

// SyncActive fsyncs the current active value log segment.
// It is primarily used in tests; the write path syncs all touched segments via SyncFIDs.
func (m *Manager) SyncActive() error {
	if m == nil {
		return nil
	}
	m.filesLock.RLock()
	seg := m.active
	m.filesLock.RUnlock()
	if seg == nil || seg.store == nil {
		return nil
	}
	seg.store.Lock.Lock()
	defer seg.store.Lock.Unlock()
	return seg.store.Sync()
}

// SyncFIDs fsyncs the provided value log segments.
func (m *Manager) SyncFIDs(fids []uint32) error {
	if m == nil || len(fids) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(fids))
	for _, fid := range fids {
		if _, ok := seen[fid]; ok {
			continue
		}
		seen[fid] = struct{}{}

		m.filesLock.RLock()
		seg := m.files[fid]
		if seg != nil && seg.store != nil {
			seg.store.Lock.Lock()
		}
		m.filesLock.RUnlock()

		if seg == nil || seg.store == nil {
			continue
		}
		err := seg.store.Sync()
		seg.store.Lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// SegmentSize reports the current size of the segment identified by fid.
func (m *Manager) SegmentSize(fid uint32) (int64, error) {
	store, err := m.getFile(fid)
	if err != nil {
		return 0, err
	}
	return store.Size(), nil
}

// SegmentInit refreshes the mmap metadata for the specified segment.
func (m *Manager) SegmentInit(fid uint32) error {
	store, err := m.getFile(fid)
	if err != nil {
		return err
	}
	return store.Init()
}

// SegmentBootstrap rewrites the header of the provided segment, resetting its
// logical contents. It is typically used when truncation shrinks a file below
// the header size and the segment needs to be treated as empty.
func (m *Manager) SegmentBootstrap(fid uint32) error {
	store, err := m.getFile(fid)
	if err != nil {
		return err
	}
	store.Lock.Lock()
	defer store.Lock.Unlock()
	return store.Bootstrap()
}

// SegmentTruncate shrinks the segment to the provided offset.
func (m *Manager) SegmentTruncate(fid uint32, offset uint32) error {
	store, err := m.getFile(fid)
	if err != nil {
		return err
	}
	store.Lock.Lock()
	defer store.Lock.Unlock()
	return store.Truncate(int64(offset))
}

// Rewind rolls back the active head to the provided pointer, truncating any bytes
// beyond it and removing files created after the pointer's file. It is primarily
// used to recover from value log write failures so that partially written
// batches don't leave garbage in the log.
func (m *Manager) Rewind(ptr kv.ValuePtr) error {
	if ptr.Bucket != m.bucket {
		return pkgerrors.Errorf("rewind: bucket mismatch: want %d got %d", m.bucket, ptr.Bucket)
	}
	var (
		extra []struct {
			seg  *segment
			name string
		}
		active *segment
	)

	m.filesLock.Lock()
	for fid, seg := range m.files {
		if fid > ptr.Fid {
			extra = append(extra, struct {
				seg  *segment
				name string
			}{seg: seg, name: seg.store.FileName()})
			delete(m.files, fid)
		}
	}
	seg, ok := m.files[ptr.Fid]
	if ok {
		active = seg
		m.active = seg
		m.activeID = ptr.Fid
		m.maxFid = ptr.Fid
		m.offset = ptr.Offset
	}
	m.refreshIndexLocked()
	m.filesLock.Unlock()

	if !ok {
		return pkgerrors.Errorf("rewind: value log file %d not found", ptr.Fid)
	}

	var firstErr error
	active.beginClose()
	active.waitForNoPins()
	if err := active.store.SetWritable(); err != nil {
		firstErr = err
	}
	active.activate()
	for _, item := range extra {
		item.seg.beginClose()
		if item.seg.isSealed() {
			item.seg.waitForNoPins()
		}
		item.seg.store.Lock.Lock()
		if err := item.seg.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		item.seg.store.Lock.Unlock()
		if err := m.cfg.FS.Remove(item.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	active.store.Lock.Lock()
	if err := active.store.Truncate(int64(ptr.Offset)); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := active.store.Init(); err != nil && firstErr == nil {
		firstErr = err
	}
	active.store.Lock.Unlock()

	return firstErr
}

func (m *Manager) Close() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	var firstErr error
	for fid, seg := range m.files {
		seg.beginClose()
		if seg.isSealed() {
			seg.waitForNoPins()
		}
		if err := seg.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.files, fid)
	}
	m.active = nil
	m.activeID = 0
	m.offset = 0
	m.refreshIndexLocked()
	return firstErr
}

func (m *Manager) ListFIDs() []uint32 {
	idx := m.loadIndex()
	fids := make([]uint32, 0, len(idx.files))
	for fid := range idx.files {
		fids = append(fids, fid)
	}
	slices.Sort(fids)
	return fids
}
