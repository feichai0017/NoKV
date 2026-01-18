// Package vlog implements the value-log segment manager and IO helpers.
package vlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

type Config struct {
	Dir      string
	FileMode os.FileMode
	MaxSize  int64
}

type Manager struct {
	cfg       Config
	filesLock sync.RWMutex
	files     map[uint32]*file.LogFile
	maxFid    uint32
	active    *file.LogFile
	activeID  uint32
	offset    uint32
	hooks     ManagerTestingHooks
}

// ManagerTestingHooks provides callbacks that are used only in tests to inject
// failures in the value-log manager. They are no-ops in production code and are
// guarded by the Manager's internal locking to avoid data races when set.
type ManagerTestingHooks struct {
	BeforeAppend func(*Manager, []byte) error
	BeforeRotate func(*Manager) error
	BeforeSync   func(*Manager, uint32) error
}

// SetTestingHooks installs testing callbacks on the manager. It is intended for
// use in tests only and should not be used by production code.
func (m *Manager) SetTestingHooks(h ManagerTestingHooks) {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	m.hooks = h
}

func (m *Manager) SetMaxSize(maxSize int64) {
	if maxSize <= 0 {
		return
	}
	m.filesLock.Lock()
	m.cfg.MaxSize = maxSize
	m.filesLock.Unlock()
}

// runBeforeAppendHook invokes the testing hook (if any) before an append.
func (m *Manager) runBeforeAppendHook(data []byte) error {
	m.filesLock.RLock()
	hook := m.hooks.BeforeAppend
	m.filesLock.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(m, data)
}

// runBeforeSyncHook invokes the testing hook (if any) before syncing a segment.
func (m *Manager) runBeforeSyncHook(fid uint32) error {
	m.filesLock.RLock()
	hook := m.hooks.BeforeSync
	m.filesLock.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(m, fid)
}

func (m *Manager) ensureActiveLocked() (*file.LogFile, uint32, error) {
	if m.active != nil {
		return m.active, m.activeID, nil
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
	return m.active, m.activeID, nil
}

func Open(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("vlog manager: dir required")
	}
	if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	mgr := &Manager{
		cfg:   cfg,
		files: make(map[uint32]*file.LogFile),
	}
	if mgr.cfg.MaxSize == 0 {
		mgr.cfg.MaxSize = int64(1 << 29)
	}
	if err := mgr.populate(); err != nil {
		return nil, err
	}
	fresh := false
	if len(mgr.files) == 0 {
		lf, err := mgr.create(0)
		if err != nil {
			return nil, err
		}
		mgr.active = lf
		mgr.activeID = 0
		fresh = true
	} else {
		mgr.activeID = mgr.maxFid
		mgr.active = mgr.files[mgr.activeID]
	}
	if mgr.active != nil {
		if fresh {
			mgr.offset = uint32(kv.ValueLogHeaderSize)
		} else {
			off, err := mgr.active.Seek(0, io.SeekEnd)
			if err != nil {
				return nil, err
			}
			mgr.offset = uint32(off)
		}
	}
	return mgr, nil
}

func (m *Manager) populate() error {
	files, err := filepath.Glob(filepath.Join(m.cfg.Dir, "*.vlog"))
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
		lf := &file.LogFile{}
		if err := lf.Open(&file.Options{
			FID:      uint64(fid),
			FileName: path,
			Dir:      m.cfg.Dir,
			Flag: func() int {
				if fid == max {
					return os.O_CREATE | os.O_RDWR
				}
				return os.O_RDONLY
			}(),
			MaxSz: int(m.cfg.MaxSize),
		}); err != nil {
			return err
		}
		m.files[fid] = lf
	}
	return nil
}

func (m *Manager) create(fid uint32) (*file.LogFile, error) {
	path := filepath.Join(m.cfg.Dir, fmt.Sprintf("%05d.vlog", fid))
	lf := &file.LogFile{}
	if err := lf.Open(&file.Options{
		FID:      uint64(fid),
		FileName: path,
		Dir:      m.cfg.Dir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(m.cfg.MaxSize),
	}); err != nil {
		return nil, err
	}
	if err := lf.Bootstrap(); err != nil {
		return nil, err
	}
	m.files[fid] = lf
	if fid > m.maxFid {
		m.maxFid = fid
	}
	return lf, nil
}

func (m *Manager) Rotate() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	return m.rotateLocked()
}

func (m *Manager) rotateLocked() error {
	if hook := m.hooks.BeforeRotate; hook != nil {
		if err := hook(m); err != nil {
			return err
		}
	}
	if m.active != nil {
		if err := m.active.DoneWriting(m.offset); err != nil {
			return err
		}
		// Previous active becomes read-only to reduce cache/RSS.
		_ = m.active.SetReadOnly()
	}
	nextID := m.maxFid + 1
	if _, err := m.create(nextID); err != nil {
		return err
	}
	m.active = m.files[nextID]
	m.activeID = nextID
	m.offset = uint32(kv.ValueLogHeaderSize)
	return nil
}

func (m *Manager) getFileRLocked(fid uint32) (*file.LogFile, func(), error) {
	m.filesLock.RLock()
	lf, ok := m.files[fid]
	if !ok {
		m.filesLock.RUnlock()
		return nil, nil, pkgerrors.Errorf("value log file %d not found", fid)
	}
	if lf.IsSealed() {
		if !lf.PinRead() {
			m.filesLock.RUnlock()
			return nil, nil, pkgerrors.Errorf("value log file %d closing", fid)
		}
		m.filesLock.RUnlock()
		return lf, lf.UnpinRead, nil
	}
	lf.Lock.RLock()
	m.filesLock.RUnlock()
	return lf, lf.Lock.RUnlock, nil
}

func (m *Manager) getFile(fid uint32) (*file.LogFile, error) {
	m.filesLock.RLock()
	lf, ok := m.files[fid]
	m.filesLock.RUnlock()
	if !ok {
		return nil, pkgerrors.Errorf("value log file %d not found", fid)
	}
	return lf, nil
}

func (m *Manager) Remove(fid uint32) error {
	m.filesLock.Lock()
	lf, ok := m.files[fid]
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
				if size := m.active.Size(); size >= 0 {
					m.offset = uint32(size)
				}
			}
		}
	}
	m.filesLock.Unlock()

	lf.BeginClose()
	if lf.IsSealed() {
		lf.WaitForNoPins()
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	if err := lf.Close(); err != nil {
		return err
	}
	return os.Remove(lf.FileName())
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
	}
}

// SyncActive fsyncs the current active value log segment.
// It is primarily used in tests; the write path syncs all touched segments via SyncFIDs.
func (m *Manager) SyncActive() error {
	if m == nil {
		return nil
	}
	m.filesLock.RLock()
	lf := m.active
	fid := m.activeID
	m.filesLock.RUnlock()
	if lf == nil {
		return nil
	}
	if err := m.runBeforeSyncHook(fid); err != nil {
		return err
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	return lf.Sync()
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
		lf := m.files[fid]
		if lf != nil {
			lf.Lock.Lock()
		}
		m.filesLock.RUnlock()

		if lf == nil {
			continue
		}
		if err := m.runBeforeSyncHook(fid); err != nil {
			lf.Lock.Unlock()
			return err
		}
		err := lf.Sync()
		lf.Lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// SegmentSize reports the current size of the segment identified by fid.
func (m *Manager) SegmentSize(fid uint32) (int64, error) {
	lf, err := m.getFile(fid)
	if err != nil {
		return 0, err
	}
	return lf.Size(), nil
}

// SegmentInit refreshes the mmap metadata for the specified segment.
func (m *Manager) SegmentInit(fid uint32) error {
	lf, err := m.getFile(fid)
	if err != nil {
		return err
	}
	return lf.Init()
}

// SegmentBootstrap rewrites the header of the provided segment, resetting its
// logical contents. It is typically used when truncation shrinks a file below
// the header size and the segment needs to be treated as empty.
func (m *Manager) SegmentBootstrap(fid uint32) error {
	lf, err := m.getFile(fid)
	if err != nil {
		return err
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	return lf.Bootstrap()
}

// SegmentTruncate shrinks the segment to the provided offset.
func (m *Manager) SegmentTruncate(fid uint32, offset uint32) error {
	lf, err := m.getFile(fid)
	if err != nil {
		return err
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	return lf.Truncate(int64(offset))
}

// Rewind rolls back the active head to the provided pointer, truncating any bytes
// beyond it and removing files created after the pointer's file. It is primarily
// used to recover from value log write failures so that partially written
// batches don't leave garbage in the log.
func (m *Manager) Rewind(ptr kv.ValuePtr) error {
	var (
		extra []struct {
			lf   *file.LogFile
			name string
		}
		active *file.LogFile
	)

	m.filesLock.Lock()
	for fid, lf := range m.files {
		if fid > ptr.Fid {
			extra = append(extra, struct {
				lf   *file.LogFile
				name string
			}{lf: lf, name: lf.FileName()})
			delete(m.files, fid)
		}
	}
	lf, ok := m.files[ptr.Fid]
	if ok {
		active = lf
		m.active = lf
		m.activeID = ptr.Fid
		m.maxFid = ptr.Fid
		m.offset = ptr.Offset
	}
	m.filesLock.Unlock()

	if !ok {
		return pkgerrors.Errorf("rewind: value log file %d not found", ptr.Fid)
	}

	var firstErr error
	if err := active.SetWritable(); err != nil {
		firstErr = err
	}
	for _, item := range extra {
		item.lf.Lock.Lock()
		if err := item.lf.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		item.lf.Lock.Unlock()
		if err := os.Remove(item.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	active.Lock.Lock()
	if err := active.Truncate(int64(ptr.Offset)); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := active.Init(); err != nil && firstErr == nil {
		firstErr = err
	}
	active.Lock.Unlock()

	return firstErr
}

func (m *Manager) Close() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	var firstErr error
	for fid, lf := range m.files {
		lf.BeginClose()
		if lf.IsSealed() {
			lf.WaitForNoPins()
		}
		if err := lf.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.files, fid)
	}
	m.active = nil
	m.activeID = 0
	m.offset = 0
	return firstErr
}

func (m *Manager) ListFIDs() []uint32 {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	fids := make([]uint32, 0, len(m.files))
	for fid := range m.files {
		fids = append(fids, fid)
	}
	slices.Sort(fids)
	return fids
}
