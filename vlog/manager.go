package vlog

import (
	"slices"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
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
}

// SetTestingHooks installs testing callbacks on the manager. It is intended for
// use in tests only and should not be used by production code.
func (m *Manager) SetTestingHooks(h ManagerTestingHooks) {
		m.filesLock.Lock()
		defer m.filesLock.Unlock()
		m.hooks = h
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
	if len(mgr.files) == 0 {
		lf, err := mgr.create(0)
		if err != nil {
			return nil, err
		}
		mgr.active = lf
		mgr.activeID = 0
	} else {
		mgr.activeID = mgr.maxFid
		mgr.active = mgr.files[mgr.activeID]
	}
	if mgr.active != nil {
		off, err := mgr.active.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		mgr.offset = uint32(off)
	}
	return mgr, nil
}

func (m *Manager) populate() error {
	files, err := filepath.Glob(filepath.Join(m.cfg.Dir, "*.vlog"))
	if err != nil {
		return err
	}
	sort.Strings(files)
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
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(m.cfg.MaxSize),
		}); err != nil {
			return err
		}
		m.files[fid] = lf
		if fid > m.maxFid {
			m.maxFid = fid
		}
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
	lf.Bootstrap()
	m.files[fid] = lf
	if fid > m.maxFid {
		m.maxFid = fid
	}
	return lf, nil
}

func (m *Manager) Append(data []byte) (*utils.ValuePtr, error) {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()

	if hook := m.hooks.BeforeAppend; hook != nil {
		if err := hook(m, data); err != nil {
			return nil, err
		}
	}

	if m.active == nil {
		if _, err := m.create(m.maxFid + 1); err != nil {
			return nil, err
		}
		m.active = m.files[m.maxFid]
		m.activeID = m.maxFid
		m.offset = 0
	}

	off := m.offset
	if err := m.active.Write(off, data); err != nil {
		return nil, err
	}
	m.offset += uint32(len(data))
	m.active.AddSize(m.offset)
	return &utils.ValuePtr{Fid: m.activeID, Offset: off, Len: uint32(len(data))}, nil
}

func (m *Manager) Rotate() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()

	if hook := m.hooks.BeforeRotate; hook != nil {
		if err := hook(m); err != nil {
			return err
		}
	}
	if m.active != nil {
		if err := m.active.DoneWriting(m.offset); err != nil {
			return err
		}
	}
	nextID := m.maxFid + 1
	if _, err := m.create(nextID); err != nil {
		return err
	}
	m.active = m.files[nextID]
	m.activeID = nextID
	m.offset = 0
	return nil
}

func (m *Manager) Read(ptr *utils.ValuePtr) ([]byte, func(), error) {
	lf, unlock, err := m.getFileRLocked(ptr.Fid)
	if err != nil {
		if unlock != nil {
			unlock()
		}
		return nil, nil, err
	}
	buf, err := lf.Read(ptr)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	data := make([]byte, len(buf))
	copy(data, buf)
	return data, unlock, nil
}

func (m *Manager) getFileRLocked(fid uint32) (*file.LogFile, func(), error) {
	m.filesLock.RLock()
	lf, ok := m.files[fid]
	if !ok {
		m.filesLock.RUnlock()
		return nil, nil, errors.Errorf("value log file %d not found", fid)
	}
	lf.Lock.RLock()
	m.filesLock.RUnlock()
	return lf, lf.Lock.RUnlock, nil
}

func (m *Manager) Remove(fid uint32) error {
	m.filesLock.Lock()
	lf, ok := m.files[fid]
	if ok {
		delete(m.files, fid)
	}
	m.filesLock.Unlock()
	if !ok {
		return nil
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	if err := lf.Close(); err != nil {
		return err
	}
	return os.Remove(lf.FileName())
}

func (m *Manager) MaxFID() uint32 {
	return atomic.LoadUint32(&m.maxFid)
}

func (m *Manager) ActiveFID() uint32 {
	return atomic.LoadUint32(&m.activeID)
}

func (m *Manager) Head() utils.ValuePtr {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return utils.ValuePtr{
		Fid:    m.activeID,
		Offset: m.offset,
	}
}

// Rewind rolls back the active head to the provided pointer, truncating any bytes
// beyond it and removing files created after the pointer's file. It is primarily
// used to recover from value log write failures so that partially written
// batches don't leave garbage in the log.
func (m *Manager) Rewind(ptr utils.ValuePtr) error {
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
		return errors.Errorf("rewind: value log file %d not found", ptr.Fid)
	}

	var firstErr error
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
	active.AddSize(ptr.Offset)
	active.Lock.Unlock()

	return firstErr
}

func (m *Manager) Close() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	var firstErr error
	for fid, lf := range m.files {
		if err := lf.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.files, fid)
	}
	m.active = nil
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

func (m *Manager) LogFile(fid uint32) (*file.LogFile, bool) {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	lf, ok := m.files[fid]
	return lf, ok
}

func EncodeHead(fid, offset uint32) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], fid)
	binary.BigEndian.PutUint32(buf[4:], offset)
	return buf
}

func DecodeHead(data []byte) (uint32, uint32) {
	if len(data) < 8 {
		return 0, 0
	}
	fid := binary.BigEndian.Uint32(data[:4])
	offset := binary.BigEndian.Uint32(data[4:])
	return fid, offset
}
