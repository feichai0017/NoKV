package vlog

import (
	"bufio"
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/file"
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
		return nil, nil, pkgerrors.Errorf("value log file %d not found", fid)
	}
	lf.Lock.RLock()
	m.filesLock.RUnlock()
	return lf, lf.Lock.RUnlock, nil
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
	atomic.StoreUint32(&m.maxFid, maxID)

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

// VerifyDir scans all value log segments and truncates any partially written
// records left behind due to crashes. It validates checksums to ensure future
// replays operate on consistent data.
func VerifyDir(cfg Config) error {
	if cfg.Dir == "" {
		return fmt.Errorf("vlog verify: dir required")
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = int64(1 << 29)
	}
	files, err := filepath.Glob(filepath.Join(cfg.Dir, "*.vlog"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, path := range files {
		lf := &file.LogFile{}
		if err := lf.Open(&file.Options{
			FID:      extractFID(path),
			FileName: path,
			Dir:      cfg.Dir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(cfg.MaxSize),
		}); err != nil {
			if stderrors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		valid, err := sanitizeValueLog(lf)
		closeErr := lf.Close()
		if err != nil && !stderrors.Is(err, utils.ErrTruncate) {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return statErr
		}
		if int64(valid) < info.Size() {
			if err := os.Truncate(path, int64(valid)); err != nil {
				utils.Err(fmt.Errorf("value log verify truncate %s: %w", path, err))
			}
		}
	}
	return nil
}

func extractFID(path string) uint64 {
	var fid uint64
	fmt.Sscanf(filepath.Base(path), "%05d.vlog", &fid)
	return fid
}

func sanitizeValueLog(lf *file.LogFile) (uint32, error) {
	if lf == nil {
		return 0, fmt.Errorf("sanitize value log: nil log file")
	}
	start, err := firstNonZeroOffset(lf)
	if err != nil {
		return 0, err
	}
	if _, err := lf.Seek(int64(start), io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(lf.FD())
	offset := start
	validEnd := offset
	for {
		entryOffset := offset
		tee := utils.NewHashReader(reader)
		headerBytes := 0
		readVarint := func() (uint64, error) {
			val, err := binary.ReadUvarint(tee)
			headerBytes = tee.BytesRead
			return val, err
		}
		klen, err := readVarint()
		if err != nil {
			if err == io.EOF {
				return validEnd, nil
			}
			if err == io.ErrUnexpectedEOF {
				return validEnd, nil
			}
			return 0, err
		}
		vlen, err := readVarint()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return validEnd, nil
			}
			return 0, err
		}
		if klen > uint64(1<<32) || vlen > uint64(1<<32) {
			return validEnd, utils.ErrTruncate
		}
		if _, err := readVarint(); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return validEnd, nil
			}
			return 0, err
		}
		if _, err := readVarint(); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return validEnd, nil
			}
			return 0, err
		}
		key := make([]byte, int(klen))
		if _, err := io.ReadFull(tee, key); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return validEnd, utils.ErrTruncate
			}
			return 0, err
		}
		val := make([]byte, int(vlen))
		if _, err := io.ReadFull(tee, val); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return validEnd, utils.ErrTruncate
			}
			return 0, err
		}
		var crcBuf [crc32.Size]byte
		if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return validEnd, utils.ErrTruncate
			}
			return 0, err
		}
		expected := utils.BytesToU32(crcBuf[:])
		if expected != tee.Sum32() {
			return validEnd, utils.ErrTruncate
		}
		recordLen := uint32(headerBytes) + uint32(len(key)) + uint32(len(val)) + crc32.Size
		validEnd = entryOffset + recordLen
		offset = validEnd
	}
}

func firstNonZeroOffset(lf *file.LogFile) (uint32, error) {
	size := lf.Size()
	start := int64(utils.VlogHeaderSize)
	if size <= start {
		return uint32(start), nil
	}
	buf := make([]byte, 1<<20)
	fd := lf.FD()
	for offset := start; offset < size; {
		toRead := len(buf)
		if rem := size - offset; rem < int64(toRead) {
			toRead = int(rem)
		}
		n, err := fd.ReadAt(buf[:toRead], offset)
		if n > 0 {
			for i := 0; i < n; i++ {
				if buf[i] != 0 {
					return uint32(offset) + uint32(i), nil
				}
			}
			offset += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return uint32(start), err
		}
	}
	return uint32(start), nil
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
		return pkgerrors.Errorf("rewind: value log file %d not found", ptr.Fid)
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
