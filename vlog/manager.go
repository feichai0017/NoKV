package vlog

import (
	"bytes"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand"
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

const entryBufferMaxReuse = 1 << 20

var entryBufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func getEntryBuffer() *bytes.Buffer {
	return entryBufferPool.Get().(*bytes.Buffer)
}

func putEntryBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > entryBufferMaxReuse {
		return
	}
	buf.Reset()
	entryBufferPool.Put(buf)
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

func (m *Manager) appendPayload(payload []byte) (*kv.ValuePtr, error) {
	lf, fid, start, err := m.reserveAppend(len(payload))
	if err != nil {
		return nil, err
	}
	lf.Lock.Lock()
	err = lf.Write(start, payload)
	lf.Lock.Unlock()
	if err != nil {
		return nil, err
	}
	return &kv.ValuePtr{Fid: fid, Offset: start, Len: uint32(len(payload))}, nil
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

// AppendEntry encodes and appends the provided entry directly into the active value log.
func (m *Manager) AppendEntry(e *kv.Entry) (*kv.ValuePtr, error) {
	if e == nil {
		return nil, fmt.Errorf("vlog manager: nil entry")
	}
	buf := getEntryBuffer()
	payload, err := kv.EncodeEntry(buf, e)
	if err != nil {
		putEntryBuffer(buf)
		return nil, err
	}
	if err := m.runBeforeAppendHook(payload); err != nil {
		putEntryBuffer(buf)
		return nil, err
	}
	ptr, err := m.appendPayload(payload)
	putEntryBuffer(buf)
	if err != nil {
		return nil, err
	}
	return ptr, nil
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

func (m *Manager) reserveAppend(sz int) (*file.LogFile, uint32, uint32, error) {
	if sz <= 0 {
		return nil, 0, 0, fmt.Errorf("vlog manager: invalid append size %d", sz)
	}
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	lf, fid, err := m.ensureActiveLocked()
	if err != nil {
		return nil, 0, 0, err
	}
	if m.offset < uint32(kv.ValueLogHeaderSize) {
		m.offset = uint32(kv.ValueLogHeaderSize)
	}
	if int(m.offset)+sz > int(m.cfg.MaxSize) {
		if err := m.rotateLocked(); err != nil {
			return nil, 0, 0, err
		}
		lf, fid, err = m.ensureActiveLocked()
		if err != nil {
			return nil, 0, 0, err
		}
	}
	start := m.offset
	m.offset += uint32(sz)
	return lf, fid, start, nil
}

func (m *Manager) reserveBatch(sz int) (*file.LogFile, uint32, uint32, error) {
	if sz <= 0 {
		return nil, 0, 0, fmt.Errorf("vlog manager: invalid append size %d", sz)
	}
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	lf, fid, err := m.ensureActiveLocked()
	if err != nil {
		return nil, 0, 0, err
	}
	if m.offset < uint32(kv.ValueLogHeaderSize) {
		m.offset = uint32(kv.ValueLogHeaderSize)
	}
	if int(m.offset)+sz > int(m.cfg.MaxSize) {
		if err := m.rotateLocked(); err != nil {
			return nil, 0, 0, err
		}
		lf, fid, err = m.ensureActiveLocked()
		if err != nil {
			return nil, 0, 0, err
		}
	}
	start := m.offset
	m.offset += uint32(sz)
	return lf, fid, start, nil
}

// AppendEntries encodes and appends a batch of entries into the value log.
// The writeMask (when provided) selects which entries are written; skipped
// entries receive zero-value pointers in the result.
func (m *Manager) AppendEntries(entries []*kv.Entry, writeMask []bool) ([]kv.ValuePtr, error) {
	ptrs := make([]kv.ValuePtr, len(entries))
	if len(entries) == 0 {
		return ptrs, nil
	}
	if writeMask != nil && len(writeMask) != len(entries) {
		return nil, fmt.Errorf("vlog manager: write mask size mismatch")
	}

	payloads := make([][]byte, len(entries))
	buffers := make([]*bytes.Buffer, 0, len(entries))
	total := 0
	releaseBuffers := func() {
		for _, b := range buffers {
			putEntryBuffer(b)
		}
	}

	for i, e := range entries {
		write := true
		if writeMask != nil {
			write = writeMask[i]
		}
		if !write {
			continue
		}
		if e == nil {
			releaseBuffers()
			return nil, fmt.Errorf("vlog manager: nil entry")
		}
		buf := getEntryBuffer()
		payload, err := kv.EncodeEntry(buf, e)
		if err != nil {
			putEntryBuffer(buf)
			releaseBuffers()
			return nil, err
		}
		if err := m.runBeforeAppendHook(payload); err != nil {
			putEntryBuffer(buf)
			releaseBuffers()
			return nil, err
		}
		payloads[i] = payload
		buffers = append(buffers, buf)
		total += len(payload)
	}

	if total == 0 {
		releaseBuffers()
		return ptrs, nil
	}

	if m.cfg.MaxSize > 0 && int64(total) > m.cfg.MaxSize {
		for i, payload := range payloads {
			if payload == nil {
				continue
			}
			ptr, err := m.appendPayload(payload)
			if err != nil {
				releaseBuffers()
				return nil, err
			}
			ptrs[i] = *ptr
		}
		releaseBuffers()
		return ptrs, nil
	}

	lf, fid, start, err := m.reserveBatch(total)
	if err != nil {
		releaseBuffers()
		return nil, err
	}

	offset := start
	lf.Lock.Lock()
	for i, payload := range payloads {
		if payload == nil {
			continue
		}
		if err := lf.Write(offset, payload); err != nil {
			lf.Lock.Unlock()
			releaseBuffers()
			return nil, err
		}
		ptrs[i] = kv.ValuePtr{
			Fid:    fid,
			Offset: offset,
			Len:    uint32(len(payload)),
		}
		offset += uint32(len(payload))
	}
	lf.Lock.Unlock()

	releaseBuffers()
	return ptrs, nil
}

func (m *Manager) Read(ptr *kv.ValuePtr) ([]byte, func(), error) {
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
	return buf, unlock, nil
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

// Iterate streams value-log records from the given file identifier starting at
// the provided offset, invoking fn for each decoded entry. It returns the last
// known-good offset (suitable for truncation) when the iteration completes or
// stops early.
func (m *Manager) Iterate(fid uint32, offset uint32, fn kv.LogEntry) (uint32, error) {
	if fn == nil {
		return offset, fmt.Errorf("vlog manager iterate: nil callback")
	}
	lf, err := m.getFile(fid)
	if err != nil {
		return 0, err
	}
	return iterateLogFile(lf, offset, fn)
}

// SampleOptions controls value-log sampling behaviour.
type SampleOptions struct {
	SizeRatio     float64
	CountRatio    float64
	FromBeginning bool
	MaxEntries    uint32
}

// SampleStats captures sampling totals.
type SampleStats struct {
	TotalMiB    float64
	DiscardMiB  float64
	Count       int
	SkippedMiB  float64
	SizeWindow  float64
	CountWindow int
}

// SampleCallback determines whether an entry contributes to the discard total.
type SampleCallback func(e *kv.Entry, vp *kv.ValuePtr) (bool, error)

// Sample iterates over a subset of entries in the given segment using the
// provided options, returning aggregate statistics and invoking the callback for
// each sampled entry. The callback's boolean return value indicates whether the
// entry should count towards the discard total.
func (m *Manager) Sample(fid uint32, opt SampleOptions, cb SampleCallback) (*SampleStats, error) {
	if cb == nil {
		return nil, fmt.Errorf("vlog manager sample: nil callback")
	}
	lf, err := m.getFile(fid)
	if err != nil {
		return nil, err
	}

	size := lf.Size()

	stats := &SampleStats{}

	var sizeLimit float64
	if opt.SizeRatio > 0 {
		sizeLimit = (float64(size) / float64(utils.Mi)) * opt.SizeRatio
		stats.SizeWindow = sizeLimit
	}

	var countLimit int
	if opt.CountRatio > 0 && opt.MaxEntries > 0 {
		countLimit = int(float64(opt.MaxEntries) * opt.CountRatio)
		if countLimit > 0 {
			stats.CountWindow = countLimit
		}
	}

	var skipMiB float64
	if !opt.FromBeginning && size > 0 {
		skipBytes := float64(rand.Int63n(size))
		skipBytes -= float64(size) * opt.SizeRatio
		if skipBytes < 0 {
			skipBytes = 0
		}
		skipMiB = skipBytes / float64(utils.Mi)
	}

	var skipped float64
	_, err = iterateLogFile(lf, 0, func(e *kv.Entry, vp *kv.ValuePtr) error {
		esz := float64(vp.Len) / float64(utils.Mi)

		if skipped < skipMiB {
			skipped += esz
			return nil
		}

		if countLimit > 0 && stats.Count >= countLimit {
			return utils.ErrStop
		}
		if sizeLimit > 0 && stats.TotalMiB >= sizeLimit {
			return utils.ErrStop
		}

		stats.TotalMiB += esz
		stats.Count++

		discard, err := cb(e, vp)
		if err != nil {
			return err
		}
		if discard {
			stats.DiscardMiB += esz
		}
		return nil
	})

	stats.SkippedMiB = skipped

	switch err {
	case nil, utils.ErrStop:
		return stats, nil
	default:
		return nil, err
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
				_ = utils.Err(fmt.Errorf("value log verify truncate %s: %w", path, err))
			}
		}
	}
	return nil
}

func extractFID(path string) uint64 {
	var fid uint64
	if _, err := fmt.Sscanf(filepath.Base(path), "%05d.vlog", &fid); err != nil {
		_ = utils.Err(err)
		return 0
	}
	return fid
}

func sanitizeValueLog(lf *file.LogFile) (uint32, error) {
	start, err := firstNonZeroOffset(lf)
	if err != nil {
		return 0, err
	}
	if _, err := lf.Seek(int64(start), io.SeekStart); err != nil {
		return 0, err
	}
	eIter := kv.NewEntryIterator(lf.FD())
	defer eIter.Close()

	offset := start
	validEnd := offset
	for eIter.Next() {
		recordLen := eIter.RecordLen()
		validEnd = offset + recordLen
		offset = validEnd
	}

	switch err := eIter.Err(); err {
	case nil, io.EOF:
		return validEnd, nil
	case kv.ErrPartialEntry, kv.ErrBadChecksum:
		return validEnd, utils.ErrTruncate
	default:
		return validEnd, err
	}
}

func firstNonZeroOffset(lf *file.LogFile) (uint32, error) {
	size := lf.Size()
	start := int64(kv.ValueLogHeaderSize)
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
			for i := range n {
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

func iterateLogFile(lf *file.LogFile, offset uint32, fn kv.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = uint32(kv.ValueLogHeaderSize)
	}
	if int64(offset) == lf.Size() {
		return offset, nil
	}

	if _, err := lf.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, pkgerrors.Wrapf(err, "value log iterate seek: %s", lf.FileName())
	}

	stream := kv.NewEntryIterator(lf.FD())
	defer stream.Close()

	validEndOffset := offset
	currentOffset := offset

	for stream.Next() {
		entry := stream.Entry()
		recordLen := stream.RecordLen()
		entry.Offset = currentOffset

		vp := kv.ValuePtr{
			Len:    recordLen,
			Offset: entry.Offset,
			Fid:    lf.FID,
		}
		validEndOffset = currentOffset + recordLen
		currentOffset = validEndOffset

		callErr := fn(entry, &vp)
		if callErr != nil {
			if callErr == utils.ErrStop {
				return validEndOffset, nil
			}
			return 0, utils.WarpErr(fmt.Sprintf("Iteration function %s", lf.FileName()), callErr)
		}
	}

	switch err := stream.Err(); err {
	case nil, io.EOF:
		return validEndOffset, nil
	case kv.ErrPartialEntry, kv.ErrBadChecksum:
		return validEndOffset, nil
	default:
		return 0, err
	}
}
