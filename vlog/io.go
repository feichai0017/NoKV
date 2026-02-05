package vlog

import (
	"bytes"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

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

func (m *Manager) appendPayload(payload []byte) (*kv.ValuePtr, error) {
	store, fid, start, err := m.reserve(len(payload))
	if err != nil {
		return nil, err
	}
	store.Lock.Lock()
	err = store.Write(start, payload)
	store.Lock.Unlock()
	if err != nil {
		return nil, err
	}
	return &kv.ValuePtr{Fid: fid, Offset: start, Len: uint32(len(payload)), Bucket: m.bucket}, nil
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

// reserve allocates space in the active segment, rotating if needed.
func (m *Manager) reserve(sz int) (*file.LogFile, uint32, uint32, error) {
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

	store, fid, start, err := m.reserve(total)
	if err != nil {
		releaseBuffers()
		return nil, err
	}

	offset := start
	store.Lock.Lock()
	for i, payload := range payloads {
		if payload == nil {
			continue
		}
		if err := store.Write(offset, payload); err != nil {
			store.Lock.Unlock()
			releaseBuffers()
			return nil, err
		}
		ptrs[i] = kv.ValuePtr{
			Fid:    fid,
			Offset: offset,
			Len:    uint32(len(payload)),
			Bucket: m.bucket,
		}
		offset += uint32(len(payload))
	}
	store.Lock.Unlock()

	releaseBuffers()
	return ptrs, nil
}

func (m *Manager) Read(ptr *kv.ValuePtr) ([]byte, func(), error) {
	store, unlock, err := m.getStoreForRead(ptr.Fid)
	if err != nil {
		if unlock != nil {
			unlock()
		}
		return nil, nil, err
	}
	buf, err := store.Read(ptr)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return buf, unlock, nil
}

type ReadMode uint8

const (
	ReadModeZeroCopy ReadMode = iota
	ReadModeCopy
	ReadModeAuto
)

// ReadOptions defines how values are materialized from the value log.
type ReadOptions struct {
	Mode                ReadMode
	SmallValueThreshold int
}

// ReadValue decodes the value payload and optionally copies it based on the read mode.
func (m *Manager) ReadValue(ptr *kv.ValuePtr, opt ReadOptions) ([]byte, func(), error) {
	raw, unlock, err := m.Read(ptr)
	if err != nil {
		return nil, unlock, err
	}
	val, _, err := kv.DecodeValueSlice(raw)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	switch opt.Mode {
	case ReadModeCopy:
		copied := kv.SafeCopy(nil, val)
		unlock()
		return copied, nil, nil
	case ReadModeAuto:
		if opt.SmallValueThreshold > 0 && len(val) <= opt.SmallValueThreshold {
			copied := kv.SafeCopy(nil, val)
			unlock()
			return copied, nil, nil
		}
	}
	return val, unlock, nil
}

// Iterate streams value-log records from the given file identifier starting at
// the provided offset, invoking fn for each decoded entry. It returns the last
// known-good offset (suitable for truncation) when the iteration completes or
// stops early.
func (m *Manager) Iterate(fid uint32, offset uint32, fn kv.LogEntry) (uint32, error) {
	if fn == nil {
		return offset, fmt.Errorf("vlog manager iterate: nil callback")
	}
	store, err := m.getFile(fid)
	if err != nil {
		return 0, err
	}
	return iterateLogFile(store, m.bucket, fid, offset, fn)
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
	store, err := m.getFile(fid)
	if err != nil {
		return nil, err
	}

	size := store.Size()

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
	_, err = iterateLogFile(store, m.bucket, fid, 0, func(e *kv.Entry, vp *kv.ValuePtr) error {
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
		fid := uint32(extractFID(path))
		store, err := openLogFile(fid, path, cfg.Dir, cfg.MaxSize, false)
		if err != nil {
			if stderrors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		valid, err := sanitizeValueLog(store)
		closeErr := store.Close()
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

func sanitizeValueLog(store *file.LogFile) (uint32, error) {
	start, err := firstNonZeroOffset(store)
	if err != nil {
		return 0, err
	}
	if _, err := store.Seek(int64(start), io.SeekStart); err != nil {
		return 0, err
	}
	eIter := kv.NewEntryIterator(store.FD())
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

func firstNonZeroOffset(store *file.LogFile) (uint32, error) {
	size := store.Size()
	start := int64(kv.ValueLogHeaderSize)
	if size <= start {
		return uint32(start), nil
	}
	buf := make([]byte, 1<<20)
	fd := store.FD()
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

func iterateLogFile(store *file.LogFile, bucket uint32, fid uint32, offset uint32, fn kv.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = uint32(kv.ValueLogHeaderSize)
	}
	if int64(offset) == store.Size() {
		return offset, nil
	}

	if _, err := store.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, pkgerrors.Wrapf(err, "value log iterate seek: %s", store.FileName())
	}

	stream := kv.NewEntryIterator(store.FD())
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
			Fid:    fid,
			Bucket: bucket,
		}
		validEndOffset = currentOffset + recordLen
		currentOffset = validEndOffset

		callErr := fn(entry, &vp)
		if callErr != nil {
			if callErr == utils.ErrStop {
				return validEndOffset, nil
			}
			return 0, utils.WarpErr(fmt.Sprintf("Iteration function %s", store.FileName()), callErr)
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
