// Package vlog implements the value-log Authoritative consumer on top of
// the engine/slab substrate. It owns ValuePtr encoding, bucket routing,
// the logical write cursor, GC sample / discard stats, and rewind on
// write failure. The mmap segment substrate (open/read/write/seal/
// rotate/remove/close) lives in engine/slab.
//
// See docs/notes/2026-04-27-slab-substrate.md for the layered design.
package vlog

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/slab"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

const defaultMaxSize int64 = 1 << 29

type Config struct {
	Dir      string
	FileMode os.FileMode
	MaxSize  int64
	Bucket   uint32
	FS       vfs.FS
}

func (cfg Config) resolveOpenConfig() (Config, error) {
	if cfg.Dir == "" {
		return Config{}, fmt.Errorf("vlog manager: dir required")
	}
	cfg.FS = vfs.Ensure(cfg.FS)
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultMaxSize
	}
	return cfg, nil
}

// Manager is the vlog Authoritative consumer of slab.Manager. It owns
// the bucket id, the logical write cursor (offset within the active
// segment), and value-log-specific operations (AppendEntries, Read,
// ReadValue, Sample, Iterate, Rewind). All physical segment lifecycle
// (rotation, sealed pin, remove, close) is delegated to inner.
type Manager struct {
	cfg    Config
	bucket uint32
	inner  *slab.Manager
	// offset is the logical write cursor in the active segment. Accessed
	// from reserve() under inner.WithLock and from Head/Rotate/Remove
	// outside that lock; atomic so concurrent stats / metric collectors
	// never observe a torn read.
	offset atomic.Uint32
}

func Open(cfg Config) (*Manager, error) {
	cfg, err := cfg.resolveOpenConfig()
	if err != nil {
		return nil, err
	}
	inner, err := slab.Open(slab.Config{
		Dir:        cfg.Dir,
		FileSuffix: vlogFileSuffix,
		FileMode:   cfg.FileMode,
		MaxSize:    cfg.MaxSize,
		HeaderSize: kv.ValueLogHeaderSize,
		FS:         cfg.FS,
	})
	if err != nil {
		return nil, err
	}
	mgr := &Manager{
		cfg:    cfg,
		bucket: cfg.Bucket,
		inner:  inner,
	}
	// Logical write cursor: fresh manager opens at the header; reload
	// inherits the active segment's persisted high-water (which slab
	// populated via LoadSizeFromFile).
	if active, _, ok := inner.Active(); ok {
		if size := active.Size(); size > int64(kv.ValueLogHeaderSize) {
			mgr.offset.Store(uint32(size))
		} else {
			mgr.offset.Store(uint32(kv.ValueLogHeaderSize))
		}
	}
	return mgr, nil
}

const vlogFileSuffix = ".vlog"

func (m *Manager) SetMaxSize(maxSize int64) { m.inner.SetMaxSize(maxSize) }

func (m *Manager) MaxFID() uint32    { return m.inner.MaxFID() }
func (m *Manager) ActiveFID() uint32 { return m.inner.ActiveFID() }

func (m *Manager) Head() kv.ValuePtr {
	return kv.ValuePtr{
		Fid:    m.inner.ActiveFID(),
		Offset: m.offset.Load(),
		Bucket: m.bucket,
	}
}

func (m *Manager) SyncActive() error            { return m.inner.SyncActive() }
func (m *Manager) SyncFIDs(fids []uint32) error { return m.inner.SyncFIDs(fids) }

func (m *Manager) SegmentSize(fid uint32) (int64, error) { return m.inner.SegmentSize(fid) }
func (m *Manager) SegmentInit(fid uint32) error          { return m.inner.SegmentInit(fid) }
func (m *Manager) SegmentBootstrap(fid uint32) error     { return m.inner.SegmentBootstrap(fid) }
func (m *Manager) SegmentTruncate(fid uint32, offset uint32) error {
	return m.inner.SegmentTruncate(fid, offset)
}

func (m *Manager) ListFIDs() []uint32 { return m.inner.ListFIDs() }

func (m *Manager) Rotate() error {
	_, _, err := m.inner.Rotate(m.offset.Load())
	if err != nil {
		return err
	}
	m.offset.Store(uint32(kv.ValueLogHeaderSize))
	return nil
}

func (m *Manager) Remove(fid uint32) error {
	return m.inner.Remove(fid, func(active *slab.Segment) {
		if active == nil {
			m.offset.Store(0)
			return
		}
		if size := active.Size(); size >= 0 {
			m.offset.Store(uint32(size))
		}
	})
}

// Rewind rolls the active head back to ptr, dropping any segments and
// bytes after it. Used by AppendEntries on write failure.
func (m *Manager) Rewind(ptr kv.ValuePtr) error {
	if ptr.Bucket != m.bucket {
		return pkgerrors.Errorf("rewind: bucket mismatch: want %d got %d", m.bucket, ptr.Bucket)
	}
	return m.inner.Rewind(slab.RewindSpec{Fid: ptr.Fid, Offset: ptr.Offset}, func(_ *slab.Segment, off uint32) {
		m.offset.Store(off)
	})
}

func (m *Manager) Close() error { return m.inner.Close() }
