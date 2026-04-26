package negativecache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/feichai0017/NoKV/engine/file"
	"github.com/feichai0017/NoKV/engine/slab"
)

// Wire format for the slab snapshot. See snapshot-substrate note §6.1
// (Negative Slab) for the rationale.
//
//	magic     uint32 ("NCSL", little-endian)
//	version   uint16
//	entries   repeated of:
//	    len     uvarint   (length of full key in bytes)
//	    key     [len]byte
//	terminator: zero-length record (uvarint 0) followed by a uint32 LE
//	            CRC32 of the framed body (header + every {len,key} pair
//	            up to and including the zero-length terminator's len byte).
//
// Reload tolerates partial trailing records and CRC mismatches by
// returning zero restored entries and forcing a clean re-warm — Derived
// consistency means we'd rather start cold than serve a corrupt cache.
const (
	snapshotMagic   uint32 = 0x4e43534c // "NCSL"
	snapshotVersion uint16 = 1
	snapshotHeader         = 4 + 2 // magic + version
	snapshotFile           = "negative.slab"
)

// PersistConfig opens a Persistence sidecar for a Cache. Persistence is
// optional — callers that want pure in-memory negative caches can use
// New instead of OpenWithPersistence.
type PersistConfig struct {
	// Dir is the directory the slab segment lives in. Must be writable.
	Dir string
	// MaxSize bounds the on-disk snapshot. Zero falls back to 64 MiB.
	// Snapshots stop appending once the limit is hit; the dropped tail
	// re-warms via the Cache's normal Remember path.
	MaxSize int64
	// FS lets tests inject a fault FS; defaults to the OS via slab.
	FS any
}

// Persistence pairs a Cache with its slab-backed snapshot file.
// Snapshot writes the cache to disk; Reload reads it back. Snapshot
// truncates and rewrites — this is a snapshot, not an append-only log.
type Persistence struct {
	dir     string
	maxSize int64
	cache   *Cache
}

// OpenWithPersistence returns a Cache plus a Persistence helper. If a
// snapshot already exists at Dir/negative.slab it is restored into the
// cache before returning, so the caller observes a warm cache from the
// first Has call.
func OpenWithPersistence(cacheCfg Config, persistCfg PersistConfig) (*Cache, *Persistence, error) {
	if persistCfg.Dir == "" {
		return nil, nil, errors.New("negativecache persistence: dir required")
	}
	if persistCfg.MaxSize <= 0 {
		persistCfg.MaxSize = 64 << 20
	}
	cache := New(cacheCfg)
	p := &Persistence{
		dir:     persistCfg.Dir,
		maxSize: persistCfg.MaxSize,
		cache:   cache,
	}
	if _, err := p.Reload(); err != nil {
		return cache, p, err
	}
	return cache, p, nil
}

// Snapshot writes every still-fresh entry from the cache to the
// snapshot file, replacing any prior contents. Returns the number of
// entries written. Truncation past MaxSize silently drops the tail.
func (p *Persistence) Snapshot() (int, error) {
	if p == nil || p.cache == nil {
		return 0, nil
	}
	keys := p.cache.SnapshotKeys()
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return 0, fmt.Errorf("negativecache snapshot dir: %w", err)
	}
	path := filepath.Join(p.dir, snapshotFile)

	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("negativecache snapshot unlink: %w", err)
	}

	var seg slab.Segment
	if err := seg.Open(&file.Options{
		FID:      0,
		FileName: path,
		MaxSz:    int(p.maxSize),
	}); err != nil {
		return 0, fmt.Errorf("negativecache snapshot open: %w", err)
	}
	defer func() { _ = seg.Close() }()

	if err := seg.Truncate(int64(snapshotHeader)); err != nil {
		return 0, fmt.Errorf("negativecache snapshot truncate: %w", err)
	}
	header := make([]byte, snapshotHeader)
	binary.LittleEndian.PutUint32(header[0:4], snapshotMagic)
	binary.LittleEndian.PutUint16(header[4:6], snapshotVersion)
	if err := seg.Write(0, header); err != nil {
		return 0, fmt.Errorf("negativecache snapshot header: %w", err)
	}

	offset := uint32(snapshotHeader)
	hasher := crc32.NewIEEE()
	written := 0
	var lenBuf [binary.MaxVarintLen64]byte
	for _, key := range keys {
		if int64(offset)+int64(len(key))+binary.MaxVarintLen64+5 >= p.maxSize {
			break
		}
		n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
		if err := seg.Write(offset, lenBuf[:n]); err != nil {
			return written, fmt.Errorf("negativecache snapshot len: %w", err)
		}
		hasher.Write(lenBuf[:n])
		offset += uint32(n)
		if err := seg.Write(offset, key); err != nil {
			return written, fmt.Errorf("negativecache snapshot key: %w", err)
		}
		hasher.Write(key)
		offset += uint32(len(key))
		written++
	}
	zero := []byte{0}
	if err := seg.Write(offset, zero); err != nil {
		return written, fmt.Errorf("negativecache snapshot terminator: %w", err)
	}
	hasher.Write(zero)
	offset++
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], hasher.Sum32())
	if err := seg.Write(offset, crcBuf[:]); err != nil {
		return written, fmt.Errorf("negativecache snapshot crc: %w", err)
	}
	offset += 4
	if err := seg.DoneWriting(offset); err != nil {
		return written, fmt.Errorf("negativecache snapshot seal: %w", err)
	}
	return written, nil
}

// Reload reads the snapshot file (if present) and replays Remember for
// each restored full key. Returns the number of restored keys.
// Missing/truncated/corrupt snapshots produce a zero count and a nil
// error — Derived class tolerates loss.
func (p *Persistence) Reload() (int, error) {
	if p == nil || p.cache == nil {
		return 0, nil
	}
	path := filepath.Join(p.dir, snapshotFile)
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if len(body) < snapshotHeader {
		return 0, nil
	}
	if magic := binary.LittleEndian.Uint32(body[0:4]); magic != snapshotMagic {
		return 0, nil
	}
	if ver := binary.LittleEndian.Uint16(body[4:6]); ver != snapshotVersion {
		return 0, nil
	}

	hasher := crc32.NewIEEE()
	cursor := snapshotHeader
	restored := 0
	for cursor < len(body) {
		klen, n := binary.Uvarint(body[cursor:])
		if n <= 0 {
			return restored, nil
		}
		hasher.Write(body[cursor : cursor+n])
		cursor += n
		if klen == 0 {
			if cursor+4 <= len(body) {
				want := binary.LittleEndian.Uint32(body[cursor : cursor+4])
				if want != hasher.Sum32() {
					return 0, nil
				}
			}
			return restored, nil
		}
		end := cursor + int(klen)
		if end > len(body) {
			return restored, nil
		}
		key := make([]byte, klen)
		copy(key, body[cursor:end])
		hasher.Write(body[cursor:end])
		p.cache.Remember(key)
		restored++
		cursor = end
	}
	return restored, nil
}
