package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/feichai0017/NoKV/engine/file"
	"github.com/feichai0017/NoKV/engine/slab"
)

// negativePersistence is the slab-backed sidecar for negativeCache. It writes
// a snapshot of every still-valid internal key on Close and restores them on
// Open by replaying the segment back into the in-memory cache.
//
// This is the first non-ValueLog consumer of engine/slab and is therefore the
// minimum viable test of the substrate: persistence is best-effort (Derived
// consistency class — see docs/notes/2026-04-27-slab-substrate.md §5), so a
// crash that loses the segment forces a re-warm but does not affect read
// correctness. There is no append-on-remember today; that would require a
// background drain to keep the segment from growing unboundedly. Snapshot-on-
// close is enough to validate the substrate; an async-append phase can come
// later if the warmup-after-crash window matters in practice.
//
// Wire format
//   - magic   uint32  ("NCSL", little-endian) — discrimination from junk files
//   - version uint16
//   - entries: repeated of
//       len uvarint  (length of internalKey)
//       key [len]byte
//   - footer: repeated entries terminated by EOF; CRC of the framed body
//             follows when len == 0.
//
// Reload tolerates partial trailing records (returns the keys that decoded
// cleanly up to the truncation point).
const (
	negativeSlabMagic   uint32 = 0x4e43534c // "NCSL"
	negativeSlabVersion uint16 = 1
	negativeSlabHeader         = 6 // magic(4) + version(2)
	negativeSlabFile           = "negative.slab"
)

type negativePersistence struct {
	dir     string
	maxSize int64
}

func newNegativePersistence(dir string, maxSize int64) *negativePersistence {
	if dir == "" {
		return nil
	}
	if maxSize <= 0 {
		maxSize = 64 << 20
	}
	return &negativePersistence{dir: dir, maxSize: maxSize}
}

// Snapshot writes every still-valid internal key from cache into the segment.
// Returns the number of entries written. Truncates any prior content.
func (p *negativePersistence) Snapshot(cache *negativeCache) (int, error) {
	if p == nil || cache == nil {
		return 0, nil
	}
	keys := cache.snapshotKeys()
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return 0, fmt.Errorf("negative slab dir: %w", err)
	}
	path := filepath.Join(p.dir, negativeSlabFile)

	// Always overwrite; this is a snapshot, not a log.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("negative slab unlink: %w", err)
	}

	var seg slab.Segment
	if err := seg.Open(&file.Options{
		FID:      0,
		FileName: path,
		MaxSz:    int(p.maxSize),
	}); err != nil {
		return 0, fmt.Errorf("negative slab open: %w", err)
	}
	defer func() { _ = seg.Close() }()

	if err := seg.Truncate(int64(negativeSlabHeader)); err != nil {
		return 0, fmt.Errorf("negative slab truncate: %w", err)
	}
	header := make([]byte, negativeSlabHeader)
	binary.LittleEndian.PutUint32(header[0:4], negativeSlabMagic)
	binary.LittleEndian.PutUint16(header[4:6], negativeSlabVersion)
	if err := seg.Write(0, header); err != nil {
		return 0, fmt.Errorf("negative slab header: %w", err)
	}

	offset := uint32(negativeSlabHeader)
	hasher := crc32.NewIEEE()
	written := 0
	var lenBuf [binary.MaxVarintLen64]byte
	for _, key := range keys {
		if int64(offset)+int64(len(key))+binary.MaxVarintLen64+5 >= p.maxSize {
			break // run out of room; rest of the keys are dropped this snapshot
		}
		n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
		if err := seg.Write(offset, lenBuf[:n]); err != nil {
			return written, fmt.Errorf("negative slab len: %w", err)
		}
		hasher.Write(lenBuf[:n])
		offset += uint32(n)
		if err := seg.Write(offset, key); err != nil {
			return written, fmt.Errorf("negative slab key: %w", err)
		}
		hasher.Write(key)
		offset += uint32(len(key))
		written++
	}
	// Terminator: zero-length record, then 4-byte CRC.
	zero := []byte{0}
	if err := seg.Write(offset, zero); err != nil {
		return written, fmt.Errorf("negative slab terminator: %w", err)
	}
	offset += 1
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], hasher.Sum32())
	if err := seg.Write(offset, crcBuf[:]); err != nil {
		return written, fmt.Errorf("negative slab crc: %w", err)
	}
	offset += 4
	if err := seg.DoneWriting(offset); err != nil {
		return written, fmt.Errorf("negative slab seal: %w", err)
	}
	return written, nil
}

// Restore reads the snapshot and re-issues remember() for every key. Returns
// the number of restored keys. Missing/truncated/corrupt segments produce a
// zero count and a nil error — Derived consistency means we tolerate loss.
func (p *negativePersistence) Restore(cache *negativeCache) (int, error) {
	if p == nil || cache == nil {
		return 0, nil
	}
	path := filepath.Join(p.dir, negativeSlabFile)
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if len(body) < negativeSlabHeader {
		return 0, nil
	}
	if magic := binary.LittleEndian.Uint32(body[0:4]); magic != negativeSlabMagic {
		return 0, nil // not our file; ignore
	}
	if ver := binary.LittleEndian.Uint16(body[4:6]); ver != negativeSlabVersion {
		return 0, nil // future format we can't read; ignore
	}

	hasher := crc32.NewIEEE()
	cursor := negativeSlabHeader
	restored := 0
	for cursor < len(body) {
		klen, n := binary.Uvarint(body[cursor:])
		if n <= 0 {
			return restored, nil // partial record; stop here
		}
		hasher.Write(body[cursor : cursor+n])
		cursor += n
		if klen == 0 {
			// Terminator. Verify CRC if room remains.
			if cursor+4 <= len(body) {
				want := binary.LittleEndian.Uint32(body[cursor : cursor+4])
				got := hasher.Sum32()
				if want != got {
					// CRC mismatch — drop everything and force re-warm.
					return 0, nil
				}
			}
			return restored, nil
		}
		end := cursor + int(klen)
		if end > len(body) {
			return restored, io.ErrUnexpectedEOF
		}
		key := body[cursor:end]
		hasher.Write(key)
		// Copy because remember holds the slice via append.
		cp := make([]byte, len(key))
		copy(cp, key)
		cache.remember(cp)
		restored++
		cursor = end
	}
	return restored, nil
}
