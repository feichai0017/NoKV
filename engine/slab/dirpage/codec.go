// Package dirpage implements the DirPageSlab Derived consumer of
// engine/slab. It materializes (mount, parent_inode) directory listings
// as packed pages in a slab so that ReadDirPlus can short-circuit a
// fan-out of N LSM prefix scans + N inode Gets into a single sequential
// page read.
//
// Wire format and consistency model are described in
// docs/notes/2026-04-27-dirpage-slab-rfc.md. Briefly:
//
//   - LSM is authoritative; pages here are best-effort cache (Derived
//     consistency class).
//   - Each materialized directory becomes one or more page records in a
//     slab segment, sequenced by page_no.
//   - Each page carries a frontier (an opaque uint64 supplied by the
//     caller; in fsmeta this is the WatchSubtree event cursor). Lookup
//     succeeds only if every page's frontier matches the caller's
//     supplied current frontier.
//   - Invalidate marks a key so that any future Lookup against the
//     current frontier-set returns a miss until MaterializeAsync writes
//     a new page set with a fresh frontier.
//
// dirpage does not know about fsmeta types. The caller maps its own
// MountID/InodeID/InodeRecord onto (uint32 mount, uint64 parent,
// uint64 inode, []byte attrBlob). This keeps engine/slab/dirpage free
// of upper-layer dependencies.
package dirpage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// dirPageMagic identifies a dirpage record at offset 0 of its on-slab
// frame. The four ASCII bytes are "DPSL" little-endian.
const dirPageMagic uint32 = 0x4c535044

// dirPageVersion is the wire-format version. Bumped only on incompatible
// changes; readers reject mismatched versions.
const dirPageVersion uint16 = 1

// recordHeaderFixed is the size of the magic + version prefix we read
// before the variable-length body to discriminate dirpage records from
// surrounding bytes (matters because slab segments may legitimately hold
// other consumers' records in the future).
const recordHeaderFixed = 4 + 2 // magic + version

// PageKey identifies one cached directory listing. Mount and Parent are
// opaque to dirpage; the consumer (typically fsmeta) maps its own typed
// identifiers to these uint widths. Mount is uint64 — fsmeta's MountID
// is a string, callers hash it (e.g. xxhash.Sum64) so collision
// probability across mounts is negligible (~5e-12 at 10K mounts).
type PageKey struct {
	Mount  uint64
	Parent uint64
}

// Entry is one materialized directory entry. AttrBlob is the consumer-
// chosen serialization of the inode attributes (e.g. fsmeta.InodeRecord
// encoded via fsmeta.EncodeInodeValue) — dirpage never inspects it.
type Entry struct {
	Name     []byte
	Inode    uint64
	AttrBlob []byte
}

// pageHeader is the decoded prefix of a dirpage record (everything before
// the entries).
type pageHeader struct {
	Mount      uint64
	Parent     uint64
	PageNo     uint32
	Frontier   uint64
	EntryCount uint32
}

// estimatePageSize gives the on-disk footprint of a page with the listed
// entries. Used by the splitter to decide when to roll over to the next
// page. Slightly pessimistic (assumes max varint width).
func estimatePageSize(entries []Entry) int {
	const overhead = recordHeaderFixed +
		binary.MaxVarintLen64*3 + // mount + parent + frontier
		binary.MaxVarintLen32*2 + // page_no + entry_count
		4 // crc32
	total := overhead
	for _, e := range entries {
		total += binary.MaxVarintLen64 // name_len
		total += len(e.Name)
		total += binary.MaxVarintLen64 // inode
		total += binary.MaxVarintLen64 // attr_blob_len
		total += len(e.AttrBlob)
	}
	return total
}

// encodePage serializes one page into dst (appending). Returns dst with
// the encoded bytes appended.
func encodePage(dst []byte, hdr pageHeader, entries []Entry) []byte {
	start := len(dst)

	// magic + version
	var fixed [recordHeaderFixed]byte
	binary.LittleEndian.PutUint32(fixed[0:4], dirPageMagic)
	binary.LittleEndian.PutUint16(fixed[4:6], dirPageVersion)
	dst = append(dst, fixed[:]...)

	// header (varint)
	dst = binary.AppendUvarint(dst, hdr.Mount)
	dst = binary.AppendUvarint(dst, hdr.Parent)
	dst = binary.AppendUvarint(dst, uint64(hdr.PageNo))
	dst = binary.AppendUvarint(dst, hdr.Frontier)
	dst = binary.AppendUvarint(dst, uint64(hdr.EntryCount))

	// entries
	for _, e := range entries {
		dst = binary.AppendUvarint(dst, uint64(len(e.Name)))
		dst = append(dst, e.Name...)
		dst = binary.AppendUvarint(dst, e.Inode)
		dst = binary.AppendUvarint(dst, uint64(len(e.AttrBlob)))
		dst = append(dst, e.AttrBlob...)
	}

	// CRC32 over everything from `start` to here
	crc := crc32.ChecksumIEEE(dst[start:])
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	dst = append(dst, crcBuf[:]...)
	return dst
}

var (
	errPageTruncated   = errors.New("dirpage: record truncated")
	errPageBadMagic    = errors.New("dirpage: bad magic")
	errPageBadVersion  = errors.New("dirpage: unsupported version")
	errPageBadChecksum = errors.New("dirpage: checksum mismatch")
)

// decodePage parses one page from buf. Returns the header, decoded
// entries, and the number of bytes consumed (so callers iterating over
// a multi-record buffer can advance their cursor).
func decodePage(buf []byte) (pageHeader, []Entry, int, error) {
	if len(buf) < recordHeaderFixed+4 { // need at least header prefix + crc
		return pageHeader{}, nil, 0, errPageTruncated
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != dirPageMagic {
		return pageHeader{}, nil, 0, errPageBadMagic
	}
	ver := binary.LittleEndian.Uint16(buf[4:6])
	if ver != dirPageVersion {
		return pageHeader{}, nil, 0, errPageBadVersion
	}

	cursor := recordHeaderFixed
	read := func() (uint64, error) {
		v, n := binary.Uvarint(buf[cursor:])
		if n <= 0 {
			return 0, errPageTruncated
		}
		cursor += n
		return v, nil
	}

	mount, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	parent, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	pageNo, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	frontier, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	entryCount, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}

	entries := make([]Entry, 0, entryCount)
	for range entryCount {
		nameLen, err := read()
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		if cursor+int(nameLen) > len(buf) {
			return pageHeader{}, nil, 0, errPageTruncated
		}
		name := make([]byte, nameLen)
		copy(name, buf[cursor:cursor+int(nameLen)])
		cursor += int(nameLen)

		inode, err := read()
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		blobLen, err := read()
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		if cursor+int(blobLen) > len(buf) {
			return pageHeader{}, nil, 0, errPageTruncated
		}
		blob := make([]byte, blobLen)
		copy(blob, buf[cursor:cursor+int(blobLen)])
		cursor += int(blobLen)

		entries = append(entries, Entry{Name: name, Inode: inode, AttrBlob: blob})
	}

	if cursor+4 > len(buf) {
		return pageHeader{}, nil, 0, errPageTruncated
	}
	want := binary.LittleEndian.Uint32(buf[cursor : cursor+4])
	got := crc32.ChecksumIEEE(buf[:cursor])
	if want != got {
		return pageHeader{}, nil, 0, errPageBadChecksum
	}
	cursor += 4

	hdr := pageHeader{
		Mount:      mount,
		Parent:     parent,
		PageNo:     uint32(pageNo),
		Frontier:   frontier,
		EntryCount: uint32(entryCount),
	}
	return hdr, entries, cursor, nil
}

// splitIntoPages partitions entries into groups, each fitting under
// maxPageBytes when encoded. Empty entries returns one empty page so the
// caller still records "this directory is materialized as empty".
func splitIntoPages(entries []Entry, maxPageBytes int) [][]Entry {
	if len(entries) == 0 {
		return [][]Entry{nil}
	}
	if maxPageBytes <= 0 {
		return [][]Entry{entries}
	}
	var pages [][]Entry
	cur := []Entry{}
	curSize := estimatePageSize(nil)
	for _, e := range entries {
		entrySize := binary.MaxVarintLen64 + len(e.Name) + binary.MaxVarintLen64 + binary.MaxVarintLen64 + len(e.AttrBlob)
		if len(cur) > 0 && curSize+entrySize > maxPageBytes {
			pages = append(pages, cur)
			cur = []Entry{}
			curSize = estimatePageSize(nil)
		}
		cur = append(cur, e)
		curSize += entrySize
	}
	if len(cur) > 0 {
		pages = append(pages, cur)
	}
	return pages
}

// keyString is a debug helper for error messages.
func (k PageKey) keyString() string {
	return fmt.Sprintf("(mount=%d parent=%d)", k.Mount, k.Parent)
}
