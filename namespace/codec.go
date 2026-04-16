package namespace

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	truthKeyPrefix       = []byte("M|")
	deltaKeyPrefix       = []byte("LD|")
	pageDeltaPrefix      = []byte("LDP|")
	pageDeltaStatePrefix = []byte("LDS|")
	readRootPrefix       = []byte("LR|")
	readPagePrefix       = []byte("LP|")
	truthValueMagic      = []byte{0xff, 'N', 'T', 1}
)

func encodeTruthKey(path []byte) []byte {
	out := make([]byte, 0, len(truthKeyPrefix)+len(path))
	out = append(out, truthKeyPrefix...)
	out = append(out, path...)
	return out
}

func encodeTruthValue(kind EntryKind, meta []byte) []byte {
	out := make([]byte, 0, len(truthValueMagic)+1+len(meta))
	out = append(out, truthValueMagic...)
	out = append(out, byte(kind))
	out = append(out, meta...)
	return out
}

func decodeTruthValue(raw []byte) (EntryKind, []byte, error) {
	if len(raw) >= len(truthValueMagic)+1 && bytes.Equal(raw[:len(truthValueMagic)], truthValueMagic) {
		return EntryKind(raw[len(truthValueMagic)]), cloneBytes(raw[len(truthValueMagic)+1:]), nil
	}
	// Legacy fallback: older tests and synthetic baselines may still write plain
	// metadata bytes directly into M|path. Interpret those as file entries.
	return EntryKindFile, cloneBytes(raw), nil
}

func encodeReadRootKey(parent []byte) []byte {
	out := make([]byte, 0, len(readRootPrefix)+len(parent))
	out = append(out, readRootPrefix...)
	out = append(out, parent...)
	return out
}

func encodeReadPagePrefix(parent []byte) []byte {
	out := make([]byte, 0, len(readPagePrefix)+len(parent)+1)
	out = append(out, readPagePrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	return out
}

func encodeReadPageKey(parent, fence []byte) []byte {
	out := encodeReadPagePrefix(parent)
	out = append(out, fence...)
	return out
}

func encodeListingDeltaPrefix(parent, pageID []byte) []byte {
	out := make([]byte, 0, len(deltaKeyPrefix)+len(parent)+1+len(pageID)+1)
	out = append(out, deltaKeyPrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	out = append(out, pageID...)
	out = append(out, '|')
	return out
}

func encodeListingDeltaKey(parent, pageID, name []byte) []byte {
	out := encodeListingDeltaPrefix(parent, pageID)
	out = append(out, name...)
	return out
}

func encodeListingDeltaParentPrefix(parent []byte) []byte {
	out := make([]byte, 0, len(deltaKeyPrefix)+len(parent)+1)
	out = append(out, deltaKeyPrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	return out
}

func encodePageDeltaPrefix(parent, pageID []byte) []byte {
	out := make([]byte, 0, len(pageDeltaPrefix)+len(parent)+1+len(pageID)+1)
	out = append(out, pageDeltaPrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	out = append(out, pageID...)
	out = append(out, '|')
	return out
}

func encodePageDeltaLogKey(parent, pageID []byte, seq uint64) []byte {
	out := encodePageDeltaPrefix(parent, pageID)
	out = append(out, '#')
	out = append(out, fmt.Appendf(nil, "%020d", seq)...)
	return out
}

func encodePageDeltaParentPrefix(parent []byte) []byte {
	out := make([]byte, 0, len(pageDeltaPrefix)+len(parent)+1)
	out = append(out, pageDeltaPrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	return out
}

func encodePageDeltaStateKey(parent, pageID []byte) []byte {
	out := make([]byte, 0, len(pageDeltaStatePrefix)+len(parent)+1+len(pageID))
	out = append(out, pageDeltaStatePrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	out = append(out, pageID...)
	return out
}

func encodePageDeltaStateParentPrefix(parent []byte) []byte {
	out := make([]byte, 0, len(pageDeltaStatePrefix)+len(parent)+1)
	out = append(out, pageDeltaStatePrefix...)
	out = append(out, parent...)
	out = append(out, '|')
	return out
}

func decodePageDeltaStatePageIDFromKey(parent, key []byte) ([]byte, error) {
	prefix := encodePageDeltaStateParentPrefix(parent)
	if !bytes.HasPrefix(key, prefix) {
		return nil, ErrParentMismatch
	}
	rest := key[len(prefix):]
	if len(rest) == 0 {
		return nil, ErrCodecCorrupted
	}
	return cloneBytes(rest), nil
}

type pageDeltaState struct {
	Pending bool
	MaxSeq  uint64
}

func encodePageDeltaStateValue(state pageDeltaState) []byte {
	if !state.Pending {
		return nil
	}
	out := make([]byte, 0, 1+binary.MaxVarintLen64)
	out = append(out, 2)
	out = binary.AppendUvarint(out, state.MaxSeq)
	return out
}

func decodePageDeltaStateValue(raw []byte) (pageDeltaState, error) {
	if len(raw) == 0 {
		return pageDeltaState{}, nil
	}
	if len(raw) == 1 && raw[0] == 1 {
		return pageDeltaState{Pending: true}, nil
	}
	if raw[0] != 2 {
		return pageDeltaState{}, ErrCodecCorrupted
	}
	maxSeq, pos, err := consumeUvarint(raw, 1)
	if err != nil {
		return pageDeltaState{}, err
	}
	if pos != len(raw) {
		return pageDeltaState{}, ErrCodecCorrupted
	}
	return pageDeltaState{Pending: true, MaxSeq: maxSeq}, nil
}

func decodeListingDeltaPageIDFromKey(parent, key []byte) ([]byte, error) {
	prefix := encodeListingDeltaParentPrefix(parent)
	if !bytes.HasPrefix(key, prefix) {
		return nil, ErrParentMismatch
	}
	rest := key[len(prefix):]
	sep := bytes.IndexByte(rest, '|')
	if sep <= 0 {
		return nil, ErrCodecCorrupted
	}
	return cloneBytes(rest[:sep]), nil
}

func decodePageDeltaPageIDFromKey(parent, key []byte) ([]byte, error) {
	prefix := encodePageDeltaParentPrefix(parent)
	if !bytes.HasPrefix(key, prefix) {
		return nil, ErrParentMismatch
	}
	rest := key[len(prefix):]
	sep := bytes.IndexByte(rest, '|')
	if sep <= 0 {
		return nil, ErrCodecCorrupted
	}
	return cloneBytes(rest[:sep]), nil
}

func decodePageDeltaSeqFromKey(parent, key []byte) (uint64, error) {
	prefix := encodePageDeltaParentPrefix(parent)
	if !bytes.HasPrefix(key, prefix) {
		return 0, ErrParentMismatch
	}
	rest := key[len(prefix):]
	sep := bytes.IndexByte(rest, '|')
	if sep <= 0 || sep == len(rest)-1 {
		return 0, ErrCodecCorrupted
	}
	seqRaw := rest[sep+1:]
	if len(seqRaw) != 21 || seqRaw[0] != '#' {
		return 0, ErrCodecCorrupted
	}
	var seq uint64
	for _, ch := range seqRaw[1:] {
		if ch < '0' || ch > '9' {
			return 0, ErrCodecCorrupted
		}
		seq = seq*10 + uint64(ch-'0')
	}
	return seq, nil
}

func encodeReadRoot(root ReadRoot) ([]byte, error) {
	size := 1 + bytesFieldSize(root.Parent) + uvarintSize(root.RootGeneration) + uvarintSize(uint64(len(root.Pages)))
	for _, page := range root.Pages {
		size += bytesFieldSize(page.FenceKey) + bytesFieldSize(page.HighFence) + bytesFieldSize(page.PageID) + uvarintSize(uint64(page.Count)) + 1 + uvarintSize(page.PublishedFrontier) + uvarintSize(page.Generation)
	}
	out := make([]byte, 0, size)
	out = append(out, 6)
	out = appendBytesField(out, root.Parent)
	out = binary.AppendUvarint(out, root.RootGeneration)
	out = binary.AppendUvarint(out, uint64(len(root.Pages)))
	for _, page := range root.Pages {
		out = appendBytesField(out, page.FenceKey)
		out = appendBytesField(out, page.HighFence)
		out = appendBytesField(out, page.PageID)
		out = binary.AppendUvarint(out, uint64(page.Count))
		out = append(out, byte(page.CoverageState))
		out = binary.AppendUvarint(out, page.PublishedFrontier)
		out = binary.AppendUvarint(out, page.Generation)
	}
	return out, nil
}

func decodeReadRoot(raw []byte) (ReadRoot, error) {
	root := ReadRoot{}
	if len(raw) == 0 {
		return root, ErrCodecCorrupted
	}
	version := raw[0]
	if version != 1 && version != 2 && version != 3 && version != 4 && version != 5 && version != 6 {
		return root, ErrCodecCorrupted
	}
	pos := 1
	var err error
	root.Parent, pos, err = consumeBytesField(raw, pos)
	if err != nil {
		return ReadRoot{}, err
	}
	if version >= 5 {
		root.RootGeneration, pos, err = consumeUvarint(raw, pos)
		if err != nil {
			return ReadRoot{}, err
		}
	}
	pageCount, next, err := consumeUvarint(raw, pos)
	if err != nil {
		return ReadRoot{}, err
	}
	pos = next
	root.Pages = make([]ReadPageRef, 0, pageCount)
	for range pageCount {
		var ref ReadPageRef
		ref.FenceKey, pos, err = consumeBytesField(raw, pos)
		if err != nil {
			return ReadRoot{}, err
		}
		if version >= 3 {
			ref.HighFence, pos, err = consumeBytesField(raw, pos)
			if err != nil {
				return ReadRoot{}, err
			}
		}
		ref.PageID, pos, err = consumeBytesField(raw, pos)
		if err != nil {
			return ReadRoot{}, err
		}
		count, next, err := consumeUvarint(raw, pos)
		if err != nil {
			return ReadRoot{}, err
		}
		pos = next
		ref.Count = uint32(count)
		if version >= 2 {
			if pos >= len(raw) {
				return ReadRoot{}, ErrCodecCorrupted
			}
			if version >= 6 {
				ref.CoverageState = PageCoverageState(raw[pos])
			} else if raw[pos] == 1 {
				ref.CoverageState = PageCoverageStateCovered
			} else {
				ref.CoverageState = PageCoverageStateUncovered
			}
			pos++
			ref.PublishedFrontier, pos, err = consumeUvarint(raw, pos)
			if err != nil {
				return ReadRoot{}, err
			}
			if version >= 4 {
				ref.Generation, pos, err = consumeUvarint(raw, pos)
				if err != nil {
					return ReadRoot{}, err
				}
			}
		} else {
			ref.CoverageState = PageCoverageStateCovered
		}
		root.Pages = append(root.Pages, ref)
	}
	if pos != len(raw) {
		return ReadRoot{}, ErrCodecCorrupted
	}
	if version < 5 {
		var maxGen uint64
		for _, ref := range root.Pages {
			if ref.Generation > maxGen {
				maxGen = ref.Generation
			}
		}
		root.RootGeneration = maxGen
	}
	return root, nil
}

func encodeReadPage(page ReadPage) ([]byte, error) {
	size := 1 + bytesFieldSize(page.PageID) + bytesFieldSize(page.LowFence) + bytesFieldSize(page.HighFence) + bytesFieldSize(page.NextPageID) + uvarintSize(page.PublishedFrontier) + uvarintSize(page.Generation) + uvarintSize(uint64(len(page.Entries)))
	for _, entry := range page.Entries {
		size += bytesFieldSize(entry.Name) + 1
	}
	out := make([]byte, 0, size)
	out = append(out, 3)
	out = appendBytesField(out, page.PageID)
	out = appendBytesField(out, page.LowFence)
	out = appendBytesField(out, page.HighFence)
	out = appendBytesField(out, page.NextPageID)
	out = binary.AppendUvarint(out, page.PublishedFrontier)
	out = binary.AppendUvarint(out, page.Generation)
	out = binary.AppendUvarint(out, uint64(len(page.Entries)))
	for _, entry := range page.Entries {
		out = appendBytesField(out, entry.Name)
		out = append(out, byte(entry.Kind))
	}
	return out, nil
}

func decodeReadPage(raw []byte) (ReadPage, error) {
	page := ReadPage{}
	if len(raw) == 0 || (raw[0] != 1 && raw[0] != 2 && raw[0] != 3) {
		return page, ErrCodecCorrupted
	}
	version := raw[0]
	pos := 1
	var err error
	page.PageID, pos, err = consumeBytesField(raw, pos)
	if err != nil {
		return ReadPage{}, err
	}
	page.LowFence, pos, err = consumeBytesField(raw, pos)
	if err != nil {
		return ReadPage{}, err
	}
	page.HighFence, pos, err = consumeBytesField(raw, pos)
	if err != nil {
		return ReadPage{}, err
	}
	page.NextPageID, pos, err = consumeBytesField(raw, pos)
	if err != nil {
		return ReadPage{}, err
	}
	if version >= 3 {
		page.PublishedFrontier, pos, err = consumeUvarint(raw, pos)
		if err != nil {
			return ReadPage{}, err
		}
	}
	if version >= 2 {
		page.Generation, pos, err = consumeUvarint(raw, pos)
		if err != nil {
			return ReadPage{}, err
		}
	}
	entryCount, next, err := consumeUvarint(raw, pos)
	if err != nil {
		return ReadPage{}, err
	}
	pos = next
	page.Entries = make([]Entry, 0, entryCount)
	for range entryCount {
		var entry Entry
		entry.Name, pos, err = consumeBytesField(raw, pos)
		if err != nil {
			return ReadPage{}, err
		}
		if pos >= len(raw) {
			return ReadPage{}, ErrCodecCorrupted
		}
		entry.Kind = EntryKind(raw[pos])
		pos++
		page.Entries = append(page.Entries, entry)
	}
	if pos != len(raw) {
		return ReadPage{}, ErrCodecCorrupted
	}
	return page, nil
}

func encodeListingDelta(delta ListingDelta) ([]byte, error) {
	size := 1 + 1 + 1 + uvarintSize(delta.Seq)
	out := make([]byte, 0, size)
	out = append(out, 2)
	out = append(out, byte(delta.Kind))
	out = append(out, byte(delta.Op))
	out = binary.AppendUvarint(out, delta.Seq)
	return out, nil
}

func decodeListingDelta(raw []byte) (ListingDelta, error) {
	var delta ListingDelta
	if len(raw) < 3 || raw[0] != 2 {
		return delta, ErrCodecCorrupted
	}
	delta.Kind = EntryKind(raw[1])
	delta.Op = DeltaOp(raw[2])
	if len(raw) > 3 {
		seq, _, err := consumeUvarint(raw, 3)
		if err != nil {
			return ListingDelta{}, err
		}
		delta.Seq = seq
	}
	return delta, nil
}

func decodeListingDeltaFromKV(parent, key, raw []byte) (ListingDelta, error) {
	delta, err := decodeListingDelta(raw)
	if err != nil {
		return ListingDelta{}, err
	}
	pageID, name, err := decodeListingDeltaKeyParts(parent, key)
	if err != nil {
		return ListingDelta{}, err
	}
	delta.Parent = cloneBytes(parent)
	delta.PageID = pageID
	delta.Name = name
	return delta, nil
}

func decodePageDeltaFromKV(parent, key, raw []byte) (ListingDelta, error) {
	return decodePageDeltaRecordFromKV(parent, key, raw)
}

func encodePageDeltaRecord(delta ListingDelta) ([]byte, error) {
	size := 1 + bytesFieldSize(delta.Name) + 1 + 1 + uvarintSize(delta.Seq)
	out := make([]byte, 0, size)
	out = append(out, 3)
	out = appendBytesField(out, delta.Name)
	out = append(out, byte(delta.Kind))
	out = append(out, byte(delta.Op))
	out = binary.AppendUvarint(out, delta.Seq)
	return out, nil
}

func decodePageDeltaRecord(raw []byte) (ListingDelta, error) {
	var delta ListingDelta
	if len(raw) == 0 || raw[0] != 3 {
		return delta, ErrCodecCorrupted
	}
	pos := 1
	name, next, err := consumeBytesField(raw, pos)
	if err != nil {
		return ListingDelta{}, err
	}
	pos = next
	if pos+2 > len(raw) {
		return ListingDelta{}, ErrCodecCorrupted
	}
	delta.Name = name
	delta.Kind = EntryKind(raw[pos])
	delta.Op = DeltaOp(raw[pos+1])
	pos += 2
	if pos < len(raw) {
		delta.Seq, pos, err = consumeUvarint(raw, pos)
		if err != nil {
			return ListingDelta{}, err
		}
	}
	if pos != len(raw) {
		return ListingDelta{}, ErrCodecCorrupted
	}
	return delta, nil
}

func decodePageDeltaRecordFromKV(parent, key, raw []byte) (ListingDelta, error) {
	delta, err := decodePageDeltaRecord(raw)
	if err != nil {
		return ListingDelta{}, err
	}
	pageID, err := decodePageDeltaPageIDFromKey(parent, key)
	if err != nil {
		return ListingDelta{}, err
	}
	delta.Parent = cloneBytes(parent)
	delta.PageID = pageID
	return delta, nil
}

func decodeListingDeltaKeyParts(parent, key []byte) ([]byte, []byte, error) {
	prefix := encodeListingDeltaParentPrefix(parent)
	if !bytes.HasPrefix(key, prefix) {
		return nil, nil, ErrParentMismatch
	}
	rest := key[len(prefix):]
	sep := bytes.IndexByte(rest, '|')
	if sep <= 0 || sep == len(rest)-1 {
		return nil, nil, ErrCodecCorrupted
	}
	return cloneBytes(rest[:sep]), cloneBytes(rest[sep+1:]), nil
}

func joinPath(parent, name []byte) []byte {
	if bytes.Equal(parent, []byte("/")) {
		out := make([]byte, 0, 1+len(name))
		out = append(out, '/')
		out = append(out, name...)
		return out
	}
	out := make([]byte, 0, len(parent)+1+len(name))
	out = append(out, parent...)
	out = append(out, '/')
	out = append(out, name...)
	return out
}

func splitPath(path []byte) (parent, name []byte, err error) {
	if len(path) == 0 || path[0] != '/' || bytes.Equal(path, []byte("/")) {
		return nil, nil, ErrInvalidPath
	}
	idx := bytes.LastIndexByte(path, '/')
	if idx < 0 || idx == len(path)-1 {
		return nil, nil, ErrInvalidPath
	}
	if idx == 0 {
		return []byte("/"), cloneBytes(path[1:]), nil
	}
	return cloneBytes(path[:idx]), cloneBytes(path[idx+1:]), nil
}

func bytesFieldSize(b []byte) int {
	return uvarintSize(uint64(len(b))) + len(b)
}

func appendBytesField(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func consumeBytesField(raw []byte, pos int) ([]byte, int, error) {
	size, next, err := consumeUvarint(raw, pos)
	if err != nil {
		return nil, 0, err
	}
	if next+int(size) > len(raw) {
		return nil, 0, ErrCodecCorrupted
	}
	out := cloneBytes(raw[next : next+int(size)])
	return out, next + int(size), nil
}

func consumeUvarint(raw []byte, pos int) (uint64, int, error) {
	if pos >= len(raw) {
		return 0, 0, ErrCodecCorrupted
	}
	val, n := binary.Uvarint(raw[pos:])
	if n <= 0 {
		return 0, 0, ErrCodecCorrupted
	}
	return val, pos + n, nil
}

func uvarintSize(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
