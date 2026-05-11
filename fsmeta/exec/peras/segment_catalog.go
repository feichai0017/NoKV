package peras

import (
	"bytes"
	"slices"

	"github.com/feichai0017/NoKV/engine/index"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
)

var perasSegmentCatalogMagic = [4]byte{'N', 'P', 'C', 1}

// SegmentCatalogRecord is the single hidden raftstore record written with a
// durable segment install. It is enough to rebuild the installed segment
// frontier and operation completion table after restart without replaying
// per-operation witness records.
type SegmentCatalogRecord struct {
	EpochID        uint64
	InstallVersion uint64
	Root           [32]byte

	OperationCount     uint64
	EntryCount         uint64
	CompletionCount    uint64
	InputMutationCount uint64
	CoalescedMutations uint64

	Completions []SegmentCompletion
}

type SegmentCatalogStore interface {
	NewInternalIterator(*index.Options) index.Iterator
}

func PerasSegmentCatalogKey(segment PerasSegment) ([]byte, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	entries := segment.Entries()
	if len(entries) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	parts, ok := fsmeta.InspectKey(entries[0].Key)
	if !ok {
		return nil, ErrInvalidPerasSegment
	}
	return fsmeta.EncodePerasSegmentCatalogKey(parts.MountKeyID, parts.Bucket, segment.Root)
}

func LoadPerasSegmentCatalogs(store SegmentCatalogStore) ([]SegmentCatalogRecord, error) {
	if store == nil {
		return nil, ErrReplayStoreRequired
	}
	it := store.NewInternalIterator(&index.Options{IsAsc: true})
	if it == nil {
		return nil, ErrReplayStoreRequired
	}
	var records []SegmentCatalogRecord
	it.Rewind()
	for it.Valid() {
		item := it.Item()
		if item == nil || item.Entry() == nil {
			it.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok || cf != entrykv.CFDefault {
			it.Next()
			continue
		}
		parts, ok := fsmeta.InspectKey(userKey)
		if !ok || parts.Kind != fsmeta.KeyKindPeras {
			it.Next()
			continue
		}
		record, err := DecodePerasSegmentCatalogRecord(entry.Value)
		if err != nil {
			_ = it.Close()
			return nil, err
		}
		if record.Root != parts.PerasRoot {
			_ = it.Close()
			return nil, ErrInvalidPerasSegment
		}
		records = append(records, record)
		it.Next()
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	slices.SortFunc(records, func(a, b SegmentCatalogRecord) int {
		if a.InstallVersion < b.InstallVersion {
			return -1
		}
		if a.InstallVersion > b.InstallVersion {
			return 1
		}
		return bytes.Compare(a.Root[:], b.Root[:])
	})
	return records, nil
}

func LoadPerasSegmentCatalog(store SegmentCatalogStore, segment PerasSegment) (SegmentCatalogRecord, bool, error) {
	if store == nil {
		return SegmentCatalogRecord{}, false, ErrReplayStoreRequired
	}
	catalogKey, err := PerasSegmentCatalogKey(segment)
	if err != nil {
		return SegmentCatalogRecord{}, false, err
	}
	it := store.NewInternalIterator(&index.Options{IsAsc: true})
	if it == nil {
		return SegmentCatalogRecord{}, false, ErrReplayStoreRequired
	}
	defer func() { _ = it.Close() }()

	it.Seek(entrykv.InternalKey(entrykv.CFDefault, catalogKey, entrykv.MaxVersion))
	if !it.Valid() {
		return SegmentCatalogRecord{}, false, nil
	}
	item := it.Item()
	if item == nil || item.Entry() == nil {
		return SegmentCatalogRecord{}, false, nil
	}
	entry := item.Entry()
	cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
	if !ok || cf != entrykv.CFDefault || !bytes.Equal(userKey, catalogKey) {
		return SegmentCatalogRecord{}, false, nil
	}
	record, err := DecodePerasSegmentCatalogRecord(entry.Value)
	if err != nil {
		return SegmentCatalogRecord{}, false, err
	}
	if record.Root != segment.Root {
		return SegmentCatalogRecord{}, false, ErrInvalidPerasSegment
	}
	return record, true, nil
}

func EncodePerasSegmentCatalogRecord(segment PerasSegment, installVersion uint64) ([]byte, error) {
	if installVersion == 0 {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	stats := segment.Stats()
	var out bytes.Buffer
	writeFixed(&out, perasSegmentCatalogMagic[:])
	writeUint64(&out, segment.EpochID)
	writeUint64(&out, installVersion)
	writeFixed(&out, segment.Root[:])
	writeUint64(&out, stats.OperationCount)
	writeUint64(&out, stats.EntryCount)
	writeUint64(&out, stats.CompletionCount)
	writeUint64(&out, stats.InputMutationCount)
	writeUint64(&out, stats.CoalescedMutations)
	writeUint64(&out, uint64(len(segment.Completions)))
	for _, completion := range segment.Completions {
		writeOperationID(&out, completion.OpID)
		writeString(&out, string(completion.Kind))
		writeUint64(&out, completion.Version)
		writeUint64(&out, uint64(completion.MutationCount))
	}
	return out.Bytes(), nil
}

func DecodePerasSegmentCatalogRecord(payload []byte) (SegmentCatalogRecord, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(perasSegmentCatalogMagic); err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	epochID, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	installVersion, err := r.readUint64()
	if err != nil || installVersion == 0 {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	var root [32]byte
	if err := r.readFixed(root[:]); err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	operationCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	entryCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	completionCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	inputMutationCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	coalescedMutations, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	encodedCompletionCount, err := r.readUint64()
	if err != nil || encodedCompletionCount != completionCount || encodedCompletionCount > uint64(maxSegmentSliceLen()) {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	completions := make([]SegmentCompletion, 0, encodedCompletionCount)
	for range encodedCompletionCount {
		opID, err := r.readOperationID()
		if err != nil {
			return SegmentCatalogRecord{}, ErrInvalidPerasSegment
		}
		kind, err := r.readString()
		if err != nil {
			return SegmentCatalogRecord{}, ErrInvalidPerasSegment
		}
		version, err := r.readUint64()
		if err != nil {
			return SegmentCatalogRecord{}, ErrInvalidPerasSegment
		}
		mutationCount, err := r.readUint64()
		if err != nil || mutationCount > uint64(^uint32(0)) {
			return SegmentCatalogRecord{}, ErrInvalidPerasSegment
		}
		completions = append(completions, SegmentCompletion{
			OpID:          opID,
			Kind:          fsmeta.OperationKind(kind),
			Version:       version,
			MutationCount: uint32(mutationCount),
		})
	}
	if !r.done() || epochID == 0 || root == ([32]byte{}) || operationCount == 0 || completionCount != uint64(len(completions)) {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	return SegmentCatalogRecord{
		EpochID:            epochID,
		InstallVersion:     installVersion,
		Root:               root,
		OperationCount:     operationCount,
		EntryCount:         entryCount,
		CompletionCount:    completionCount,
		InputMutationCount: inputMutationCount,
		CoalescedMutations: coalescedMutations,
		Completions:        completions,
	}, nil
}
