package peras

import (
	"bytes"
	"slices"

	"github.com/feichai0017/NoKV/engine/index"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
)

var (
	perasSegmentCatalogMagic = [4]byte{'N', 'P', 'C', 1}
	perasSegmentIndexMagic   = [4]byte{'N', 'P', 'I', 1}
)

// SegmentCatalogRecord is the hidden segment object written with a durable
// segment install. Bucket-local index records point to this object so restart
// can rebuild the installed segment frontier and operation completion table
// without replaying per-operation witness records.
type SegmentCatalogRecord struct {
	EpochID        uint64
	InstallVersion uint64
	Root           [32]byte

	SegmentPayloadDigest [32]byte
	SegmentPayloadSize   uint64
	SegmentPayload       []byte

	OperationCount     uint64
	EntryCount         uint64
	CompletionCount    uint64
	InputMutationCount uint64
	CoalescedMutations uint64

	Completions []SegmentCompletion
}

// SegmentCatalogIndexRecord is the per-bucket discovery record for one sealed
// segment object.
type SegmentCatalogIndexRecord struct {
	EpochID              uint64
	InstallVersion       uint64
	Root                 [32]byte
	SegmentPayloadDigest [32]byte
	SegmentPayloadSize   uint64
	ObjectKey            []byte
}

type SegmentCatalogStore interface {
	NewInternalIterator(*index.Options) index.Iterator
}

func PerasSegmentCatalogIndexKeys(segment PerasSegment) ([][]byte, error) {
	buckets, err := perasSegmentCatalogBuckets(segment)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		key, err := fsmeta.EncodePerasSegmentCatalogIndexKey(bucket.mount, bucket.bucket, segment.Root)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func PerasSegmentObjectKey(segment PerasSegment) ([]byte, error) {
	buckets, err := perasSegmentCatalogBuckets(segment)
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	return fsmeta.EncodePerasSegmentObjectKey(buckets[0].mount, buckets[0].bucket, segment.Root)
}

type perasSegmentCatalogBucket struct {
	mount  fsmeta.MountKeyID
	bucket fsmeta.AffinityBucket
}

func perasSegmentCatalogBuckets(segment PerasSegment) ([]perasSegmentCatalogBucket, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	if len(segment.entries) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	seen := make(map[perasSegmentCatalogBucket]struct{})
	out := make([]perasSegmentCatalogBucket, 0)
	for _, entry := range segment.entries {
		parts, ok := fsmeta.InspectKey(entry.Key)
		if !ok {
			return nil, ErrInvalidPerasSegment
		}
		key := perasSegmentCatalogBucket{mount: parts.MountKeyID, bucket: parts.Bucket}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	slices.SortFunc(out, func(a, b perasSegmentCatalogBucket) int {
		if a.mount < b.mount {
			return -1
		}
		if a.mount > b.mount {
			return 1
		}
		if a.bucket < b.bucket {
			return -1
		}
		if a.bucket > b.bucket {
			return 1
		}
		return 0
	})
	return out, nil
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
		if parts.PerasRecord != fsmeta.PerasSegmentRecordObject {
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
	catalogKey, err := PerasSegmentObjectKey(segment)
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
	payload, err := EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return EncodePerasSegmentCatalogRecordWithPayload(segment, installVersion, payload, digest)
}

// EncodePerasSegmentCatalogRecordWithPayload writes a catalog record using a
// segment payload the caller has already verified against segment.Root and
// digest. This avoids re-encoding the payload on the raftstore install path.
func EncodePerasSegmentCatalogRecordWithPayload(segment PerasSegment, installVersion uint64, payload []byte, digest [32]byte) ([]byte, error) {
	if installVersion == 0 {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	if len(payload) == 0 || digest == ([32]byte{}) {
		return nil, ErrInvalidPerasSegment
	}
	stats := segment.Stats()
	var out bytes.Buffer
	writeFixed(&out, perasSegmentCatalogMagic[:])
	writeUint64(&out, segment.EpochID)
	writeUint64(&out, installVersion)
	writeFixed(&out, segment.Root[:])
	writeFixed(&out, digest[:])
	writeUint64(&out, uint64(len(payload)))
	writeBytes(&out, payload)
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

func EncodePerasSegmentCatalogIndexRecord(record SegmentCatalogRecord, objectKey []byte) ([]byte, error) {
	if err := validateSegmentCatalogPayload(record); err != nil {
		return nil, err
	}
	if len(objectKey) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	var out bytes.Buffer
	writeFixed(&out, perasSegmentIndexMagic[:])
	writeUint64(&out, record.EpochID)
	writeUint64(&out, record.InstallVersion)
	writeFixed(&out, record.Root[:])
	writeFixed(&out, record.SegmentPayloadDigest[:])
	writeUint64(&out, record.SegmentPayloadSize)
	writeBytes(&out, objectKey)
	return out.Bytes(), nil
}

func DecodePerasSegmentCatalogIndexRecord(payload []byte) (SegmentCatalogIndexRecord, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(perasSegmentIndexMagic); err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	epochID, err := r.readUint64()
	if err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	installVersion, err := r.readUint64()
	if err != nil || installVersion == 0 {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	var root [32]byte
	if err := r.readFixed(root[:]); err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	var payloadDigest [32]byte
	if err := r.readFixed(payloadDigest[:]); err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	payloadSize, err := r.readUint64()
	if err != nil || payloadSize == 0 {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	objectKey, err := r.readBytes()
	if err != nil || len(objectKey) == 0 {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	if !r.done() || epochID == 0 || root == ([32]byte{}) || payloadDigest == ([32]byte{}) {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	parts, ok := fsmeta.InspectKey(objectKey)
	if !ok || parts.Kind != fsmeta.KeyKindPeras || parts.PerasRecord != fsmeta.PerasSegmentRecordObject || parts.PerasRoot != root {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	return SegmentCatalogIndexRecord{
		EpochID:              epochID,
		InstallVersion:       installVersion,
		Root:                 root,
		SegmentPayloadDigest: payloadDigest,
		SegmentPayloadSize:   payloadSize,
		ObjectKey:            objectKey,
	}, nil
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
	var payloadDigest [32]byte
	if err := r.readFixed(payloadDigest[:]); err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	payloadSize, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	segmentPayload, err := r.readBytes()
	if err != nil {
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
	record := SegmentCatalogRecord{
		EpochID:              epochID,
		InstallVersion:       installVersion,
		Root:                 root,
		SegmentPayloadDigest: payloadDigest,
		SegmentPayloadSize:   payloadSize,
		SegmentPayload:       segmentPayload,
		OperationCount:       operationCount,
		EntryCount:           entryCount,
		CompletionCount:      completionCount,
		InputMutationCount:   inputMutationCount,
		CoalescedMutations:   coalescedMutations,
		Completions:          completions,
	}
	if err := validateSegmentCatalogPayload(record); err != nil {
		return SegmentCatalogRecord{}, err
	}
	return record, nil
}

func validateSegmentCatalogPayload(record SegmentCatalogRecord) error {
	if record.Root == ([32]byte{}) || record.SegmentPayloadDigest == ([32]byte{}) {
		return ErrInvalidPerasSegment
	}
	if record.SegmentPayloadSize == 0 || uint64(len(record.SegmentPayload)) != record.SegmentPayloadSize {
		return ErrInvalidPerasSegment
	}
	segment, err := VerifyPerasSegmentPayload(record.SegmentPayload, record.Root, record.SegmentPayloadDigest)
	if err != nil {
		return err
	}
	stats := segment.Stats()
	if stats.OperationCount != record.OperationCount ||
		stats.EntryCount != record.EntryCount ||
		stats.CompletionCount != record.CompletionCount ||
		stats.InputMutationCount != record.InputMutationCount ||
		stats.CoalescedMutations != record.CoalescedMutations ||
		len(segment.Completions) != len(record.Completions) {
		return ErrInvalidPerasSegment
	}
	for i, completion := range segment.Completions {
		if completion != record.Completions[i] {
			return ErrInvalidPerasSegment
		}
	}
	return nil
}
