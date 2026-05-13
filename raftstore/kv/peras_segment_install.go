package kv

import (
	"bytes"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
)

func BuildMVCCSegmentInstallEntries(segment fsperas.PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment, version, payload, digest)
}

func buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	stats := segment.Stats()
	entries := make([]*entrykv.Entry, 0, int(stats.EntryCount)*3+1)
	err := segment.ForEachEntry(func(entry fsperas.SegmentKV) error {
		mutationEntries, err := buildMutationMVCCReplayEntries(fsperas.ReplayMutation{
			Key:    entry.Key,
			Value:  entry.Value,
			Delete: entry.Delete,
		}, version)
		if err != nil {
			return err
		}
		entries = append(entries, mutationEntries...)
		return nil
	})
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	catalogEntries, err := buildMVCCSegmentCatalogInstallEntries(segment, version, catalogValue, digest, uint64(len(payload)))
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	entries = append(entries, catalogEntries...)
	return entries, nil
}

func BuildMVCCSegmentCatalogInstallEntries(segment fsperas.PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment, version, payload, digest)
}

func BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	if _, err := fsperas.VerifyPerasSegmentPayload(payload, segment.Root, digest); err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayload(segment, version, payload, digest)
}

func buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayload(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntries(segment, version, catalogValue, digest, uint64(len(payload)))
}

func BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte, objectKey []byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	if _, err := fsperas.VerifyPerasSegmentPayload(payload, segment.Root, digest); err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment, version, payload, digest, objectKey)
}

func buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte, objectKey []byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	bucket, err := perasSegmentObjectBucket(segment, objectKey)
	if err != nil {
		return nil, err
	}
	canonicalObjectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	indexValue, err := encodeSegmentCatalogIndexValue(segment, version, payload, digest, canonicalObjectKey)
	if err != nil {
		return nil, err
	}
	indexKey, err := fsmeta.EncodePerasSegmentCatalogIndexKey(bucket.mount, bucket.bucket, segment.Root)
	if err != nil {
		return nil, err
	}
	entries := []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFDefault, indexKey, version, indexValue, 0, 0),
	}
	if bytes.Equal(objectKey, canonicalObjectKey) {
		catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return nil, err
		}
		entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, objectKey, version, catalogValue, 0, 0))
	}
	return entries, nil
}

func buildMVCCSegmentCatalogInstallEntries(segment fsperas.PerasSegment, version uint64, objectValue []byte, digest [32]byte, payloadSize uint64) ([]*entrykv.Entry, error) {
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	indexValue, err := encodeSegmentCatalogIndexValue(segment, version, nil, digest, objectKey)
	if err != nil {
		return nil, err
	}
	indexKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, err
	}
	entries := make([]*entrykv.Entry, 0, len(indexKeys)+1)
	entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, objectKey, version, objectValue, 0, 0))
	for _, key := range indexKeys {
		entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, key, version, indexValue, 0, 0))
	}
	_ = payloadSize
	return entries, nil
}

func encodeSegmentCatalogIndexValue(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte, objectKey []byte) ([]byte, error) {
	if payload == nil {
		var err error
		payload, err = fsperas.EncodePerasSegment(segment)
		if err != nil {
			return nil, err
		}
	}
	recordValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		return nil, err
	}
	record, err := fsperas.DecodePerasSegmentCatalogRecord(recordValue)
	if err != nil {
		return nil, err
	}
	return fsperas.EncodePerasSegmentCatalogIndexRecord(record, objectKey)
}

type perasSegmentCatalogBucket struct {
	mount  fsmeta.MountKeyID
	bucket fsmeta.AffinityBucket
}

func perasSegmentObjectBucket(segment fsperas.PerasSegment, objectKey []byte) (perasSegmentCatalogBucket, error) {
	parts, ok := fsmeta.InspectKey(objectKey)
	if !ok {
		return perasSegmentCatalogBucket{}, fsperas.ErrInvalidPerasSegment
	}
	keys, err := fsperas.PerasSegmentCatalogObjectKeys(segment)
	if err != nil {
		return perasSegmentCatalogBucket{}, err
	}
	for _, key := range keys {
		if bytes.Equal(key, objectKey) {
			return perasSegmentCatalogBucket{mount: parts.MountKeyID, bucket: parts.Bucket}, nil
		}
	}
	return perasSegmentCatalogBucket{}, fsperas.ErrInvalidPerasSegment
}

func buildMutationMVCCReplayEntries(mutation fsperas.ReplayMutation, version uint64) ([]*entrykv.Entry, error) {
	if len(mutation.Key) == 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	if mutation.Delete {
		if mutation.Value != nil {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Delete, StartTs: version})
		return []*entrykv.Entry{
			entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
			entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, write, 0, 0),
		}, nil
	}
	if mutation.Value == nil {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	write := txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: version}
	if txnmvcc.CanInlineShortValue(kvrpcpb.Mutation_Put, mutation.Value) {
		write.ShortValue = cloneBytes(mutation.Value)
		return []*entrykv.Entry{
			entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, txnmvcc.EncodeWrite(write), 0, 0),
		}, nil
	}
	return []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
		entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, cloneBytes(mutation.Value), 0, 0),
		entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, txnmvcc.EncodeWrite(write), 0, 0),
	}, nil
}

func releaseMVCCReplayEntries(entries []*entrykv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
