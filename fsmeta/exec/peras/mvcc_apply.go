package peras

import (
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
)

// BuildMVCCSegmentInstallEntries materializes one sealed segment as one
// MVCC-visible install version. This is the raftstore Peras install boundary:
// the segment keeps per-operation completion metadata, while the LSM receives
// only the coalesced final key state.
func BuildMVCCSegmentInstallEntries(segment PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	entries := make([]*entrykv.Entry, 0, len(segment.entries)*3+1)
	for _, entry := range segment.entries {
		mutationEntries, err := buildMutationMVCCReplayEntries(ReplayMutation{
			Key:    entry.Key,
			Value:  entry.Value,
			Delete: entry.Delete,
		}, version)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return nil, err
		}
		entries = append(entries, mutationEntries...)
	}
	payload, err := EncodePerasSegment(segment)
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	digest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	catalogValue, err := EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
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

// BuildMVCCSegmentCatalogInstallEntries installs the sealed segment object
// itself. The ordinary per-key MVCC materialization remains a background
// optimization; the durable commit unit is this catalog entry.
func BuildMVCCSegmentCatalogInstallEntries(segment PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	payload, err := EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment, version, payload, digest)
}

// BuildMVCCSegmentCatalogInstallEntriesWithPayload is the raftstore install
// path for an already verified segment payload.
func BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	catalogValue, err := EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntries(segment, version, catalogValue, digest, uint64(len(payload)))
}

// BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey installs one
// bucket-local copy of a segment catalog. Cross-bucket segments are installed
// by issuing this once per touched bucket; each raft region only writes its
// local object and index keys.
func BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment PerasSegment, version uint64, payload []byte, digest [32]byte, objectKey []byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	bucket, err := perasSegmentObjectBucket(segment, objectKey)
	if err != nil {
		return nil, err
	}
	catalogValue, err := EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		return nil, err
	}
	indexValue, err := encodePerasSegmentCatalogIndexRecord(segment.EpochID, version, segment.Root, digest, uint64(len(payload)), objectKey)
	if err != nil {
		return nil, err
	}
	indexKey, err := fsmeta.EncodePerasSegmentCatalogIndexKey(bucket.mount, bucket.bucket, segment.Root)
	if err != nil {
		return nil, err
	}
	return []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFDefault, objectKey, version, catalogValue, 0, 0),
		entrykv.NewInternalEntry(entrykv.CFDefault, indexKey, version, indexValue, 0, 0),
	}, nil
}

func buildMVCCSegmentCatalogInstallEntries(segment PerasSegment, version uint64, objectValue []byte, digest [32]byte, payloadSize uint64) ([]*entrykv.Entry, error) {
	objectKey, err := PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	indexValue, err := encodePerasSegmentCatalogIndexRecord(segment.EpochID, version, segment.Root, digest, payloadSize, objectKey)
	if err != nil {
		return nil, err
	}
	indexKeys, err := PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, err
	}
	entries := make([]*entrykv.Entry, 0, len(indexKeys)+1)
	entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, objectKey, version, objectValue, 0, 0))
	for _, key := range indexKeys {
		entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, key, version, indexValue, 0, 0))
	}
	return entries, nil
}

func buildMutationMVCCReplayEntries(mutation ReplayMutation, version uint64) ([]*entrykv.Entry, error) {
	if len(mutation.Key) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	if mutation.Delete {
		if mutation.Value != nil {
			return nil, ErrInvalidPerasSegment
		}
		write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Delete, StartTs: version})
		return []*entrykv.Entry{
			entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
			entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, write, 0, 0),
		}, nil
	}
	if mutation.Value == nil {
		return nil, ErrInvalidPerasSegment
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
