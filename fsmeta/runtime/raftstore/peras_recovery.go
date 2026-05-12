package raftstore

import (
	"bytes"
	"context"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

func (c *RemotePerasCommitter) RecoverWitnessSegments(ctx context.Context, scope compile.AuthorityScope, epochID uint64) error {
	if c == nil || epochID == 0 || c.installer == nil {
		return errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.LoadRootSealedSegments(ctx, scope); err != nil {
		return err
	}
	records, err := c.collectWitnessSegments(ctx, epochID)
	if err != nil {
		return c.recordErrorf("probe peras segment witnesses: %w", err)
	}
	for _, record := range records {
		if c.segmentInstalled(record.SegmentRoot) {
			c.recoverySkipTotal.Add(1)
			continue
		}
		if err := fsperas.VerifySegmentWitnessRecord(record); err != nil {
			return c.recordErrorf("verify peras segment witness: %w", err)
		}
		segment, err := fsperas.VerifyPerasSegmentPayload(record.SegmentPayload, record.SegmentRoot, record.SegmentPayloadDigest)
		if err != nil {
			return c.recordErrorf("decode peras witness segment: %w", err)
		}
		if !perasSegmentWithinScope(segment, scope) {
			continue
		}
		stats := segment.Stats()
		if record.OperationCount != stats.OperationCount || record.EntryCount != stats.EntryCount {
			return c.recordError(fsperas.ErrInvalidWitnessRecord)
		}
		job := perasFlushJob{
			scope:   scope,
			segment: segment,
			payload: record.SegmentPayload,
			digest:  record.SegmentPayloadDigest,
		}
		if _, err := c.submitInstallJob(ctx, job); err != nil {
			return c.recordErrorf("recover peras segment install: %w", err)
		}
		if err := c.installSegment(fsperas.ReplayPlan{}, segment); err != nil {
			return err
		}
		c.recoveryInstallTotal.Add(1)
	}
	return nil
}

func (c *RemotePerasCommitter) LoadInstalledSegments(ctx context.Context, scope compile.AuthorityScope) error {
	if c == nil || c.catalog == nil || scope.MountKeyID == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := c.scanInstalledSegmentCatalogs(ctx, scope)
	if err != nil {
		return c.recordErrorf("load peras segment catalogs: %w", err)
	}
	for _, record := range records {
		if c.segmentInstalled(record.Root) {
			continue
		}
		segment, err := fsperas.VerifyPerasSegmentPayload(record.SegmentPayload, record.Root, record.SegmentPayloadDigest)
		if err != nil {
			return c.recordErrorf("decode peras segment catalog: %w", err)
		}
		if !perasSegmentWithinScope(segment, scope) {
			continue
		}
		if err := c.installSegment(fsperas.ReplayPlan{}, segment); err != nil {
			return err
		}
		c.catalogLoadTotal.Add(1)
	}
	return nil
}

func (c *RemotePerasCommitter) LoadRootSealedSegments(ctx context.Context, scope compile.AuthorityScope) error {
	if c == nil {
		return errPerasCommitterInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.seals == nil {
		return c.LoadInstalledSegments(ctx, scope)
	}
	seals, err := c.seals.ListPerasAuthoritySeals(ctx, scope)
	if err != nil {
		return c.recordErrorf("list rooted peras segment seals: %w", err)
	}
	if len(seals) == 0 {
		return nil
	}
	if err := c.LoadInstalledSegments(ctx, scope); err != nil {
		return err
	}
	for _, seal := range seals {
		if !seal.Valid() {
			return c.recordError(fsperas.ErrInvalidPerasSegment)
		}
		c.rootSealTotal.Add(1)
		if c.segmentInstalled(seal.SegmentRoot) {
			continue
		}
		c.rootSealMissingTotal.Add(1)
		return c.recordErrorf("rooted peras segment seal missing installed catalog: %w", fsperas.ErrInvalidPerasSegment)
	}
	return nil
}

func (c *RemotePerasCommitter) scanInstalledSegmentCatalogs(ctx context.Context, scope compile.AuthorityScope) ([]fsperas.SegmentCatalogRecord, error) {
	buckets := perasCatalogBuckets(scope)
	if len(buckets) == 0 {
		return nil, nil
	}
	records := make([]fsperas.SegmentCatalogRecord, 0)
	seen := make(map[[32]byte]struct{})
	for _, bucket := range buckets {
		prefix, err := fsmeta.EncodePerasSegmentCatalogIndexPrefix(scope.MountKeyID, bucket)
		if err != nil {
			return nil, err
		}
		next := runtimeCloneBytes(prefix)
		for {
			kvs, err := c.catalog.Scan(ctx, next, defaultPerasSegmentCatalogScanLimit, 0)
			if err != nil {
				return nil, err
			}
			if len(kvs) == 0 {
				break
			}
			exhausted := false
			for _, kv := range kvs {
				if !bytes.HasPrefix(kv.Key, prefix) {
					exhausted = true
					break
				}
				index, err := fsperas.DecodePerasSegmentCatalogIndexRecord(kv.Value)
				if err != nil {
					return nil, err
				}
				parts, ok := fsmeta.InspectKey(kv.Key)
				if !ok || parts.Kind != fsmeta.KeyKindPeras || parts.PerasRecord != fsmeta.PerasSegmentRecordIndex || parts.PerasRoot != index.Root {
					return nil, fsperas.ErrInvalidPerasSegment
				}
				if _, ok := seen[index.Root]; ok {
					next = scanAfterKey(kv.Key)
					continue
				}
				record, err := c.loadInstalledSegmentObject(ctx, index)
				if err != nil {
					return nil, err
				}
				seen[index.Root] = struct{}{}
				records = append(records, record)
				next = scanAfterKey(kv.Key)
			}
			if exhausted || len(kvs) < defaultPerasSegmentCatalogScanLimit {
				break
			}
		}
	}
	slices.SortFunc(records, func(a, b fsperas.SegmentCatalogRecord) int {
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

func (c *RemotePerasCommitter) loadInstalledSegmentObject(ctx context.Context, index fsperas.SegmentCatalogIndexRecord) (fsperas.SegmentCatalogRecord, error) {
	kvs, err := c.catalog.Scan(ctx, index.ObjectKey, 1, 0)
	if err != nil {
		return fsperas.SegmentCatalogRecord{}, err
	}
	if len(kvs) == 0 || !bytes.Equal(kvs[0].Key, index.ObjectKey) {
		return fsperas.SegmentCatalogRecord{}, fsperas.ErrInvalidPerasSegment
	}
	record, err := fsperas.DecodePerasSegmentCatalogRecord(kvs[0].Value)
	if err != nil {
		return fsperas.SegmentCatalogRecord{}, err
	}
	if record.Root != index.Root ||
		record.InstallVersion != index.InstallVersion ||
		record.SegmentPayloadDigest != index.SegmentPayloadDigest ||
		record.SegmentPayloadSize != index.SegmentPayloadSize {
		return fsperas.SegmentCatalogRecord{}, fsperas.ErrInvalidPerasSegment
	}
	return record, nil
}

func scanAfterKey(key []byte) []byte {
	next := make([]byte, 0, len(key)+1)
	next = append(next, key...)
	return append(next, 0)
}
