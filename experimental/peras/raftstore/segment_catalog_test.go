// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/stretchr/testify/require"
)

func TestLoadPerasSegmentCatalogsScansInstalledSegments(t *testing.T) {
	db := openPerasReplayDB(t)
	segment := fsmetaSegmentForTest(t)
	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	require.NoError(t, db.ApplyInternalEntries(entries))
	releaseMVCCReplayEntries(entries)

	records, err := LoadPerasSegmentCatalogs(db)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, segment.Root, records[0].Root)
	require.Equal(t, uint64(99), records[0].InstallVersion)
	require.Equal(t, segment.Stats().OperationCount, records[0].OperationCount)
	require.Len(t, records[0].Completions, len(segment.Completions))
}

func TestLoadPerasSegmentCatalogFindsInstalledSegment(t *testing.T) {
	db := openPerasReplayDB(t)
	segment := fsmetaSegmentForTest(t)
	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	require.NoError(t, db.ApplyInternalEntries(entries))
	releaseMVCCReplayEntries(entries)

	record, ok, err := LoadPerasSegmentCatalog(db, segment)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, segment.Root, record.Root)
	require.Equal(t, uint64(99), record.InstallVersion)
	require.Equal(t, segment.Stats().EntryCount, record.EntryCount)
	require.Len(t, record.Completions, len(segment.Completions))
}

func TestLoadPerasSegmentCatalogInstallForObjectKeyUsesBucketIndex(t *testing.T) {
	db := openPerasReplayDB(t)
	segment := fsmetaMultiBucketSegmentForTest(t)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	objectKeys, err := fsperas.PerasSegmentCatalogObjectKeys(segment)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(objectKeys), 2)

	for _, objectKey := range objectKeys {
		entries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, objectKey)
		require.NoError(t, err)
		require.NoError(t, db.ApplyInternalEntries(entries))
		releaseMVCCReplayEntries(entries)
	}

	for _, objectKey := range objectKeys {
		installed, err := LoadPerasSegmentCatalogInstallForObjectKey(db, segment, objectKey)
		require.NoError(t, err)
		require.True(t, installed)
	}
	canonical, ok, err := LoadPerasSegmentCatalogForObjectKey(db, segment, objectKeys[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, segment.Root, canonical.Root)
	_, ok, err = LoadPerasSegmentCatalogForObjectKey(db, segment, objectKeys[1])
	require.NoError(t, err)
	require.False(t, ok)
}
