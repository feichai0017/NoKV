// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

type fakeScrubClient struct {
	dirs map[fsmeta.InodeID][]fsmeta.DentryAttrPair
	err  error
}

func (c *fakeScrubClient) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	if c.err != nil {
		return nil, c.err
	}
	entries := c.dirs[req.Parent]
	start := 0
	for start < len(entries) && entries[start].Dentry.Name <= req.StartAfter {
		start++
	}
	limit := int(req.Limit)
	if limit == 0 || limit > len(entries)-start {
		limit = len(entries) - start
	}
	return append([]fsmeta.DentryAttrPair(nil), entries[start:start+limit]...), nil
}

func (c *fakeScrubClient) Close() error {
	return nil
}

func TestScrubMountReportsHealthyNamespace(t *testing.T) {
	cli := &fakeScrubClient{dirs: map[fsmeta.InodeID][]fsmeta.DentryAttrPair{
		fsmeta.RootInode: {
			scrubPair(fsmeta.RootInode, "dir", 10, fsmeta.InodeTypeDirectory, 1),
			scrubPair(fsmeta.RootInode, "file", 11, fsmeta.InodeTypeFile, 2),
			scrubPair(fsmeta.RootInode, "hardlink", 11, fsmeta.InodeTypeFile, 2),
		},
		10: {
			scrubPair(10, "child", 12, fsmeta.InodeTypeFile, 1),
		},
	}}

	report, err := scrubMount(context.Background(), cli, "vol", fsmeta.RootInode, 2, 8)
	require.NoError(t, err)
	require.True(t, report.OK())
	require.Equal(t, uint64(2), report.Directories)
	require.Equal(t, uint64(4), report.Dentries)
	require.Equal(t, uint64(3), report.Inodes)
}

func TestScrubMountReportsInvariantIssues(t *testing.T) {
	cli := &fakeScrubClient{dirs: map[fsmeta.InodeID][]fsmeta.DentryAttrPair{
		fsmeta.RootInode: {
			{
				Dentry: fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "bad-type", Inode: 20, Type: fsmeta.InodeTypeFile},
				Inode:  fsmeta.InodeRecord{Inode: 20, Type: fsmeta.InodeTypeDirectory, LinkCount: 1},
			},
			scrubPair(fsmeta.RootInode, "link-a", 21, fsmeta.InodeTypeFile, 1),
			scrubPair(fsmeta.RootInode, "link-b", 21, fsmeta.InodeTypeFile, 1),
		},
	}}

	report, err := scrubMount(context.Background(), cli, "vol", fsmeta.RootInode, 8, 8)
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Contains(t, scrubIssueKinds(report.Issues), issueDentryTypeMismatch)
	require.Contains(t, scrubIssueKinds(report.Issues), issueLinkCountMismatch)
}

func TestScrubMountLimitsIssues(t *testing.T) {
	cli := &fakeScrubClient{dirs: map[fsmeta.InodeID][]fsmeta.DentryAttrPair{
		fsmeta.RootInode: {
			scrubPair(fsmeta.RootInode, "", 10, fsmeta.InodeTypeFile, 2),
			scrubPair(99, "wrong-parent", 11, fsmeta.InodeTypeFile, 2),
		},
	}}

	report, err := scrubMount(context.Background(), cli, "vol", fsmeta.RootInode, 8, 1)
	require.NoError(t, err)
	require.Len(t, report.Issues, 2)
	require.Equal(t, issueIssueLimitExhausted, report.Issues[1].Kind)
}

func TestScrubMountPropagatesReadDirError(t *testing.T) {
	want := errors.New("read failed")
	_, err := scrubMount(context.Background(), &fakeScrubClient{err: want}, "vol", fsmeta.RootInode, 8, 8)
	require.ErrorIs(t, err, want)
}

func scrubPair(parent fsmeta.InodeID, name string, inode fsmeta.InodeID, typ fsmeta.InodeType, links uint32) fsmeta.DentryAttrPair {
	return fsmeta.DentryAttrPair{
		Dentry: fsmeta.DentryRecord{Parent: parent, Name: name, Inode: inode, Type: typ},
		Inode:  fsmeta.InodeRecord{Inode: inode, Type: typ, LinkCount: links},
	}
}

func scrubIssueKinds(issues []scrubIssue) []scrubIssueKind {
	kinds := make([]scrubIssueKind, 0, len(issues))
	for _, issue := range issues {
		kinds = append(kinds, issue.Kind)
	}
	return kinds
}
