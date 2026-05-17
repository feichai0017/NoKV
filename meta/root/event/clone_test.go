// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestCloneEventDetachesVisibleAuthorityGrant(t *testing.T) {
	grant := rootproto.VisibleAuthorityGrant{
		GrantID:  "visible-1",
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{1},
			Parents:    []uint64{10},
			Inodes:     []uint64{20},
		},
		ExpiresUnixNano: 1_000,
	}
	event := rootevent.CloneEvent(rootevent.VisibleAuthorityGranted(grant))
	event.VisibleGrant.Scope.Buckets[0] = 9
	event.VisibleGrant.Scope.Parents[0] = 99
	event.VisibleGrant.Scope.Inodes[0] = 999

	cloned := rootevent.CloneEvent(event)
	event.VisibleGrant.Scope.Buckets[0] = 8
	event.VisibleGrant.Scope.Parents[0] = 88
	event.VisibleGrant.Scope.Inodes[0] = 888
	require.Equal(t, []uint16{9}, cloned.VisibleGrant.Scope.Buckets)
	require.Equal(t, []uint64{99}, cloned.VisibleGrant.Scope.Parents)
	require.Equal(t, []uint64{999}, cloned.VisibleGrant.Scope.Inodes)
}
