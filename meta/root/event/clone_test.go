package event_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestCloneEventDetachesCapsuleAuthorityGrant(t *testing.T) {
	grant := rootproto.CapsuleAuthorityGrant{
		GrantID:  "capsule-1",
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{1},
			Parents:    []uint64{10},
			Inodes:     []uint64{20},
		},
		ExpiresUnixNano: 1_000,
	}
	event := rootevent.CloneEvent(rootevent.CapsuleAuthorityGranted(grant))
	event.CapsuleGrant.Scope.Buckets[0] = 9
	event.CapsuleGrant.Scope.Parents[0] = 99
	event.CapsuleGrant.Scope.Inodes[0] = 999

	cloned := rootevent.CloneEvent(event)
	event.CapsuleGrant.Scope.Buckets[0] = 8
	event.CapsuleGrant.Scope.Parents[0] = 88
	event.CapsuleGrant.Scope.Inodes[0] = 888
	require.Equal(t, []uint16{9}, cloned.CapsuleGrant.Scope.Buckets)
	require.Equal(t, []uint64{99}, cloned.CapsuleGrant.Scope.Parents)
	require.Equal(t, []uint64{999}, cloned.CapsuleGrant.Scope.Inodes)
}
