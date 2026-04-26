package fsmeta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotPublisherFunc(t *testing.T) {
	token := SnapshotSubtreeToken{Mount: "vol", RootInode: 1, ReadVersion: 10}
	var seen SnapshotSubtreeToken
	publisher := SnapshotPublisherFunc(func(_ context.Context, t SnapshotSubtreeToken) error {
		seen = t
		return nil
	})

	require.NoError(t, publisher.PublishSnapshotSubtree(context.Background(), token))
	require.Equal(t, token, seen)
	require.NoError(t, publisher.RetireSnapshotSubtree(context.Background(), token))

	var nilPublisher SnapshotPublisherFunc
	require.NoError(t, nilPublisher.PublishSnapshotSubtree(context.Background(), token))
}

func TestWatchPrefix(t *testing.T) {
	explicit, err := WatchPrefix(WatchRequest{KeyPrefix: []byte("fsm/custom")})
	require.NoError(t, err)
	require.Equal(t, []byte("fsm/custom"), explicit)
	explicit[0] = 'x'
	prefixAgain, err := WatchPrefix(WatchRequest{KeyPrefix: []byte("fsm/custom")})
	require.NoError(t, err)
	require.Equal(t, byte('f'), prefixAgain[0])

	dentryPrefix, err := WatchPrefix(WatchRequest{Mount: "vol", RootInode: 7})
	require.NoError(t, err)
	want, err := EncodeDentryPrefix("vol", 7)
	require.NoError(t, err)
	require.Equal(t, want, dentryPrefix)

	_, err = WatchPrefix(WatchRequest{Mount: "vol", KeyPrefix: []byte("fsm/custom")})
	require.ErrorIs(t, err, ErrInvalidRequest)
	_, err = WatchPrefix(WatchRequest{Mount: "vol", RootInode: 7, DescendRecursively: true})
	require.ErrorIs(t, err, ErrInvalidRequest)
}
