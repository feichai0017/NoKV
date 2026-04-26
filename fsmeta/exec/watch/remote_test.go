package watch

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestRemoteSourceHelpers(t *testing.T) {
	require.Equal(t, fsmeta.WatchEventSourceCommit, applyWatchSourceFromProto(kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_COMMIT))
	require.Equal(t, fsmeta.WatchEventSourceResolveLock, applyWatchSourceFromProto(kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_RESOLVE_LOCK))
	require.Zero(t, applyWatchSourceFromProto(kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_UNSPECIFIED))

	require.True(t, isPermanentWatchError(status.Error(codes.FailedPrecondition, "retired")))
	require.True(t, isPermanentWatchError(status.Error(codes.Unimplemented, "disabled")))
	require.False(t, isPermanentWatchError(status.Error(codes.Unavailable, "retry")))

	require.Equal(t, 200*time.Millisecond, nextBackoff(100*time.Millisecond))
	require.Equal(t, remoteWatchMaxBackoff, nextBackoff(remoteWatchMaxBackoff))
	require.NotEmpty(t, normalizeDialOptions(nil))
	custom := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	require.Len(t, normalizeDialOptions(custom), 1)
}

func TestPublishApplyWatchEvent(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{KeyPrefix: []byte("k/")})
	require.NoError(t, err)
	defer sub.Close()

	publishApplyWatchEvent(router, &kvrpcpb.ApplyWatchEvent{
		RegionId:      7,
		Term:          2,
		Index:         3,
		CommitVersion: 99,
		Source:        kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_COMMIT,
		Keys:          [][]byte{[]byte("k/a"), []byte("other")},
	})

	require.Equal(t, fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 7, Term: 2, Index: 3},
		CommitVersion: 99,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/a"),
	}, <-sub.Events())

	publishApplyWatchEvent(router, nil)
	publishApplyWatchEvent(nil, &kvrpcpb.ApplyWatchEvent{Source: kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_COMMIT})
	publishApplyWatchEvent(router, &kvrpcpb.ApplyWatchEvent{
		Source: kvrpcpb.ApplyWatchEventSource_APPLY_WATCH_EVENT_SOURCE_UNSPECIFIED,
		Keys:   [][]byte{[]byte("k/b")},
	})
	select {
	case got := <-sub.Events():
		t.Fatalf("unexpected event after ignored publish: %+v", got)
	default:
	}
}

func TestSleepBackoffHonorsCancellationAndStatsNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, sleepBackoff(ctx, time.Hour))
	require.NoError(t, stopRemoteStore(nil))

	var source *RemoteSource
	require.Equal(t, map[string]any{
		"remote_stores":                       0,
		"apply_observer_dropped_events_total": uint64(0),
	}, source.Stats())
	require.NoError(t, source.Close())
}
