package store

import (
	"testing"
	"time"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/stretchr/testify/require"
)

type channelApplyObserver struct {
	ch chan ApplyEvent
}

func (o *channelApplyObserver) OnApply(evt ApplyEvent) {
	o.ch <- evt
}

type blockingApplyObserver struct {
	entered chan struct{}
	release chan struct{}
}

func (o *blockingApplyObserver) OnApply(ApplyEvent) {
	select {
	case o.entered <- struct{}{}:
	default:
	}
	<-o.release
}

func TestStoreApplyObserverReceivesCommandPipelineEvents(t *testing.T) {
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{
			Header: req.GetHeader(),
			Responses: []*raftcmdpb.Response{{
				Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{}},
			}},
		}, nil
	}
	st := NewStore(Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(st.Close)
	observer := &channelApplyObserver{ch: make(chan ApplyEvent, 1)}
	reg, err := st.RegisterApplyObserver(observer, 4)
	require.NoError(t, err)
	t.Cleanup(reg.Close)

	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: 9, RequestId: 1},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_COMMIT,
			Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
				Keys: [][]byte{[]byte("dentry")}, CommitVersion: 33,
			}},
		}},
	}
	payload, err := command.Encode(req)
	require.NoError(t, err)

	require.NoError(t, st.applyEntries([]myraft.Entry{{Type: myraft.EntryNormal, Term: 4, Index: 12, Data: payload}}))
	select {
	case evt := <-observer.ch:
		require.Equal(t, uint64(9), evt.RegionID)
		require.Equal(t, uint64(4), evt.Term)
		require.Equal(t, uint64(12), evt.Index)
		require.Equal(t, ApplyEventSourceCommit, evt.Source)
		require.Equal(t, uint64(33), evt.CommitVersion)
		require.Equal(t, [][]byte{[]byte("dentry")}, evt.Keys)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for apply observer event")
	}
}

func TestStoreApplyObserverDropsWhenSubscriberIsSlow(t *testing.T) {
	st := NewStore(Config{StoreID: 1})
	t.Cleanup(st.Close)
	observer := &blockingApplyObserver{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	reg, err := st.RegisterApplyObserver(observer, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		close(observer.release)
		reg.Close()
	})

	evt := ApplyEvent{RegionID: 1, Term: 1, Index: 1, Source: ApplyEventSourceCommit, CommitVersion: 2, Keys: [][]byte{[]byte("a")}}
	st.observers.Emit(evt)
	select {
	case <-observer.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observer to block")
	}
	st.observers.Emit(evt)
	st.observers.Emit(evt)

	require.Eventually(t, func() bool {
		return st.DroppedApplyObserverEvents() > 0
	}, time.Second, 10*time.Millisecond)
}
