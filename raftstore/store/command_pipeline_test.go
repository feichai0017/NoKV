package store

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
)

func mustCommandEntry(t testing.TB, regionID, peerID, requestID uint64) myraft.Entry {
	t.Helper()
	return mustCommandEntryWithRequests(t, regionID, peerID, requestID)
}

func mustCommandEntryWithRequests(t testing.TB, regionID, peerID, requestID uint64, requests ...*raftcmdpb.Request) myraft.Entry {
	t.Helper()
	payload, err := command.Encode(&raftcmdpb.RaftCmdRequest{
		Header:   &raftcmdpb.CmdHeader{RegionId: regionID, PeerId: peerID, RequestId: requestID},
		Requests: requests,
	})
	require.NoError(t, err)
	return myraft.Entry{Type: myraft.EntryNormal, Data: payload}
}

func testProposalKey(regionID, peerID, requestID uint64) commandProposalKey {
	return commandProposalKey{regionID: regionID, peerID: peerID, requestID: requestID}
}

func TestCommandPipelineApplyEntriesReturnsApplyError(t *testing.T) {
	applyErr := errors.New("apply boom")
	var applied []uint64
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		applied = append(applied, req.GetHeader().GetRequestId())
		return nil, applyErr
	}, 1)

	prop1, err := cp.registerProposal(testProposalKey(1, 101, 11))
	require.NoError(t, err)
	prop2, err := cp.registerProposal(testProposalKey(1, 101, 22))
	require.NoError(t, err)
	require.NotNil(t, prop1)
	require.NotNil(t, prop2)

	err = cp.applyEntries([]myraft.Entry{
		mustCommandEntry(t, 1, 101, 11),
		mustCommandEntry(t, 1, 101, 22),
	})
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, []uint64{11}, applied)

	result := <-prop1.ch
	require.ErrorIs(t, result.err, applyErr)
	require.Nil(t, result.resp)

	select {
	case <-prop2.ch:
		t.Fatal("second proposal should not complete after apply failure")
	default:
	}
}

func TestCommandPipelineRegisterProposalRejectsDuplicateID(t *testing.T) {
	cp := newCommandPipeline(nil)

	key := testProposalKey(2, 202, 7)
	first, err := cp.registerProposal(key)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := cp.registerProposal(key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate proposal id")
	require.Nil(t, second)

	cp.completeProposal(key, &raftcmdpb.RaftCmdResponse{}, nil)
	result := <-first.ch
	require.NoError(t, result.err)
	require.NotNil(t, result.resp)
}

func TestCommandPipelineRemoveProposalDropsPendingResult(t *testing.T) {
	cp := newCommandPipeline(nil)

	key := testProposalKey(3, 303, 9)
	prop, err := cp.registerProposal(key)
	require.NoError(t, err)
	require.NotNil(t, prop)

	cp.removeProposal(key)
	cp.completeProposal(key, &raftcmdpb.RaftCmdResponse{}, nil)

	select {
	case <-prop.ch:
		t.Fatal("removed proposal should not receive a completion result")
	default:
	}
}

func TestCommandPipelineIgnoresForeignPeerRequestIDCollision(t *testing.T) {
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	})

	localKey := testProposalKey(7, 701, 1)
	prop, err := cp.registerProposal(localKey)
	require.NoError(t, err)
	require.NotNil(t, prop)

	require.NoError(t, cp.applyEntries([]myraft.Entry{
		mustCommandEntry(t, 7, 702, 1),
	}))

	select {
	case <-prop.ch:
		t.Fatal("foreign peer entry with colliding request id completed local proposal")
	default:
	}

	cp.completeProposal(localKey, &raftcmdpb.RaftCmdResponse{}, nil)
	result := <-prop.ch
	require.NoError(t, result.err)
	require.NotNil(t, result.resp)
}

func TestCommandPipelineRejectsUnframedPayload(t *testing.T) {
	cp := newCommandPipeline(func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		t.Fatal("unframed payload must not reach applier")
		return nil, nil
	})

	err := cp.applyEntries([]myraft.Entry{
		{Type: myraft.EntryNormal, Data: []byte("legacy-payload")},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported unframed raft payload")
}

func TestCommandPipelineAppliesDisjointCommandsInParallel(t *testing.T) {
	started := make(chan uint64, 2)
	release := make(chan struct{})
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		started <- req.GetHeader().GetRequestId()
		<-release
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 2)

	done := make(chan error, 1)
	go func() {
		done <- cp.applyEntries([]myraft.Entry{
			mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("a"))),
			mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("b"))),
		})
	}()

	require.Eventually(t, func() bool {
		return len(started) == 2
	}, time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-done)
}

func TestCommandPipelineBatchesReadyMVCCCommands(t *testing.T) {
	var singleCalls atomic.Int32
	var batchCalls atomic.Int32
	var batchSizes []int
	cp := newCommandPipelineWithBatch(
		func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
			singleCalls.Add(1)
			return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
		},
		func(reqs []*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error) {
			batchCalls.Add(1)
			batchSizes = append(batchSizes, len(reqs))
			resps := make([]*raftcmdpb.RaftCmdResponse, len(reqs))
			for i, req := range reqs {
				resps[i] = &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}
			}
			return resps, nil
		},
		4,
	)

	require.NoError(t, cp.applyEntries([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("batch-a"))),
		mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("batch-b"))),
		mustCommandEntryWithRequests(t, 1, 101, 3, testPrewriteRequest([]byte("batch-c"))),
	}))
	require.Zero(t, singleCalls.Load())
	require.Equal(t, int32(1), batchCalls.Load())
	require.Equal(t, []int{3}, batchSizes)
}

func TestCommandPipelineSerializesConflictingCommands(t *testing.T) {
	started := make(chan uint64, 2)
	releaseFirst := make(chan struct{})
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		id := req.GetHeader().GetRequestId()
		started <- id
		if id == 1 {
			<-releaseFirst
		}
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 2)

	done := make(chan error, 1)
	go func() {
		done <- cp.applyEntries([]myraft.Entry{
			mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("same-key"))),
			mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("same-key"))),
		})
	}()

	require.Equal(t, uint64(1), <-started)
	select {
	case id := <-started:
		t.Fatalf("conflicting command %d started before first command completed", id)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	require.Equal(t, uint64(2), <-started)
	require.NoError(t, <-done)
}

func TestCommandPipelineApplyWindowSchedulesAcrossBatches(t *testing.T) {
	started := make(chan uint64, 2)
	release := make(chan struct{})
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		started <- req.GetHeader().GetRequestId()
		<-release
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 2)

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("a"))),
	}, nil, func(err error) { done1 <- err }))
	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("b"))),
	}, nil, func(err error) { done2 <- err }))

	require.Eventually(t, func() bool {
		return len(started) == 2
	}, time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-done1)
	require.NoError(t, <-done2)
}

func TestCommandPipelineAsyncSerialParallelismOnePreservesOrder(t *testing.T) {
	started := make(chan uint64, 2)
	releaseFirst := make(chan struct{})
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		id := req.GetHeader().GetRequestId()
		started <- id
		if id == 1 {
			<-releaseFirst
		}
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 1)

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("a"))),
	}, nil, func(err error) { done1 <- err }))
	require.Equal(t, uint64(1), <-started)

	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("b"))),
	}, nil, func(err error) { done2 <- err }))
	select {
	case id := <-started:
		t.Fatalf("serial async command %d started before first completed", id)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	require.Equal(t, uint64(2), <-started)
	require.NoError(t, <-done1)
	require.NoError(t, <-done2)
}

func TestCommandPipelineApplyWindowSerializesConflictsAcrossBatches(t *testing.T) {
	started := make(chan uint64, 2)
	releaseFirst := make(chan struct{})
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		id := req.GetHeader().GetRequestId()
		started <- id
		if id == 1 {
			<-releaseFirst
		}
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 2)

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("same-key"))),
	}, nil, func(err error) { done1 <- err }))
	require.Equal(t, uint64(1), <-started)

	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("same-key"))),
	}, nil, func(err error) { done2 <- err }))
	select {
	case id := <-started:
		t.Fatalf("conflicting command %d started before first command completed", id)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	require.Equal(t, uint64(2), <-started)
	require.NoError(t, <-done1)
	require.NoError(t, <-done2)
}

func TestCommandPipelineApplyWindowCompletesInSubmissionOrder(t *testing.T) {
	started := make(chan uint64, 2)
	releaseFirst := make(chan struct{})
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		id := req.GetHeader().GetRequestId()
		started <- id
		if id == 1 {
			<-releaseFirst
		}
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 2)

	prop1, err := cp.registerProposal(testProposalKey(1, 101, 1))
	require.NoError(t, err)
	prop2, err := cp.registerProposal(testProposalKey(1, 101, 2))
	require.NoError(t, err)

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("a"))),
	}, nil, func(err error) { done1 <- err }))
	require.NoError(t, cp.applyEntriesAsync([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("b"))),
	}, nil, func(err error) { done2 <- err }))

	require.Eventually(t, func() bool {
		return len(started) == 2
	}, time.Second, time.Millisecond)
	select {
	case <-prop2.ch:
		t.Fatal("second proposal completed before the first order slot")
	case <-done2:
		t.Fatal("second apply callback completed before the first order slot")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)

	result1 := <-prop1.ch
	require.NoError(t, result1.err)
	result2 := <-prop2.ch
	require.NoError(t, result2.err)
	require.NoError(t, <-done1)
	require.NoError(t, <-done2)
}

func TestCommandPipelineFatalParallelApplyErrorCompletesWholeWave(t *testing.T) {
	applyErr := errors.New("disk write failed")
	var calls atomic.Int32
	cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		if calls.Add(1) == 1 {
			return nil, applyErr
		}
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}, 3)

	props := make([]*commandProposal, 0, 3)
	for i := 1; i <= 3; i++ {
		prop, err := cp.registerProposal(testProposalKey(1, 101, uint64(i)))
		require.NoError(t, err)
		props = append(props, prop)
	}

	err := cp.applyEntries([]myraft.Entry{
		mustCommandEntryWithRequests(t, 1, 101, 1, testPrewriteRequest([]byte("a"))),
		mustCommandEntryWithRequests(t, 1, 101, 2, testPrewriteRequest([]byte("b"))),
		mustCommandEntryWithRequests(t, 1, 101, 3, testPrewriteRequest([]byte("c"))),
	})
	require.ErrorIs(t, err, applyErr)
	require.ErrorContains(t, err, "fatal apply window failed")

	for _, prop := range props {
		result := <-prop.ch
		require.ErrorIs(t, result.err, applyErr)
		require.Nil(t, result.resp)
	}
}

func TestCommandRuntimeHelpers(t *testing.T) {
	var nilStore *Store
	require.NotNil(t, nilStore.runtimeContext())
	require.Error(t, nilStore.applyEntries(nil))

	applier := func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{}, nil
	}
	st := NewStore(Config{CommandApplier: applier, CommandTimeout: 2 * time.Second})
	t.Cleanup(func() { st.Close() })

	require.NotNil(t, st.cmds)
	require.Equal(t, 2*time.Second, st.cmds.timeout)
	require.Equal(t, context.Background().Err(), st.runtimeContext().Err())
	require.NoError(t, st.applyEntries([]myraft.Entry{}))

	empty := NewStore(Config{})
	t.Cleanup(func() { empty.Close() })
	require.NoError(t, empty.applyEntries([]myraft.Entry{}))
}

func BenchmarkCommandPipelineApplyEntries(b *testing.B) {
	entries := make([]myraft.Entry, 32)
	for i := range entries {
		key := fmt.Appendf(nil, "bench-key-%02d", i)
		entries[i] = mustCommandEntryWithRequests(b, 1, 101, uint64(i+1), testPrewriteRequest(key))
	}
	for _, tc := range []struct {
		name        string
		parallelism int
	}{
		{name: "serial", parallelism: 1},
		{name: "parallel_4", parallelism: 4},
		{name: "parallel_8", parallelism: 8},
	} {
		b.Run(tc.name, func(b *testing.B) {
			cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
				time.Sleep(50 * time.Microsecond)
				return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
			}, tc.parallelism)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cp.applyEntries(entries); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCommandPipelineApplyWindowBatches(b *testing.B) {
	entries := make([]myraft.Entry, 32)
	for i := range entries {
		key := fmt.Appendf(nil, "window-bench-key-%02d", i)
		entries[i] = mustCommandEntryWithRequests(b, 1, 101, uint64(i+1), testPrewriteRequest(key))
	}
	b.Run("serial_batches", func(b *testing.B) {
		cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
			time.Sleep(50 * time.Microsecond)
			return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
		}, 1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, entry := range entries {
				if err := cp.applyEntries([]myraft.Entry{entry}); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("window_8_batches", func(b *testing.B) {
		cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
			time.Sleep(50 * time.Microsecond)
			return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
		}, 8)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			done := make(chan error, len(entries))
			for _, entry := range entries {
				if err := cp.applyEntriesAsync([]myraft.Entry{entry}, nil, func(err error) {
					done <- err
				}); err != nil {
					b.Fatal(err)
				}
			}
			for range entries {
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkCommandPipelineBatchApplier(b *testing.B) {
	entries := make([]myraft.Entry, 32)
	for i := range entries {
		key := fmt.Appendf(nil, "batch-applier-key-%02d", i)
		entries[i] = mustCommandEntryWithRequests(b, 1, 101, uint64(i+1), testPrewriteRequest(key))
	}
	b.Run("single_applier", func(b *testing.B) {
		cp := newCommandPipeline(func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
			time.Sleep(50 * time.Microsecond)
			return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
		}, 8)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cp.applyEntries(entries); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("batch_applier", func(b *testing.B) {
		cp := newCommandPipelineWithBatch(
			func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
				time.Sleep(50 * time.Microsecond)
				return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
			},
			func(reqs []*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error) {
				time.Sleep(50 * time.Microsecond)
				resps := make([]*raftcmdpb.RaftCmdResponse, len(reqs))
				for i, req := range reqs {
					resps[i] = &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}
				}
				return resps, nil
			},
			8,
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cp.applyEntries(entries); err != nil {
				b.Fatal(err)
			}
		}
	})
}
