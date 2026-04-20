package store

import (
	"testing"
	"time"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/stretchr/testify/require"
)

func TestStoreOperationCooldown(t *testing.T) {
	st := &Store{
		sched: &schedulerRuntime{
			operation: operationRuntime{
				input:     make(chan scheduledOp, 8),
				stop:      make(chan struct{}),
				interval:  20 * time.Millisecond,
				cooldown:  80 * time.Millisecond,
				burst:     1,
				pending:   make(map[operationKey]bool),
				lastApply: make(map[operationKey]time.Time),
			},
		},
	}
	st.sched.operation.wg.Add(1)
	go st.runOperationLoop()
	defer st.stopOperationLoop()

	op := Operation{Type: OperationLeaderTransfer, Region: 9, Source: 1, Target: 2}
	key := operationKey{region: op.Region, typeID: op.Type}

	st.enqueueOperation(op)
	require.Eventually(t, func() bool {
		st.sched.operation.mu.Lock()
		defer st.sched.operation.mu.Unlock()
		return !st.sched.operation.lastApply[key].IsZero()
	}, time.Second, 10*time.Millisecond)

	st.sched.operation.mu.Lock()
	first := st.sched.operation.lastApply[key]
	st.sched.operation.mu.Unlock()

	st.enqueueOperation(op)
	require.Eventually(t, func() bool {
		st.sched.operation.mu.Lock()
		defer st.sched.operation.mu.Unlock()
		return st.sched.operation.lastApply[key].After(first)
	}, time.Second, 10*time.Millisecond)

	st.sched.operation.mu.Lock()
	second := st.sched.operation.lastApply[key]
	st.sched.operation.mu.Unlock()
	require.GreaterOrEqual(t, second.Sub(first), 60*time.Millisecond)
}

func TestStoreSchedulerStatusTracksQueueDrop(t *testing.T) {
	st := &Store{
		sched: &schedulerRuntime{
			operation: operationRuntime{
				input:     make(chan scheduledOp, 1),
				pending:   make(map[operationKey]bool),
				lastApply: make(map[operationKey]time.Time),
			},
		},
	}
	st.sched.operation.input <- scheduledOp{op: Operation{Type: OperationLeaderTransfer, Region: 1, Source: 1, Target: 2}}

	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 2, Source: 3, Target: 4})

	status := st.SchedulerStatus()
	require.True(t, status.Degraded)
	require.Equal(t, SchedulerModeDegraded, status.Mode)
	require.Equal(t, uint64(1), status.DroppedOperations)
	require.Contains(t, status.LastError, "scheduler queue full")

	<-st.sched.operation.input
	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 3, Source: 5, Target: 6})

	status = st.SchedulerStatus()
	require.False(t, status.Degraded)
	require.Equal(t, SchedulerModeHealthy, status.Mode)
	require.Equal(t, uint64(1), status.DroppedOperations)
}

func TestStoreCloseReportsDroppedOperationsToScheduler(t *testing.T) {
	sink := newTestSchedulerSink()
	st := NewStore(Config{
		Scheduler:          sink,
		StoreID:            9,
		HeartbeatInterval:  time.Hour,
		OperationQueueSize: 8,
		OperationInterval:  time.Hour,
	})

	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 11, Source: 1, Target: 2})
	st.Close()

	stores := sink.StoreSnapshot()
	require.Len(t, stores, 1)
	require.Equal(t, uint64(9), stores[0].StoreID)
	require.Equal(t, uint64(1), stores[0].DroppedOperations)
}

func TestStoreCloseKeepsDurableSchedulerOperations(t *testing.T) {
	_, localMeta := openStoreDB(t)
	st := NewStore(Config{
		LocalMeta:          localMeta,
		OperationQueueSize: 8,
		OperationInterval:  time.Hour,
	})

	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 11, Source: 1, Target: 2})
	require.Eventually(t, func() bool {
		return len(localMeta.PendingSchedulerOperations()) == 1
	}, time.Second, 10*time.Millisecond)
	st.Close()

	require.Len(t, localMeta.PendingSchedulerOperations(), 1)
}

func TestStoreDropsDurableSchedulerOperationAfterAttemptLimit(t *testing.T) {
	_, localMeta := openStoreDB(t)
	require.NoError(t, localMeta.SavePendingSchedulerOperation(localmeta.PendingSchedulerOperation{
		Kind:         localmeta.PendingSchedulerOperationLeaderTransfer,
		RegionID:     17,
		SourcePeerID: 0,
		TargetPeerID: 2,
		Attempts:     maxSchedulerOperationAttempts - 1,
	}))
	st := NewStore(Config{
		LocalMeta:          localMeta,
		OperationQueueSize: 8,
		OperationInterval:  10 * time.Millisecond,
	})
	defer st.Close()

	require.Eventually(t, func() bool {
		return len(localMeta.PendingSchedulerOperations()) == 0
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(1), st.SchedulerStatus().DroppedOperations)
}
