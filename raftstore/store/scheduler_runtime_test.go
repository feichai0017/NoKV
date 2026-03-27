package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreOperationCooldown(t *testing.T) {
	st := &Store{
		sched: &schedulerRuntime{
			input:     make(chan Operation, 8),
			stop:      make(chan struct{}),
			interval:  20 * time.Millisecond,
			cooldown:  80 * time.Millisecond,
			burst:     1,
			pending:   make(map[operationKey]struct{}),
			lastApply: make(map[operationKey]time.Time),
		},
	}
	st.sched.wg.Add(1)
	go st.runOperationLoop()
	defer st.stopOperationLoop()

	op := Operation{Type: OperationLeaderTransfer, Region: 9, Source: 1, Target: 2}
	key := operationKey{region: op.Region, typeID: op.Type}

	st.enqueueOperation(op)
	require.Eventually(t, func() bool {
		st.sched.mu.Lock()
		defer st.sched.mu.Unlock()
		return !st.sched.lastApply[key].IsZero()
	}, time.Second, 10*time.Millisecond)

	st.sched.mu.Lock()
	first := st.sched.lastApply[key]
	st.sched.mu.Unlock()

	st.enqueueOperation(op)
	require.Eventually(t, func() bool {
		st.sched.mu.Lock()
		defer st.sched.mu.Unlock()
		return st.sched.lastApply[key].After(first)
	}, time.Second, 10*time.Millisecond)

	st.sched.mu.Lock()
	second := st.sched.lastApply[key]
	st.sched.mu.Unlock()
	require.GreaterOrEqual(t, second.Sub(first), 60*time.Millisecond)
}

func TestStoreSchedulerStatusTracksQueueDrop(t *testing.T) {
	st := &Store{
		sched: &schedulerRuntime{
			input:     make(chan Operation, 1),
			pending:   make(map[operationKey]struct{}),
			lastApply: make(map[operationKey]time.Time),
		},
	}
	st.sched.input <- Operation{Type: OperationLeaderTransfer, Region: 1, Source: 1, Target: 2}

	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 2, Source: 3, Target: 4})

	status := st.SchedulerStatus()
	require.True(t, status.Degraded)
	require.Equal(t, uint64(1), status.DroppedOperations)
	require.Contains(t, status.LastError, "scheduler queue full")

	<-st.sched.input
	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 3, Source: 5, Target: 6})

	status = st.SchedulerStatus()
	require.False(t, status.Degraded)
	require.Equal(t, uint64(1), status.DroppedOperations)
}
