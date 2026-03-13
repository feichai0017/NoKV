package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreOperationCooldown(t *testing.T) {
	st := &Store{
		operationInput:     make(chan Operation, 8),
		operationStop:      make(chan struct{}),
		operationInterval:  20 * time.Millisecond,
		operationCooldown:  80 * time.Millisecond,
		operationBurst:     1,
		operationPending:   make(map[operationKey]struct{}),
		operationLastApply: make(map[operationKey]time.Time),
	}
	st.operationWG.Add(1)
	go st.runOperationLoop()
	defer st.stopOperationLoop()

	op := Operation{Type: OperationLeaderTransfer, Region: 9, Source: 1, Target: 2}
	key := operationKey{region: op.Region, typeID: op.Type}

	st.enqueueOperation(op)
	require.Eventually(t, func() bool {
		st.operationMu.Lock()
		defer st.operationMu.Unlock()
		return !st.operationLastApply[key].IsZero()
	}, time.Second, 10*time.Millisecond)

	st.operationMu.Lock()
	first := st.operationLastApply[key]
	st.operationMu.Unlock()

	st.enqueueOperation(op)
	require.Eventually(t, func() bool {
		st.operationMu.Lock()
		defer st.operationMu.Unlock()
		return st.operationLastApply[key].After(first)
	}, time.Second, 10*time.Millisecond)

	st.operationMu.Lock()
	second := st.operationLastApply[key]
	st.operationMu.Unlock()
	require.GreaterOrEqual(t, second.Sub(first), 60*time.Millisecond)
}

func TestStoreSchedulerStatusTracksQueueDrop(t *testing.T) {
	st := &Store{
		operationInput:     make(chan Operation, 1),
		operationPending:   make(map[operationKey]struct{}),
		operationLastApply: make(map[operationKey]time.Time),
	}
	st.operationInput <- Operation{Type: OperationLeaderTransfer, Region: 1, Source: 1, Target: 2}

	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 2, Source: 3, Target: 4})

	status := st.SchedulerStatus()
	require.True(t, status.Degraded)
	require.Equal(t, uint64(1), status.DroppedOperations)
	require.Contains(t, status.LastError, "scheduler queue full")

	<-st.operationInput
	st.enqueueOperation(Operation{Type: OperationLeaderTransfer, Region: 3, Source: 5, Target: 6})

	status = st.SchedulerStatus()
	require.False(t, status.Degraded)
	require.Equal(t, uint64(1), status.DroppedOperations)
}
