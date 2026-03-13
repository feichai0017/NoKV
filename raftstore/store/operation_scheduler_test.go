package store

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOperationSchedulerAppliesWithCooldown(t *testing.T) {
	var mu sync.Mutex
	var applied []time.Time
	os := newOperationScheduler(8, 20*time.Millisecond, 80*time.Millisecond, 1, func(op Operation) bool {
		mu.Lock()
		applied = append(applied, time.Now())
		mu.Unlock()
		return true
	})
	defer os.stopLoop()

	op := Operation{Type: OperationLeaderTransfer, Region: 9, Source: 1, Target: 2}
	os.enqueue(op)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1
	}, time.Second, 10*time.Millisecond)

	os.enqueue(op)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) >= 2
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	first := applied[0]
	second := applied[1]
	mu.Unlock()
	require.GreaterOrEqual(t, second.Sub(first), 60*time.Millisecond)
}
