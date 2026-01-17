package compact

import (
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

type fakeExecutor struct {
	priorities []Priority
	doErr      error
	needsSeq   []bool
	adjusted   int
	doCalls    int
	pickCalls  int
}

func (f *fakeExecutor) PickCompactLevels() []Priority {
	f.pickCalls++
	return f.priorities
}

func (f *fakeExecutor) DoCompact(_ int, _ Priority) error {
	f.doCalls++
	return f.doErr
}

func (f *fakeExecutor) NeedsCompaction() bool {
	if len(f.needsSeq) == 0 {
		return false
	}
	val := f.needsSeq[0]
	f.needsSeq = f.needsSeq[1:]
	return val
}

func (f *fakeExecutor) AdjustThrottle() {
	f.adjusted++
}

func TestManagerRunOnceAndCycle(t *testing.T) {
	exec := &fakeExecutor{
		priorities: []Priority{{Level: 0, Score: 1, Adjusted: 1}},
		needsSeq:   []bool{true, false},
	}
	cm := NewManager(exec, 2)
	ok := cm.RunOnce(0)
	require.True(t, ok)
	require.Equal(t, 1, exec.doCalls)

	cm.runCycle(0, "test")
	require.GreaterOrEqual(t, exec.adjusted, 1)

	exec.doErr = utils.ErrFillTables
	require.False(t, cm.run(0, Priority{Level: 1, Adjusted: 1}))

	exec.doErr = errors.New("boom")
	require.False(t, cm.run(0, Priority{Level: 1, Adjusted: 1}))
}

func TestManagerStartClose(t *testing.T) {
	exec := &fakeExecutor{}
	cm := NewManager(exec, 1)
	closeCh := make(chan struct{})
	close(closeCh)
	cm.Start(0, closeCh, nil)
}
