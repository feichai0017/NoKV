package compact

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeExecutor struct {
	priorities []Priority
	doErr      error
	needsSeq   []bool
	adjusted   int
	doCalls    int
	pickCalls  int
	lastDo     Priority
}

func (f *fakeExecutor) PickCompactLevels() []Priority {
	f.pickCalls++
	return f.priorities
}

func (f *fakeExecutor) DoCompact(_ int, p Priority) error {
	f.doCalls++
	f.lastDo = p
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
	cm := NewManager(exec, 2, nil)
	ok := cm.RunOnce(0)
	require.True(t, ok)
	require.Equal(t, 1, exec.doCalls)

	cm.runCycle(0, "test")
	require.GreaterOrEqual(t, exec.adjusted, 1)

	exec.doErr = ErrFillTables
	require.False(t, cm.run(0, Priority{Level: 1, Adjusted: 1}))

	exec.doErr = errors.New("boom")
	require.False(t, cm.run(0, Priority{Level: 1, Adjusted: 1}))
}

func TestManagerStartClose(t *testing.T) {
	exec := &fakeExecutor{}
	cm := NewManager(exec, 1, nil)
	closeCh := make(chan struct{})
	close(closeCh)
	cm.Start(0, closeCh, nil)
}

type reversePolicy struct {
	called int
}

func (p *reversePolicy) Name() string { return "reverse-test" }

func (p *reversePolicy) Arrange(_ int, priorities []Priority) []Priority {
	p.called++
	out := append([]Priority(nil), priorities...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func TestManagerRunOnceUsesPolicyOrdering(t *testing.T) {
	exec := &fakeExecutor{
		priorities: []Priority{
			{Level: 1, Score: 1.2, Adjusted: 1.2},
			{Level: 0, Score: 1.1, Adjusted: 1.1},
		},
	}
	policy := &reversePolicy{}
	cm := NewManager(exec, 1, policy)

	ok := cm.RunOnce(0)
	require.True(t, ok)
	require.Equal(t, 1, policy.called)
	require.Equal(t, 0, exec.lastDo.Level, "policy ordering should decide first tried priority")
}
