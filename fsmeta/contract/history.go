package contract

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
)

const maxConcurrentHistoryBatch = 5

type scheduledOperation struct {
	index int
	op    Operation
}

type observedOperation struct {
	index    int
	op       Operation
	result   Result
	started  int64
	finished int64
}

// RunConcurrentBatches executes generated fsmeta operations in small
// overlapping batches and checks every completed batch against a bounded
// linearization oracle. It is deliberately factorial and bounded: the point is
// to catch non-serializable metadata histories in nightly runs without turning
// the regular PR path into a large model checker.
func RunConcurrentBatches(ctx context.Context, exec Executor, model *Model, ops []Operation, batchSize int) error {
	if exec == nil {
		return errExecutorRequired
	}
	if model == nil {
		return errModelRequired
	}
	if batchSize <= 1 {
		return Run(ctx, exec, model, ops)
	}
	if batchSize > maxConcurrentHistoryBatch {
		return fmt.Errorf("fsmeta/contract: concurrent batch size %d exceeds max %d", batchSize, maxConcurrentHistoryBatch)
	}

	history := make([]string, 0, len(ops))
	batch := make([]scheduledOperation, 0, batchSize)
	batchID := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		observed := executeConcurrentBatch(ctx, exec, model, batch)
		next, order, err := linearizeBatch(model, observed)
		for _, event := range observed {
			history = append(history, fmt.Sprintf("%03d batch=%03d start=%d finish=%d %s -> got=%s",
				event.index, batchID, event.started, event.finished, event.op, summarize(event.result)))
		}
		if err != nil {
			return fmt.Errorf("batch %d failed: %w\nbatch:\n%s\nhistory:\n%s",
				batchID, err, describeObserved(observed), strings.Join(history, "\n"))
		}
		history = append(history, fmt.Sprintf("batch %03d linearized_as=%s", batchID, describeOrder(observed, order)))
		replaceModel(model, next)
		if err := model.CheckInvariants(); err != nil {
			return fmt.Errorf("batch %d corrupted model invariants: %w\nhistory:\n%s", batchID, err, strings.Join(history, "\n"))
		}
		batch = batch[:0]
		batchID++
		return nil
	}

	for i, op := range ops {
		if historyBarrier(op) {
			if err := flush(); err != nil {
				return err
			}
			if err := runSequentialObserved(ctx, exec, model, i, op, &history); err != nil {
				return err
			}
			continue
		}
		batch = append(batch, scheduledOperation{index: i, op: op})
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func historyBarrier(op Operation) bool {
	switch op.Kind {
	case OpAdvanceTime, OpSnapshotSubtree:
		return true
	default:
		return false
	}
}

func runSequentialObserved(ctx context.Context, exec Executor, model *Model, index int, op Operation, history *[]string) error {
	got := execute(ctx, exec, model, op)
	want := applyObserved(model, op, got)
	*history = append(*history, fmt.Sprintf("%03d sequential %s -> got=%s want=%s", index, op, summarize(got), summarize(want)))
	if err := compareResult(got, want); err != nil {
		return fmt.Errorf("step %d failed: %w\nhistory:\n%s", index, err, strings.Join(*history, "\n"))
	}
	if err := model.CheckInvariants(); err != nil {
		return fmt.Errorf("step %d corrupted model invariants: %w\nhistory:\n%s", index, err, strings.Join(*history, "\n"))
	}
	return nil
}

func executeConcurrentBatch(ctx context.Context, exec Executor, model *Model, batch []scheduledOperation) []observedOperation {
	observed := make([]observedOperation, len(batch))
	var seq atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, entry := range batch {
		observed[i].index = entry.index
		observed[i].op = entry.op
		wg.Go(func() {
			<-start
			observed[i].started = seq.Add(1)
			observed[i].result = execute(ctx, exec, model, entry.op)
			observed[i].finished = seq.Add(1)
		})
	}
	close(start)
	wg.Wait()
	return observed
}

func linearizeBatch(base *Model, observed []observedOperation) (*Model, []int, error) {
	used := make([]bool, len(observed))
	order := make([]int, 0, len(observed))
	var firstMismatch error

	var search func(*Model) (*Model, bool)
	search = func(current *Model) (*Model, bool) {
		if len(order) == len(observed) {
			return current, true
		}
		for i := range observed {
			if used[i] || !respectsRealTime(i, used, observed) {
				continue
			}
			next := cloneModel(current)
			want := applyObserved(next, observed[i].op, observed[i].result)
			if err := compareResult(observed[i].result, want); err != nil {
				if firstMismatch == nil {
					firstMismatch = fmt.Errorf("op %03d %s: %w; got=%s want=%s",
						observed[i].index, observed[i].op, err, summarize(observed[i].result), summarize(want))
				}
				continue
			}
			if err := next.CheckInvariants(); err != nil {
				if firstMismatch == nil {
					firstMismatch = fmt.Errorf("op %03d %s corrupts candidate invariants: %w", observed[i].index, observed[i].op, err)
				}
				continue
			}
			used[i] = true
			order = append(order, i)
			if final, ok := search(next); ok {
				return final, true
			}
			order = order[:len(order)-1]
			used[i] = false
		}
		return nil, false
	}

	next, ok := search(cloneModel(base))
	if ok {
		return next, append([]int(nil), order...), nil
	}
	if firstMismatch == nil {
		firstMismatch = fmt.Errorf("no candidate operation respects real-time constraints")
	}
	return nil, nil, fmt.Errorf("no serial order matched observed concurrent results: %w", firstMismatch)
}

func respectsRealTime(candidate int, used []bool, observed []observedOperation) bool {
	for i := range observed {
		if i == candidate || used[i] {
			continue
		}
		if observed[i].finished < observed[candidate].started {
			return false
		}
	}
	return true
}

func applyObserved(model *Model, op Operation, got Result) Result {
	if op.Kind == OpSnapshotSubtree {
		if got.Err == nil {
			return model.ApplySnapshot(op, got.Token)
		}
		return model.Apply(op)
	}
	return model.Apply(op)
}

func cloneModel(in *Model) *Model {
	if in == nil {
		return nil
	}
	out := &Model{
		Mount:       in.Mount,
		Root:        in.Root,
		NowUnixNs:   in.NowUnixNs,
		dentries:    cloneDentries(in.dentries),
		inodes:      cloneInodes(in.inodes),
		sessions:    make(map[fsmeta.SessionID]fsmeta.SessionRecord, len(in.sessions)),
		owners:      make(map[fsmeta.InodeID]fsmeta.SessionRecord, len(in.owners)),
		snapshots:   make(map[uint64]snapshotState, len(in.snapshots)),
		snapshotRef: make(map[int]uint64, len(in.snapshotRef)),
	}
	maps.Copy(out.sessions, in.sessions)
	maps.Copy(out.owners, in.owners)
	maps.Copy(out.snapshotRef, in.snapshotRef)
	for version, snapshot := range in.snapshots {
		out.snapshots[version] = snapshotState{
			dentries: cloneDentries(snapshot.dentries),
			inodes:   cloneInodes(snapshot.inodes),
		}
	}
	return out
}

func replaceModel(dst *Model, src *Model) {
	*dst = *src
}

func describeObserved(observed []observedOperation) string {
	lines := make([]string, 0, len(observed))
	for _, event := range observed {
		lines = append(lines, fmt.Sprintf("%03d start=%d finish=%d %s -> %s",
			event.index, event.started, event.finished, event.op, summarize(event.result)))
	}
	return strings.Join(lines, "\n")
}

func describeOrder(observed []observedOperation, order []int) string {
	parts := make([]string, 0, len(order))
	for _, index := range order {
		parts = append(parts, fmt.Sprintf("%03d", observed[index].index))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
