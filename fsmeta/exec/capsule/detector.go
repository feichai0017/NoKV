package capsule

import (
	"bytes"
	"errors"
	"slices"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

var (
	ErrInvalidOperationID = errors.New("fsmeta capsule: invalid operation id")
	ErrDuplicateOperation = errors.New("fsmeta capsule: duplicate operation id")
)

// OperationID is the holder-local idempotency key for one Capsule operation.
// It is deliberately transport-neutral; the eventual wire protocol can encode
// client identity however it wants as long as the pair remains stable.
type OperationID struct {
	ClientID string
	Seq      uint64
}

func (id OperationID) Valid() bool {
	return id.ClientID != "" || id.Seq != 0
}

// ConflictDetector tracks holder-issued, not-yet-sealed operations and returns
// the exact conflict-DAG predecessors for a new semantic delta.
type ConflictDetector struct {
	mu      sync.Mutex
	pending map[OperationID]trackedOperation
	order   []OperationID
}

type trackedOperation struct {
	id     OperationID
	reads  []trackedKey
	writes []trackedKey
}

type trackedKey struct {
	key    []byte
	prefix bool
}

func NewConflictDetector() *ConflictDetector {
	return &ConflictDetector{pending: make(map[OperationID]trackedOperation)}
}

func (d *ConflictDetector) Admit(id OperationID, delta compile.SemanticDelta) ([]OperationID, error) {
	if !id.Valid() {
		return nil, ErrInvalidOperationID
	}
	current := trackedFromDelta(id, delta)

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pending == nil {
		d.pending = make(map[OperationID]trackedOperation)
	}
	if _, ok := d.pending[id]; ok {
		return nil, ErrDuplicateOperation
	}
	predecessors := make([]OperationID, 0)
	for _, predecessorID := range d.order {
		predecessor, ok := d.pending[predecessorID]
		if !ok {
			continue
		}
		if conflicts(predecessor, current) {
			predecessors = append(predecessors, predecessorID)
		}
	}
	d.pending[id] = current
	d.order = append(d.order, id)
	return predecessors, nil
}

func (d *ConflictDetector) Remove(id OperationID) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, id)
	d.order = slices.DeleteFunc(d.order, func(current OperationID) bool {
		return current == id
	})
}

func (d *ConflictDetector) Len() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

func trackedFromDelta(id OperationID, delta compile.SemanticDelta) trackedOperation {
	out := trackedOperation{
		id:     id,
		reads:  make([]trackedKey, 0, len(delta.ReadPredicates)),
		writes: make([]trackedKey, 0, len(delta.WriteEffects)),
	}
	for _, predicate := range delta.ReadPredicates {
		out.reads = append(out.reads, trackedKey{
			key:    cloneBytes(predicate.Key),
			prefix: predicate.Kind == compile.PredicatePrefixScan,
		})
	}
	for _, effect := range delta.WriteEffects {
		out.writes = append(out.writes, trackedKey{key: cloneBytes(effect.Key)})
	}
	return out
}

func conflicts(left, right trackedOperation) bool {
	return keySetsConflict(left.writes, right.writes) ||
		keySetsConflict(left.writes, right.reads) ||
		keySetsConflict(left.reads, right.writes)
}

func keySetsConflict(left, right []trackedKey) bool {
	for _, l := range left {
		for _, r := range right {
			if keysConflict(l, r) {
				return true
			}
		}
	}
	return false
}

func keysConflict(left, right trackedKey) bool {
	if len(left.key) == 0 || len(right.key) == 0 {
		return true
	}
	switch {
	case left.prefix && right.prefix:
		return bytes.HasPrefix(left.key, right.key) || bytes.HasPrefix(right.key, left.key)
	case left.prefix:
		return bytes.HasPrefix(right.key, left.key)
	case right.prefix:
		return bytes.HasPrefix(left.key, right.key)
	default:
		return bytes.Equal(left.key, right.key)
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
