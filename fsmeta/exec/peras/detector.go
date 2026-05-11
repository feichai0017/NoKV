package peras

import (
	"bytes"
	"errors"
	"slices"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

var (
	ErrInvalidOperationID = errors.New("fsmeta peras: invalid operation id")
	ErrDuplicateOperation = errors.New("fsmeta peras: duplicate operation id")
)

// OperationID is the holder-local idempotency key for one Peras operation.
// It is deliberately transport-neutral; the eventual wire protocol can encode
// client identity however it wants as long as the pair remains stable.
type OperationID struct {
	ClientID string
	Seq      uint64
}

func (id OperationID) Valid() bool {
	return id.ClientID != "" || id.Seq != 0
}

// ConflictDetector tracks holder-issued, not-yet-applied operations and returns
// the exact committed predecessors needed to preserve semantic order.
type ConflictDetector struct {
	mu            sync.Mutex
	pending       map[OperationID]trackedOperation
	order         []OperationID
	readByKey     map[string][]OperationID
	writeByKey    map[string][]OperationID
	broadKeyCount int
	nextSeq       uint64
}

type trackedOperation struct {
	id     OperationID
	seq    uint64
	reads  []trackedKey
	writes []trackedKey
}

type trackedKey struct {
	key    []byte
	index  string
	prefix bool
}

func NewConflictDetector() *ConflictDetector {
	return &ConflictDetector{
		pending:    make(map[OperationID]trackedOperation),
		readByKey:  make(map[string][]OperationID),
		writeByKey: make(map[string][]OperationID),
	}
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
	if d.readByKey == nil {
		d.readByKey = make(map[string][]OperationID)
	}
	if d.writeByKey == nil {
		d.writeByKey = make(map[string][]OperationID)
	}
	if _, ok := d.pending[id]; ok {
		return nil, ErrDuplicateOperation
	}
	var predecessors []OperationID
	if d.mustScanAll(current) {
		predecessors = d.scanPredecessors(current)
	} else {
		predecessors = d.indexedPredecessors(current)
	}
	d.nextSeq++
	current.seq = d.nextSeq
	d.pending[id] = current
	d.order = append(d.order, id)
	d.indexOperation(current)
	return predecessors, nil
}

func (d *ConflictDetector) Remove(id OperationID) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if op, ok := d.pending[id]; ok {
		d.unindexOperation(op)
	}
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

func (d *ConflictDetector) IDs() []OperationID {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]OperationID, 0, len(d.order))
	for _, id := range d.order {
		if _, ok := d.pending[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

func trackedFromDelta(id OperationID, delta compile.SemanticDelta) trackedOperation {
	out := trackedOperation{
		id:     id,
		reads:  make([]trackedKey, 0, len(delta.ReadPredicates)),
		writes: make([]trackedKey, 0, len(delta.WriteEffects)),
	}
	for _, predicate := range delta.ReadPredicates {
		out.reads = append(out.reads, newTrackedKey(predicate.Key, predicate.Kind == compile.PredicatePrefixScan))
	}
	for _, effect := range delta.WriteEffects {
		out.writes = append(out.writes, newTrackedKey(effect.Key, false))
	}
	return out
}

func newTrackedKey(key []byte, prefix bool) trackedKey {
	out := trackedKey{
		key:    cloneBytes(key),
		prefix: prefix,
	}
	if !prefix && len(out.key) > 0 {
		out.index = string(out.key)
	}
	return out
}

func (d *ConflictDetector) mustScanAll(current trackedOperation) bool {
	return d.broadKeyCount > 0 || hasBroadKey(current)
}

func (d *ConflictDetector) scanPredecessors(current trackedOperation) []OperationID {
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
	return predecessors
}

func (d *ConflictDetector) indexedPredecessors(current trackedOperation) []OperationID {
	var candidates map[OperationID]struct{}
	for _, write := range current.writes {
		candidates = addCandidateIDs(candidates, d.writeByKey[write.index])
		candidates = addCandidateIDs(candidates, d.readByKey[write.index])
	}
	for _, read := range current.reads {
		candidates = addCandidateIDs(candidates, d.writeByKey[read.index])
	}
	if len(candidates) == 0 {
		return nil
	}
	predecessors := make([]OperationID, 0, len(candidates))
	for id := range candidates {
		if _, ok := d.pending[id]; ok {
			predecessors = append(predecessors, id)
		}
	}
	sort.Slice(predecessors, func(i, j int) bool {
		return d.pending[predecessors[i]].seq < d.pending[predecessors[j]].seq
	})
	return predecessors
}

func (d *ConflictDetector) indexOperation(op trackedOperation) {
	if hasBroadKey(op) {
		d.broadKeyCount++
		return
	}
	for _, read := range op.reads {
		d.readByKey[read.index] = append(d.readByKey[read.index], op.id)
	}
	for _, write := range op.writes {
		d.writeByKey[write.index] = append(d.writeByKey[write.index], op.id)
	}
}

func (d *ConflictDetector) unindexOperation(op trackedOperation) {
	if hasBroadKey(op) {
		if d.broadKeyCount > 0 {
			d.broadKeyCount--
		}
		return
	}
	for _, read := range op.reads {
		d.readByKey[read.index] = removeIndexedID(d.readByKey[read.index], op.id)
		if len(d.readByKey[read.index]) == 0 {
			delete(d.readByKey, read.index)
		}
	}
	for _, write := range op.writes {
		d.writeByKey[write.index] = removeIndexedID(d.writeByKey[write.index], op.id)
		if len(d.writeByKey[write.index]) == 0 {
			delete(d.writeByKey, write.index)
		}
	}
}

func hasBroadKey(op trackedOperation) bool {
	for _, key := range op.reads {
		if key.prefix || len(key.key) == 0 {
			return true
		}
	}
	for _, key := range op.writes {
		if key.prefix || len(key.key) == 0 {
			return true
		}
	}
	return false
}

func addCandidateIDs(out map[OperationID]struct{}, ids []OperationID) map[OperationID]struct{} {
	if len(ids) == 0 {
		return out
	}
	if out == nil {
		out = make(map[OperationID]struct{}, len(ids))
	}
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func removeIndexedID(ids []OperationID, target OperationID) []OperationID {
	return slices.DeleteFunc(ids, func(id OperationID) bool {
		return id == target
	})
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
