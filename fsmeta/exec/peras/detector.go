// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"slices"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// OperationID is the holder-local idempotency key for one Peras operation.
// It is deliberately transport-neutral; the eventual wire protocol can encode
// client identity however it wants as long as the pair remains stable.
type OperationID struct {
	ClientID string
	Seq      uint64
}

func (id OperationID) Valid() bool {
	return id.ClientID != "" && id.Seq != 0
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

func (d *ConflictDetector) Admit(id OperationID, op compile.MaterializedOp) ([]OperationID, error) {
	if !id.Valid() {
		return nil, ErrInvalidOperationID
	}
	current := trackedFromMaterialized(id, op)

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

func (d *ConflictDetector) AdmitReplay(op ReplayOperation) error {
	if err := validateVisibleReplayOperation(op); err != nil {
		return err
	}
	current := trackedFromReplay(op)

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
	if _, ok := d.pending[op.OpID]; ok {
		return ErrDuplicateOperation
	}
	d.nextSeq++
	current.seq = d.nextSeq
	d.pending[op.OpID] = current
	d.order = append(d.order, op.OpID)
	d.indexOperation(current)
	return nil
}

func (d *ConflictDetector) Remove(id OperationID) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.removeLocked(id) {
		return
	}
	d.order = slices.DeleteFunc(d.order, func(current OperationID) bool {
		return current == id
	})
}

// RemoveMany retires pending operations and compacts detector order once.
func (d *ConflictDetector) RemoveMany(ids ...OperationID) {
	if d == nil {
		return
	}
	if len(ids) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	removed := make(map[OperationID]struct{}, len(ids))
	for _, id := range ids {
		if d.removeLocked(id) {
			removed[id] = struct{}{}
		}
	}
	if len(removed) == 0 {
		return
	}
	write := 0
	for _, current := range d.order {
		if _, ok := removed[current]; ok {
			continue
		}
		d.order[write] = current
		write++
	}
	for i := write; i < len(d.order); i++ {
		d.order[i] = OperationID{}
	}
	d.order = d.order[:write]
}

func (d *ConflictDetector) removeLocked(id OperationID) bool {
	op, ok := d.pending[id]
	if !ok {
		return false
	}
	d.unindexOperation(op)
	delete(d.pending, id)
	return true
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
	return d.IDsLimit(0)
}

// IDsLimit returns pending operation IDs in admission order. A non-positive
// limit returns every pending ID.
func (d *ConflictDetector) IDsLimit(maxIDs int) []OperationID {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	capacity := len(d.order)
	if maxIDs > 0 && maxIDs < capacity {
		capacity = maxIDs
	}
	out := make([]OperationID, 0, capacity)
	for _, id := range d.order {
		if _, ok := d.pending[id]; ok {
			out = append(out, id)
			if maxIDs > 0 && len(out) >= maxIDs {
				break
			}
		}
	}
	return out
}

func trackedFromReplay(op ReplayOperation) trackedOperation {
	out := trackedOperation{
		id:     op.OpID,
		reads:  []trackedKey{newTrackedKey(nil, false)},
		writes: make([]trackedKey, 0, len(op.Mutations)),
	}
	for _, mutation := range op.Mutations {
		out.writes = append(out.writes, newTrackedKey(mutation.Key, false))
	}
	return out
}

func trackedFromMaterialized(id OperationID, op compile.MaterializedOp) trackedOperation {
	out := trackedOperation{
		id:     id,
		reads:  make([]trackedKey, 0, len(op.Footprint.Reads)),
		writes: make([]trackedKey, 0, len(op.Footprint.Writes)),
	}
	for _, ref := range op.Footprint.Reads {
		out.reads = append(out.reads, newTrackedKey(ref.Key, ref.Mode == compile.KeyAccessReadPrefix))
	}
	for _, ref := range op.Footprint.Writes {
		out.writes = append(out.writes, newTrackedKey(ref.Key, false))
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
