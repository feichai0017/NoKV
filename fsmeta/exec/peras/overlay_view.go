// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// OverlayKV is one visible Peras overlay row.
type OverlayKV struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type overlayEntry struct {
	opID   OperationID
	key    []byte
	value  []byte
	delete bool
}

// OverlayView is the authority-local visible read plane before a Peras segment
// is sealed and installed. It owns only semantic overlay state; durable
// segment catalog discovery stays in the runtime layer.
type OverlayView struct {
	mu             sync.RWMutex
	entries        map[string]overlayEntry
	sortedKeys     []string
	sortedDirty    bool
	directoryKeys  map[string]map[string]struct{}
	directoryRuns  map[string][]string
	directoryDirty map[string]bool
	directoryEpoch map[string]uint64
	epoch          uint64
	known          map[string]bool
	emptyDirs      map[string]struct{}
	emptySessions  map[string]struct{}
}

func NewOverlayView() *OverlayView {
	return &OverlayView{
		entries:        make(map[string]overlayEntry),
		directoryKeys:  make(map[string]map[string]struct{}),
		directoryRuns:  make(map[string][]string),
		directoryDirty: make(map[string]bool),
		directoryEpoch: make(map[string]uint64),
		known:          make(map[string]bool),
		emptyDirs:      make(map[string]struct{}),
		emptySessions:  make(map[string]struct{}),
	}
}

func (v *OverlayView) Add(id OperationID, op compile.MaterializedOp) error {
	if v == nil {
		return ErrInvalidPerasSegment
	}
	if err := op.ValidateForAdmission(); err != nil {
		return ErrInvalidPerasSegment
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	for _, effect := range op.Effects {
		if len(effect.Key) == 0 {
			return ErrInvalidPerasSegment
		}
		entry := overlayEntry{
			opID: id,
			key:  cloneBytes(effect.Key),
		}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ErrInvalidPerasSegment
			}
			entry.value = cloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return ErrInvalidPerasSegment
		}
		v.entries[string(effect.Key)] = entry
		v.sortedDirty = true
		v.indexDirectoryKeyLocked(effect.Key)
	}
	return RememberOperationFacts(v.known, v.emptyDirs, v.emptySessions, op)
}

func (v *OverlayView) AddReplayOperation(op ReplayOperation) error {
	if v == nil {
		return ErrInvalidPerasSegment
	}
	if err := validateVisibleReplayOperation(op); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	for _, mutation := range op.Mutations {
		if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
			return ErrInvalidPerasSegment
		}
		entry := overlayEntry{
			opID:   op.OpID,
			key:    cloneBytes(mutation.Key),
			value:  cloneBytes(mutation.Value),
			delete: mutation.Delete,
		}
		v.entries[string(mutation.Key)] = entry
		v.known[string(mutation.Key)] = !mutation.Delete
		if !mutation.Delete {
			ForgetEmptySessionNamespaceForKey(v.emptySessions, mutation.Key)
		}
		v.sortedDirty = true
		v.indexDirectoryKeyLocked(mutation.Key)
	}
	return nil
}

func (v *OverlayView) AddSegment(segment PerasSegment) error {
	if v == nil {
		return ErrInvalidPerasSegment
	}
	if len(segment.EntriesView()) == 0 {
		return ErrInvalidPerasSegment
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	for _, kv := range segment.EntriesView() {
		if len(kv.Key) == 0 || (!kv.Delete && kv.Value == nil) {
			return ErrInvalidPerasSegment
		}
		v.entries[string(kv.Key)] = overlayEntry{
			key:    cloneBytes(kv.Key),
			value:  cloneBytes(kv.Value),
			delete: kv.Delete,
		}
		v.sortedDirty = true
		v.indexDirectoryKeyLocked(kv.Key)
	}
	return nil
}

func (v *OverlayView) Get(key []byte) (value []byte, deleted bool, ok bool) {
	value, deleted, ok = v.GetView(key)
	if !ok {
		return nil, false, false
	}
	return cloneBytes(value), deleted, true
}

// GetView returns overlay-owned value bytes. Callers must not mutate the
// returned slice.
func (v *OverlayView) GetView(key []byte) (value []byte, deleted bool, ok bool) {
	if v == nil {
		return nil, false, false
	}
	v.mu.RLock()
	entry, ok := v.entries[string(key)]
	v.mu.RUnlock()
	if !ok {
		return nil, false, false
	}
	return entry.value, entry.delete, true
}

func (v *OverlayView) KeyState(key []byte) (present bool, known bool) {
	if v == nil {
		return false, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if entry, ok := v.entries[string(key)]; ok {
		return !entry.delete, true
	}
	present, ok := v.known[string(key)]
	if ok {
		return present, true
	}
	if SessionNamespaceEmptyForKey(v.emptySessions, key) {
		return false, true
	}
	return present, ok
}

func (v *OverlayView) DirectoryEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if v == nil {
		return false
	}
	v.mu.RLock()
	_, ok := v.emptyDirs[DirectoryFactKey(mount, inode)]
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) SessionNamespaceEmpty(mount fsmeta.MountIdentity, inode fsmeta.InodeID) bool {
	if v == nil {
		return false
	}
	v.mu.RLock()
	_, ok := v.emptySessions[SessionNamespaceFactKey(mount, inode)]
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) RememberKey(key []byte, present bool) {
	if v == nil || len(key) == 0 {
		return
	}
	v.mu.Lock()
	v.initLocked()
	v.known[string(key)] = present
	if present {
		ForgetEmptySessionNamespaceForKey(v.emptySessions, key)
	}
	v.mu.Unlock()
}

func (v *OverlayView) RememberEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.initLocked()
	RememberEmptyDirectoryFact(v.emptyDirs, mount, inode)
	v.mu.Unlock()
}

func (v *OverlayView) RememberEmptySessionNamespace(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.initLocked()
	RememberEmptySessionNamespaceFact(v.emptySessions, mount, inode)
	v.mu.Unlock()
}

func (v *OverlayView) Scan(start []byte, limit uint32) []OverlayKV {
	if v == nil || limit == 0 {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rebuildSortedKeysLocked()
	startKey := string(start)
	idx := sort.SearchStrings(v.sortedKeys, startKey)
	end := min(idx+int(limit), len(v.sortedKeys))
	out := make([]OverlayKV, 0, end-idx)
	for _, key := range v.sortedKeys[idx:end] {
		entry := v.entries[key]
		out = append(out, OverlayKV{
			Key:    cloneBytes(entry.key),
			Value:  cloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	return out
}

func (v *OverlayView) ScanDirectory(prefix, start []byte, limit uint32) []OverlayKV {
	if v == nil || limit == 0 || len(prefix) == 0 {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rebuildDirectoryRunLocked(string(prefix))
	keys := v.directoryRuns[string(prefix)]
	if len(keys) == 0 {
		return nil
	}
	startKey := string(start)
	idx := sort.SearchStrings(keys, startKey)
	end := min(idx+int(limit), len(keys))
	out := make([]OverlayKV, 0, end-idx)
	for _, key := range keys[idx:end] {
		entry := v.entries[key]
		out = append(out, OverlayKV{
			Key:    cloneBytes(entry.key),
			Value:  cloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	return out
}

func (v *OverlayView) HasDirectory(prefix []byte) bool {
	if v == nil || len(prefix) == 0 {
		return false
	}
	v.mu.RLock()
	keys := v.directoryKeys[string(prefix)]
	ok := len(keys) > 0
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) DirectoryFrontier(prefix []byte) uint64 {
	if v == nil || len(prefix) == 0 {
		return 0
	}
	v.mu.RLock()
	frontier := v.directoryEpoch[string(prefix)]
	if len(v.directoryKeys[string(prefix)]) == 0 {
		frontier = 0
	}
	v.mu.RUnlock()
	return frontier
}

func MergeOverlayScans(base, overlay []OverlayKV, limit uint32) []OverlayKV {
	if limit == 0 {
		return nil
	}
	out := make([]OverlayKV, 0, limit)
	i, j := 0, 0
	for len(out) < int(limit) && (i < len(base) || j < len(overlay)) {
		switch {
		case i >= len(base):
			out = append(out, cloneOverlayKV(overlay[j]))
			j++
		case j >= len(overlay):
			out = append(out, cloneOverlayKV(base[i]))
			i++
		default:
			cmp := bytes.Compare(base[i].Key, overlay[j].Key)
			switch {
			case cmp < 0:
				out = append(out, cloneOverlayKV(base[i]))
				i++
			case cmp > 0:
				out = append(out, cloneOverlayKV(overlay[j]))
				j++
			default:
				out = append(out, cloneOverlayKV(overlay[j]))
				i++
				j++
			}
		}
	}
	return out
}

func (v *OverlayView) RemovePlan(plan ReplayPlan) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, op := range plan.Operations {
		for _, mutation := range op.Mutations {
			entry, ok := v.entries[string(mutation.Key)]
			if ok && entry.opID == op.OpID {
				delete(v.entries, string(mutation.Key))
				v.sortedDirty = true
				v.removeDirectoryKeyLocked(mutation.Key)
			}
		}
	}
}

func (v *OverlayView) Stats() (overlayKeys, knownKeys, emptyDirs, emptySessions int) {
	if v == nil {
		return 0, 0, 0, 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.entries), len(v.known), len(v.emptyDirs), len(v.emptySessions)
}

func (v *OverlayView) ReadIndexStats() (directories, dirty int) {
	if v == nil {
		return 0, 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, keys := range v.directoryKeys {
		if len(keys) > 0 {
			directories++
		}
	}
	for _, isDirty := range v.directoryDirty {
		if isDirty {
			dirty++
		}
	}
	return directories, dirty
}

func (v *OverlayView) initLocked() {
	if v.entries == nil {
		v.entries = make(map[string]overlayEntry)
	}
	if v.directoryKeys == nil {
		v.directoryKeys = make(map[string]map[string]struct{})
	}
	if v.directoryRuns == nil {
		v.directoryRuns = make(map[string][]string)
	}
	if v.directoryDirty == nil {
		v.directoryDirty = make(map[string]bool)
	}
	if v.directoryEpoch == nil {
		v.directoryEpoch = make(map[string]uint64)
	}
	if v.known == nil {
		v.known = make(map[string]bool)
	}
	if v.emptyDirs == nil {
		v.emptyDirs = make(map[string]struct{})
	}
	if v.emptySessions == nil {
		v.emptySessions = make(map[string]struct{})
	}
	if v.sortedKeys == nil {
		v.sortedDirty = true
	}
}

func (v *OverlayView) rebuildSortedKeysLocked() {
	if !v.sortedDirty && len(v.sortedKeys) == len(v.entries) {
		return
	}
	v.sortedKeys = v.sortedKeys[:0]
	for key := range v.entries {
		v.sortedKeys = append(v.sortedKeys, key)
	}
	sort.Strings(v.sortedKeys)
	v.sortedDirty = false
}

func (v *OverlayView) rebuildDirectoryRunLocked(prefix string) {
	v.initLocked()
	keys := v.directoryKeys[prefix]
	if len(keys) == 0 {
		delete(v.directoryRuns, prefix)
		delete(v.directoryDirty, prefix)
		return
	}
	if !v.directoryDirty[prefix] && len(v.directoryRuns[prefix]) == len(keys) {
		return
	}
	run := v.directoryRuns[prefix][:0]
	for key := range keys {
		if _, ok := v.entries[key]; ok {
			run = append(run, key)
		}
	}
	sort.Strings(run)
	v.directoryRuns[prefix] = run
	v.directoryDirty[prefix] = false
}

func (v *OverlayView) indexDirectoryKeyLocked(key []byte) {
	prefix, ok := dentryDirectoryPrefix(key)
	if !ok {
		return
	}
	v.initLocked()
	keys := v.directoryKeys[prefix]
	if keys == nil {
		keys = make(map[string]struct{})
		v.directoryKeys[prefix] = keys
	}
	keys[string(key)] = struct{}{}
	v.directoryDirty[prefix] = true
	v.bumpDirectoryFrontierLocked(prefix)
}

func (v *OverlayView) removeDirectoryKeyLocked(key []byte) {
	prefix, ok := dentryDirectoryPrefix(key)
	if !ok {
		return
	}
	keyString := string(key)
	if keys := v.directoryKeys[prefix]; keys != nil {
		delete(keys, keyString)
		if len(keys) == 0 {
			delete(v.directoryKeys, prefix)
			delete(v.directoryRuns, prefix)
			delete(v.directoryDirty, prefix)
			delete(v.directoryEpoch, prefix)
			return
		}
	}
	v.directoryDirty[prefix] = true
	v.bumpDirectoryFrontierLocked(prefix)
}

func (v *OverlayView) bumpDirectoryFrontierLocked(prefix string) {
	v.epoch++
	if v.epoch == 0 {
		v.epoch = 1
	}
	v.directoryEpoch[prefix] = v.epoch
}

func dentryDirectoryPrefix(key []byte) (string, bool) {
	name, ok := fsmeta.DentryNameBytesOfKey(key)
	if !ok || len(name) == 0 || len(name) > len(key) {
		return "", false
	}
	return string(key[:len(key)-len(name)]), true
}

func cloneOverlayKV(in OverlayKV) OverlayKV {
	return OverlayKV{
		Key:    cloneBytes(in.Key),
		Value:  cloneBytes(in.Value),
		Delete: in.Delete,
	}
}
