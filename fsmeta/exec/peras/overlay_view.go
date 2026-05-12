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
	mu            sync.RWMutex
	entries       map[string]overlayEntry
	known         map[string]bool
	emptyDirs     map[string]struct{}
	emptySessions map[string]struct{}
}

func NewOverlayView() *OverlayView {
	return &OverlayView{
		entries:       make(map[string]overlayEntry),
		known:         make(map[string]bool),
		emptyDirs:     make(map[string]struct{}),
		emptySessions: make(map[string]struct{}),
	}
}

func (v *OverlayView) Add(id OperationID, op compile.CompiledOp) error {
	if v == nil {
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
	}
	return RememberOperationFacts(v.known, v.emptyDirs, v.emptySessions, op)
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
	}
	return nil
}

func (v *OverlayView) Get(key []byte) (value []byte, deleted bool, ok bool) {
	if v == nil {
		return nil, false, false
	}
	v.mu.RLock()
	entry, ok := v.entries[string(key)]
	v.mu.RUnlock()
	if !ok {
		return nil, false, false
	}
	return cloneBytes(entry.value), entry.delete, true
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
	v.mu.RLock()
	keys := make([]string, 0, len(v.entries))
	for key := range v.entries {
		if bytes.Compare([]byte(key), start) >= 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > int(limit) {
		keys = keys[:limit]
	}
	out := make([]OverlayKV, 0, len(keys))
	for _, key := range keys {
		entry := v.entries[key]
		out = append(out, OverlayKV{
			Key:    cloneBytes(entry.key),
			Value:  cloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	v.mu.RUnlock()
	return out
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

func (v *OverlayView) initLocked() {
	if v.entries == nil {
		v.entries = make(map[string]overlayEntry)
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
}

func cloneOverlayKV(in OverlayKV) OverlayKV {
	return OverlayKV{
		Key:    cloneBytes(in.Key),
		Value:  cloneBytes(in.Value),
		Delete: in.Delete,
	}
}
