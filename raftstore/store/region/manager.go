package region

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/feichai0017/NoKV/metrics"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// Manager owns the store-local region catalog: in-memory meta map,
// range-by-start-key index, peer registry, durable mirror via localmeta,
// and the publish-side notify hook used to feed the scheduler runtime.
type Manager struct {
	mu          sync.RWMutex
	metaByID    map[uint64]localmeta.RegionMeta
	metaByStart []startIndexEntry
	peers       map[uint64]*peer.Peer
	localMeta   *localmeta.Store
	regionMet   *metrics.RegionMetrics
	notify      func(rootevent.Event)
}

type startIndexEntry struct {
	start []byte
	id    uint64
}

// NewManager constructs an empty Manager. notify, when non-nil, is invoked
// once per applied catalog change with the corresponding rootevent.Event;
// callers wrap that into their own scheduler-publish event type.
func NewManager(localMeta *localmeta.Store, regionMetrics *metrics.RegionMetrics, notify func(rootevent.Event)) *Manager {
	return &Manager{
		metaByID:  make(map[uint64]localmeta.RegionMeta),
		peers:     make(map[uint64]*peer.Peer),
		localMeta: localMeta,
		regionMet: regionMetrics,
		notify:    notify,
	}
}

// LoadBootstrap seeds the catalog from a snapshot of localmeta state at
// startup. Calls do not fire notify; callers replay bootstrap events through
// localmeta directly.
func (m *Manager) LoadBootstrap(snapshot map[uint64]localmeta.RegionMeta) {
	if m == nil || len(snapshot) == 0 {
		return
	}
	m.mu.Lock()
	for id, meta := range snapshot {
		metaCopy := localmeta.CloneRegionMeta(meta)
		m.metaByID[id] = metaCopy
		if m.regionMet != nil {
			m.regionMet.RecordState(metaCopy.ID, metaCopy.State)
		}
	}
	m.rebuildRangeIndexLocked()
	m.mu.Unlock()
}

// Metrics returns the RegionMetrics recorder this manager updates.
func (m *Manager) Metrics() *metrics.RegionMetrics {
	if m == nil {
		return nil
	}
	return m.regionMet
}

// LocalMeta returns the durable mirror this manager persists through.
func (m *Manager) LocalMeta() *localmeta.Store {
	if m == nil {
		return nil
	}
	return m.localMeta
}

// SetPeer associates a peer with regionID; nil clears.
func (m *Manager) SetPeer(regionID uint64, p *peer.Peer) {
	if m == nil || regionID == 0 {
		return
	}
	m.mu.Lock()
	if p == nil {
		delete(m.peers, regionID)
	} else {
		m.peers[regionID] = p
	}
	m.mu.Unlock()
}

// Peer returns the registered peer for regionID, or nil.
func (m *Manager) Peer(regionID uint64) *peer.Peer {
	if m == nil || regionID == 0 {
		return nil
	}
	m.mu.RLock()
	p := m.peers[regionID]
	m.mu.RUnlock()
	return p
}

// Meta returns a deep copy of the region metadata for regionID.
func (m *Manager) Meta(regionID uint64) (localmeta.RegionMeta, bool) {
	if m == nil || regionID == 0 {
		return localmeta.RegionMeta{}, false
	}
	m.mu.RLock()
	meta, ok := m.metaByID[regionID]
	m.mu.RUnlock()
	if !ok {
		return localmeta.RegionMeta{}, false
	}
	return localmeta.CloneRegionMeta(meta), true
}

// Metas returns deep copies of all tracked region metadata.
func (m *Manager) Metas() []localmeta.RegionMeta {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	out := make([]localmeta.RegionMeta, 0, len(m.metaByID))
	for _, meta := range m.metaByID {
		out = append(out, localmeta.CloneRegionMeta(meta))
	}
	m.mu.RUnlock()
	return out
}

// MetaByKey returns the region metadata that owns key, looking up via the
// range-by-start-key index.
func (m *Manager) MetaByKey(key []byte) (localmeta.RegionMeta, bool) {
	if m == nil || len(key) == 0 {
		return localmeta.RegionMeta{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx := sort.Search(len(m.metaByStart), func(i int) bool {
		return bytes.Compare(m.metaByStart[i].start, key) > 0
	}) - 1
	if idx < 0 {
		return localmeta.RegionMeta{}, false
	}
	meta, ok := m.metaByID[m.metaByStart[idx].id]
	if !ok || !keyInRange(meta, key) {
		return localmeta.RegionMeta{}, false
	}
	return localmeta.CloneRegionMeta(meta), true
}

// Apply persists meta to localmeta, updates the in-memory mirror, and
// (when publish is true) calls notify with the corresponding root event.
func (m *Manager) Apply(meta localmeta.RegionMeta, publish bool) error {
	if m == nil {
		return ErrNil
	}
	if meta.ID == 0 {
		return ErrZeroID
	}
	metaCopy := localmeta.CloneRegionMeta(meta)
	if metaCopy.State == 0 {
		metaCopy.State = metaregion.ReplicaStateRunning
	}

	var currentState metaregion.ReplicaState
	m.mu.RLock()
	if existing, ok := m.metaByID[metaCopy.ID]; ok {
		currentState = existing.State
	} else {
		currentState = metaregion.ReplicaStateNew
	}
	m.mu.RUnlock()

	if !validRegionStateTransition(currentState, metaCopy.State) {
		return fmt.Errorf("raftstore/region: invalid region %d state transition %v -> %v", metaCopy.ID, currentState, metaCopy.State)
	}

	if m.localMeta != nil {
		// Local durable metadata is the restart source of truth. Persist it
		// before updating the in-memory mirror so crash recovery can always
		// rebuild the catalog from disk if this process dies in between.
		if err := m.localMeta.SaveRegion(metaCopy); err != nil {
			return err
		}
	}

	m.mu.Lock()
	_, existed := m.metaByID[metaCopy.ID]
	m.metaByID[metaCopy.ID] = localmeta.CloneRegionMeta(metaCopy)
	m.rebuildRangeIndexLocked()
	p := m.peers[metaCopy.ID]
	m.mu.Unlock()

	syncPeerMirror(p, metaCopy)
	if m.regionMet != nil {
		m.regionMet.RecordState(metaCopy.ID, metaCopy.State)
	}
	if publish && m.notify != nil {
		m.notify(applyRootEvent(metaCopy, existed))
	}
	return nil
}

// ApplyState sets the replica state for regionID to state and emits notify.
func (m *Manager) ApplyState(regionID uint64, state metaregion.ReplicaState) error {
	if m == nil {
		return ErrNil
	}
	meta, ok := m.Meta(regionID)
	if !ok {
		return ErrNotFound(regionID)
	}
	meta.State = state
	return m.Apply(meta, true)
}

// Remove tombstones and then evicts regionID from the catalog. When publish
// is true notify fires with the removal root event.
func (m *Manager) Remove(regionID uint64, publish bool) error {
	if m == nil {
		return ErrNil
	}
	if regionID == 0 {
		return ErrZeroID
	}
	meta, ok := m.Meta(regionID)
	if !ok {
		return ErrNotFound(regionID)
	}
	if meta.State != metaregion.ReplicaStateTombstone {
		meta.State = metaregion.ReplicaStateTombstone
		if err := m.Apply(meta, publish); err != nil {
			return err
		}
	}
	if m.localMeta != nil {
		if err := m.localMeta.DeleteRegion(regionID); err != nil {
			return err
		}
	}
	m.mu.Lock()
	delete(m.metaByID, regionID)
	delete(m.peers, regionID)
	m.rebuildRangeIndexLocked()
	m.mu.Unlock()
	if m.regionMet != nil {
		m.regionMet.RecordRemove(regionID)
	}
	if publish && m.notify != nil {
		m.notify(removalRootEvent(regionID))
	}
	return nil
}

// RuntimeStatus returns the runtime view of regionID: meta + hosted/leader
// state from the registered peer + applied raft pointer from localmeta.
func (m *Manager) RuntimeStatus(regionID uint64) (RuntimeStatus, bool) {
	meta, ok := m.Meta(regionID)
	if !ok {
		return RuntimeStatus{}, false
	}
	status := RuntimeStatus{Meta: meta}
	peerRef := m.Peer(regionID)
	if peerRef == nil {
		return status, true
	}
	raftStatus := peerRef.Status()
	status.Hosted = true
	status.LocalPeerID = peerRef.ID()
	status.LeaderPeerID = raftStatus.Lead
	status.Leader = raftStatus.RaftState == myraft.StateLeader
	if m.localMeta != nil {
		if ptr, ok := m.localMeta.RaftPointer(regionID); ok {
			status.AppliedIndex = ptr.AppliedIndex
			status.AppliedTerm = ptr.AppliedTerm
		}
	}
	return status, true
}

func (m *Manager) rebuildRangeIndexLocked() {
	m.metaByStart = m.metaByStart[:0]
	for id, meta := range m.metaByID {
		m.metaByStart = append(m.metaByStart, startIndexEntry{
			start: append([]byte(nil), meta.StartKey...),
			id:    id,
		})
	}
	sort.Slice(m.metaByStart, func(i, j int) bool {
		cmp := bytes.Compare(m.metaByStart[i].start, m.metaByStart[j].start)
		if cmp != 0 {
			return cmp < 0
		}
		return m.metaByStart[i].id < m.metaByStart[j].id
	})
}

// syncPeerMirror updates a peer's in-memory region snapshot after the local
// region truth has already been persisted and applied to the catalog.
func syncPeerMirror(p *peer.Peer, meta localmeta.RegionMeta) {
	if p == nil {
		return
	}
	p.ApplyRegionMetaMirror(meta)
}

func applyRootEvent(meta localmeta.RegionMeta, existed bool) rootevent.Event {
	desc := localmeta.Descriptor(meta, 0)
	if !existed {
		return rootevent.RegionBootstrapped(desc)
	}
	return rootevent.RegionDescriptorPublished(desc)
}

func removalRootEvent(regionID uint64) rootevent.Event {
	return rootevent.RegionTombstoned(regionID)
}

func validRegionStateTransition(current, next metaregion.ReplicaState) bool {
	if current == next {
		return true
	}
	switch current {
	case metaregion.ReplicaStateNew:
		return next == metaregion.ReplicaStateRunning
	case metaregion.ReplicaStateRunning:
		return next == metaregion.ReplicaStateRemoving || next == metaregion.ReplicaStateTombstone
	case metaregion.ReplicaStateRemoving:
		return next == metaregion.ReplicaStateTombstone
	case metaregion.ReplicaStateTombstone:
		return next == metaregion.ReplicaStateTombstone
	default:
		return false
	}
}

func keyInRange(meta localmeta.RegionMeta, key []byte) bool {
	if len(key) == 0 {
		return true
	}
	if len(meta.StartKey) > 0 && bytes.Compare(key, meta.StartKey) < 0 {
		return false
	}
	if len(meta.EndKey) > 0 && bytes.Compare(key, meta.EndKey) >= 0 {
		return false
	}
	return true
}
