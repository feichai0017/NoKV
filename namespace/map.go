package namespace

import (
	"bytes"
	"hash/fnv"
	"sort"
	"sync"
)

// ListingMap is an in-memory prototype implementation of ListingIndex.
//
// It exists to support early design validation and benchmark work. It is not a
// production persistence layer; it simply makes the namespace contracts
// executable without forcing intrusive changes into the current LSM/WAL stack.
type ListingMap struct {
	mu     sync.RWMutex
	shards int
	pages  map[string]map[uint32]*ListingPage
}

// NewListingMap creates an in-memory listing index with a fixed shard count per
// parent prefix. A shard count <= 0 is rejected by falling back to 1.
func NewListingMap(shards int) *ListingMap {
	if shards <= 0 {
		shards = 1
	}
	return &ListingMap{
		shards: shards,
		pages:  make(map[string]map[uint32]*ListingPage),
	}
}

func (m *ListingMap) AddChild(parent []byte, child Entry) error {
	if len(child.Name) == 0 {
		return ErrChildNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	parentKey := string(parent)
	shardID := m.shardFor(child.Name)
	page := m.ensurePageLocked(parentKey, shardID)
	idx := sort.Search(len(page.Entries), func(i int) bool {
		return bytes.Compare(page.Entries[i].Name, child.Name) >= 0
	})
	if idx < len(page.Entries) && bytes.Equal(page.Entries[idx].Name, child.Name) {
		return ErrChildExists
	}
	page.Entries = append(page.Entries, Entry{})
	copy(page.Entries[idx+1:], page.Entries[idx:])
	page.Entries[idx] = cloneEntry(child)
	return nil
}

func (m *ListingMap) RemoveChild(parent []byte, name []byte) error {
	if len(name) == 0 {
		return ErrChildNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	parentKey := string(parent)
	shardID := m.shardFor(name)
	page := m.lookupPageLocked(parentKey, shardID)
	if page == nil {
		return ErrPageNotFound
	}
	idx := sort.Search(len(page.Entries), func(i int) bool {
		return bytes.Compare(page.Entries[i].Name, name) >= 0
	})
	if idx >= len(page.Entries) || !bytes.Equal(page.Entries[idx].Name, name) {
		return ErrChildNotFound
	}
	copy(page.Entries[idx:], page.Entries[idx+1:])
	page.Entries = page.Entries[:len(page.Entries)-1]
	return nil
}

func (m *ListingMap) List(parent []byte, cursor Cursor, limit int) ([]Entry, Cursor, error) {
	if limit <= 0 {
		return nil, Cursor{}, ErrInvalidLimit
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	parentKey := string(parent)
	perParent := m.pages[parentKey]
	if len(perParent) == 0 {
		return nil, Cursor{}, nil
	}

	startShard := 0
	if len(cursor.PageID) > 0 {
		shard, ok := parsePageID(cursor.PageID)
		if !ok {
			return nil, Cursor{}, ErrCursorCorrupted
		}
		startShard = int(shard)
	}

	out := make([]Entry, 0, limit)
	next := Cursor{}
	lastName := cursor.LastName
	for shard := startShard; shard < m.shards && len(out) < limit; shard++ {
		page := perParent[uint32(shard)]
		lastName = nilIfShardChanges(shard, startShard, cursor.LastName)
		if page == nil || len(page.Entries) == 0 {
			continue
		}
		startIdx := 0
		if len(lastName) > 0 {
			startIdx = sort.Search(len(page.Entries), func(i int) bool {
				return bytes.Compare(page.Entries[i].Name, lastName) > 0
			})
		}
		for i := startIdx; i < len(page.Entries) && len(out) < limit; i++ {
			out = append(out, cloneEntry(page.Entries[i]))
			next.PageID = cloneBytes(page.PageID)
			next.LastName = cloneBytes(page.Entries[i].Name)
		}
	}
	if len(out) == 0 {
		return nil, Cursor{}, nil
	}
	return out, next, nil
}

func (m *ListingMap) shardFor(name []byte) uint32 {
	return shardFor(name, m.shards)
}

func shardFor(name []byte, shards int) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(name)
	return h.Sum32() % uint32(shards)
}

func (m *ListingMap) ensurePageLocked(parent string, shardID uint32) *ListingPage {
	perParent := m.pages[parent]
	if perParent == nil {
		perParent = make(map[uint32]*ListingPage)
		m.pages[parent] = perParent
	}
	page := perParent[shardID]
	if page == nil {
		page = &ListingPage{
			Prefix: cloneBytes([]byte(parent)),
			PageID: encodePageID(shardID),
		}
		perParent[shardID] = page
	}
	return page
}

func (m *ListingMap) lookupPageLocked(parent string, shardID uint32) *ListingPage {
	perParent := m.pages[parent]
	if perParent == nil {
		return nil
	}
	return perParent[shardID]
}

func encodePageID(shardID uint32) []byte {
	return []byte{byte(shardID >> 24), byte(shardID >> 16), byte(shardID >> 8), byte(shardID)}
}

func parsePageID(pageID []byte) (uint32, bool) {
	if len(pageID) != 4 {
		return 0, false
	}
	return uint32(pageID[0])<<24 | uint32(pageID[1])<<16 | uint32(pageID[2])<<8 | uint32(pageID[3]), true
}

func nilIfShardChanges(current, start int, lastName []byte) []byte {
	if current != start {
		return nil
	}
	return lastName
}

func cloneEntry(in Entry) Entry {
	return Entry{
		Name:    cloneBytes(in.Name),
		Kind:    in.Kind,
		MetaKey: cloneBytes(in.MetaKey),
	}
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
