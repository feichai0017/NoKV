package namespace

import (
	"bytes"
	"sort"
	"sync"
)

type MutationKind uint8

const (
	MutationPut MutationKind = iota
	MutationDelete
)

type Mutation struct {
	Kind  MutationKind
	Key   []byte
	Value []byte
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// KV is the smallest backing store surface needed by the namespace prototype.
// It intentionally models a batch commit boundary because the companion listing
// design depends on truth and listing updates being committed together.
type KV interface {
	Apply(batch []Mutation) error
	Get(key []byte) ([]byte, error)
	ScanPrefix(prefix []byte) ([]KVPair, error)
}

// MapStore is an in-memory KV implementation for prototype and benchmark use.
type MapStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMapStore() *MapStore {
	return &MapStore{data: make(map[string][]byte)}
}

func (m *MapStore) Apply(batch []Mutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mut := range batch {
		key := string(mut.Key)
		switch mut.Kind {
		case MutationPut:
			m.data[key] = cloneBytes(mut.Value)
		case MutationDelete:
			delete(m.data, key)
		}
	}
	return nil
}

func (m *MapStore) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return cloneBytes(val), nil
}

func (m *MapStore) ScanPrefix(prefix []byte) ([]KVPair, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		if bytes.HasPrefix([]byte(key), prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]KVPair, 0, len(keys))
	for _, key := range keys {
		out = append(out, KVPair{
			Key:   []byte(key),
			Value: cloneBytes(m.data[key]),
		})
	}
	return out, nil
}
