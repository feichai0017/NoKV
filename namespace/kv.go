package namespace

import (
	"bytes"
	"errors"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/index"
	entrykv "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
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
	ScanPrefix(prefix, start []byte, limit int) ([]KVPair, error)
}

// DBView is the minimal DB surface the namespace adapter needs from the host engine.
// It exists only to keep the namespace package decoupled from the root DB type.
type DBView interface {
	ApplyInternalEntries(entries []*entrykv.Entry) error
	Get(key []byte) (*entrykv.Entry, error)
	NewIterator(opt *index.Options) index.Iterator
}

// NoKVStore adapts an embedded DB that satisfies DBView to the minimal KV
// surface used by the namespace listing implementation.
//
// This keeps namespace work on the real engine path:
// WAL -> current mutable tier -> LSM -> iterator scan.
type NoKVStore struct {
	db      DBView
	version atomic.Uint64
}

func NewNoKVStore(db DBView) *NoKVStore {
	store := &NoKVStore{db: db}
	store.version.Store(uint64(time.Now().UnixNano()))
	return store
}

func (s *NoKVStore) Apply(batch []Mutation) error {
	if s == nil || s.db == nil {
		return ErrInvalidPath
	}
	if len(batch) == 0 {
		return nil
	}
	entries := make([]*entrykv.Entry, 0, len(batch))
	defer func() {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
	}()

	base := s.version.Add(uint64(len(batch)))
	start := base - uint64(len(batch)) + 1
	for i, mut := range batch {
		version := start + uint64(i)
		switch mut.Kind {
		case MutationPut:
			entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, mut.Key, version, mut.Value, 0, 0))
		case MutationDelete:
			entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, mut.Key, version, nil, entrykv.BitDelete, 0))
		default:
			return utils.ErrInvalidRequest
		}
	}
	return s.db.ApplyInternalEntries(entries)
}

func (s *NoKVStore) Get(key []byte) ([]byte, error) {
	if s == nil || s.db == nil {
		return nil, ErrInvalidPath
	}
	entry, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	return entry.Value, nil
}

func (s *NoKVStore) ScanPrefix(prefix, start []byte, limit int) ([]KVPair, error) {
	if s == nil || s.db == nil {
		return nil, ErrInvalidPath
	}
	iter := s.db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()

	lower := prefix
	if len(start) > 0 && bytes.Compare(start, prefix) > 0 {
		lower = start
	}
	iter.Seek(lower)
	out := make([]KVPair, 0, 16)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil {
			break
		}
		entry := item.Entry()
		if entry == nil || !bytes.HasPrefix(entry.Key, prefix) {
			break
		}
		out = append(out, KVPair{
			Key:   cloneBytes(entry.Key),
			Value: cloneBytes(entry.Value),
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}
