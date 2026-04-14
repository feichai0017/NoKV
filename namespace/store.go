package namespace

import (
	"bytes"
	"sort"
)

// Store is the first executable prototype of the namespace-aware listing
// design. It keeps authoritative truth and listing pages in the same backing KV
// so one Apply(batch) call defines the commit boundary for both.
//
// This is still a prototype:
//   - it does not expose distributed semantics
//   - it does not implement rename
//   - it intentionally keeps ordering weaker than full global lexicographical
//     listing across shards
type Store struct {
	kv     KV
	shards int
}

func NewStore(kv KV, shards int) *Store {
	if shards <= 0 {
		shards = 1
	}
	return &Store{kv: kv, shards: shards}
}

func (s *Store) Create(path []byte, kind EntryKind, meta []byte) error {
	if s == nil || s.kv == nil {
		return ErrInvalidPath
	}
	parent, name, err := splitPath(path)
	if err != nil {
		return err
	}
	truthKey := encodeTruthKey(path)
	existing, err := s.kv.Get(truthKey)
	if err != nil {
		return err
	}
	if existing != nil {
		return ErrPathExists
	}

	page, pageKey, err := s.loadListingPage(parent, name)
	if err != nil {
		return err
	}
	insertIdx := sort.Search(len(page.Entries), func(i int) bool {
		return bytes.Compare(page.Entries[i].Name, name) >= 0
	})
	if insertIdx < len(page.Entries) && bytes.Equal(page.Entries[insertIdx].Name, name) {
		return ErrChildExists
	}
	page.Entries = append(page.Entries, Entry{})
	copy(page.Entries[insertIdx+1:], page.Entries[insertIdx:])
	page.Entries[insertIdx] = Entry{
		Name:    cloneBytes(name),
		Kind:    kind,
		MetaKey: cloneBytes(truthKey),
	}
	pageRaw, err := encodeListingPage(page)
	if err != nil {
		return err
	}
	return s.kv.Apply([]Mutation{
		{Kind: MutationPut, Key: truthKey, Value: cloneBytes(meta)},
		{Kind: MutationPut, Key: pageKey, Value: pageRaw},
	})
}

func (s *Store) Delete(path []byte) error {
	if s == nil || s.kv == nil {
		return ErrInvalidPath
	}
	parent, name, err := splitPath(path)
	if err != nil {
		return err
	}
	truthKey := encodeTruthKey(path)
	existing, err := s.kv.Get(truthKey)
	if err != nil {
		return err
	}
	if existing == nil {
		return ErrPathNotFound
	}

	page, pageKey, err := s.loadListingPage(parent, name)
	if err != nil {
		return err
	}
	idx := sort.Search(len(page.Entries), func(i int) bool {
		return bytes.Compare(page.Entries[i].Name, name) >= 0
	})
	if idx >= len(page.Entries) || !bytes.Equal(page.Entries[idx].Name, name) {
		return ErrChildNotFound
	}
	copy(page.Entries[idx:], page.Entries[idx+1:])
	page.Entries = page.Entries[:len(page.Entries)-1]

	batch := []Mutation{{Kind: MutationDelete, Key: truthKey}}
	if len(page.Entries) == 0 {
		batch = append(batch, Mutation{Kind: MutationDelete, Key: pageKey})
	} else {
		pageRaw, err := encodeListingPage(page)
		if err != nil {
			return err
		}
		batch = append(batch, Mutation{Kind: MutationPut, Key: pageKey, Value: pageRaw})
	}
	return s.kv.Apply(batch)
}

func (s *Store) Lookup(path []byte) ([]byte, error) {
	if s == nil || s.kv == nil {
		return nil, ErrInvalidPath
	}
	_, _, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	val, err := s.kv.Get(encodeTruthKey(path))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, ErrPathNotFound
	}
	return val, nil
}

func (s *Store) List(parent []byte, cursor Cursor, limit int) ([]Entry, Cursor, error) {
	if s == nil || s.kv == nil {
		return nil, Cursor{}, ErrInvalidPath
	}
	if limit <= 0 {
		return nil, Cursor{}, ErrInvalidLimit
	}
	pairs, err := s.kv.ScanPrefix(encodeListingPrefix(parent))
	if err != nil {
		return nil, Cursor{}, err
	}
	if len(pairs) == 0 {
		return nil, Cursor{}, nil
	}

	out := make([]Entry, 0, limit)
	next := Cursor{}
	started := len(cursor.PageID) == 0
	for _, pair := range pairs {
		page, err := decodeListingPage(pair.Value)
		if err != nil {
			return nil, Cursor{}, err
		}
		if !bytes.Equal(page.Prefix, parent) {
			return nil, Cursor{}, ErrParentMismatch
		}
		if !started {
			cmp := bytes.Compare(page.PageID, cursor.PageID)
			if cmp < 0 {
				continue
			}
			if cmp == 0 {
				started = true
			} else {
				started = true
				cursor.LastName = nil
			}
		}
		startIdx := 0
		if bytes.Equal(page.PageID, cursor.PageID) && len(cursor.LastName) > 0 {
			startIdx = sort.Search(len(page.Entries), func(i int) bool {
				return bytes.Compare(page.Entries[i].Name, cursor.LastName) > 0
			})
		}
		for i := startIdx; i < len(page.Entries) && len(out) < limit; i++ {
			out = append(out, cloneEntry(page.Entries[i]))
			next.PageID = cloneBytes(page.PageID)
			next.LastName = cloneBytes(page.Entries[i].Name)
		}
		if len(out) == limit {
			break
		}
	}
	if len(out) == 0 {
		return nil, Cursor{}, nil
	}
	if len(out) < limit {
		next = Cursor{}
	}
	return out, next, nil
}

func (s *Store) loadListingPage(parent, name []byte) (ListingPage, []byte, error) {
	pageID := encodePageID(s.shardFor(name))
	pageKey := encodeListingPageKey(parent, pageID)
	raw, err := s.kv.Get(pageKey)
	if err != nil {
		return ListingPage{}, nil, err
	}
	if raw == nil {
		return ListingPage{
			Prefix: cloneBytes(parent),
			PageID: cloneBytes(pageID),
		}, pageKey, nil
	}
	page, err := decodeListingPage(raw)
	if err != nil {
		return ListingPage{}, nil, err
	}
	if !bytes.Equal(page.Prefix, parent) {
		return ListingPage{}, nil, ErrParentMismatch
	}
	return page, pageKey, nil
}

func (s *Store) shardFor(name []byte) uint32 {
	return shardFor(name, s.shards)
}
