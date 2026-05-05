package client

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/protobuf/proto"
)

type AuthorityVerifierKey struct {
	ClusterID string
	DutyID    rootproto.DutyID
	Scope     rootproto.DutyScope
}

type AuthorityVerifierState struct {
	Key                   AuthorityVerifierKey
	MaxSeenEra            uint64
	RetiredEraFloor       uint64
	MaxRootToken          rootproto.AuthorityRootToken
	MaxDescriptorRevision uint64
	MaxFrontier           rootproto.DutyBound
}

type AuthorityVerifierStore interface {
	LoadAuthorityVerifier(key AuthorityVerifierKey) (AuthorityVerifierState, error)
	SaveAuthorityVerifier(state AuthorityVerifierState) error
}

type MemoryAuthorityVerifierStore struct {
	mu     sync.Mutex
	states map[string]AuthorityVerifierState
}

func NewMemoryAuthorityVerifierStore() *MemoryAuthorityVerifierStore {
	return &MemoryAuthorityVerifierStore{states: make(map[string]AuthorityVerifierState)}
}

func (s *MemoryAuthorityVerifierStore) LoadAuthorityVerifier(key AuthorityVerifierKey) (AuthorityVerifierState, error) {
	if s == nil {
		return AuthorityVerifierState{Key: key}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[authorityVerifierKeyString(key)]
	if !ok {
		state.Key = key
	}
	return state, nil
}

func (s *MemoryAuthorityVerifierStore) SaveAuthorityVerifier(state AuthorityVerifierState) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[string]AuthorityVerifierState)
	}
	s.states[authorityVerifierKeyString(state.Key)] = state
	return nil
}

type FileAuthorityVerifierStore struct {
	mu     sync.Mutex
	path   string
	loaded bool
	states map[string]AuthorityVerifierState
}

func NewFileAuthorityVerifierStore(path string) *FileAuthorityVerifierStore {
	return &FileAuthorityVerifierStore{path: path}
}

func (s *FileAuthorityVerifierStore) LoadAuthorityVerifier(key AuthorityVerifierKey) (AuthorityVerifierState, error) {
	if s == nil {
		return AuthorityVerifierState{Key: key}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return AuthorityVerifierState{}, err
	}
	state, ok := s.states[authorityVerifierKeyString(key)]
	if !ok {
		state.Key = key
	}
	return state, nil
}

func (s *FileAuthorityVerifierStore) SaveAuthorityVerifier(state AuthorityVerifierState) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	s.states[authorityVerifierKeyString(state.Key)] = state
	return s.flushLocked()
}

func (s *FileAuthorityVerifierStore) loadLocked() error {
	if s.loaded {
		return nil
	}
	if s.path == "" {
		return errors.New("authority verifier store path is empty")
	}
	s.states = make(map[string]AuthorityVerifierState)
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.loaded = true
			return nil
		}
		return err
	}
	var pbStore metapb.RootAuthorityVerifierStore
	if err := proto.Unmarshal(raw, &pbStore); err != nil {
		return err
	}
	for _, pbState := range pbStore.GetStates() {
		state := authorityVerifierStateFromProto(pbState)
		s.states[authorityVerifierKeyString(state.Key)] = state
	}
	s.loaded = true
	return nil
}

func (s *FileAuthorityVerifierStore) flushLocked() error {
	keys := make([]string, 0, len(s.states))
	for key := range s.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pbStore := &metapb.RootAuthorityVerifierStore{States: make([]*metapb.RootAuthorityVerifierState, 0, len(keys))}
	for _, key := range keys {
		pbStore.States = append(pbStore.States, authorityVerifierStateToProto(s.states[key]))
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(pbStore)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".authority-verifier-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return err
	}
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

func authorityVerifierKeyString(key AuthorityVerifierKey) string {
	scope := key.Scope
	return fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%d\x00%x\x00%x",
		key.ClusterID,
		key.DutyID,
		scope.Kind,
		scope.MountID,
		scope.SubtreeRoot,
		scope.StartKey,
		scope.EndKey,
	)
}

func authorityVerifierStateToProto(state AuthorityVerifierState) *metapb.RootAuthorityVerifierState {
	return &metapb.RootAuthorityVerifierState{
		Key: &metapb.RootAuthorityVerifierKey{
			ClusterId: state.Key.ClusterID,
			DutyId:    string(state.Key.DutyID),
			Scope:     metawire.RootDutyScopeToProto(state.Key.Scope),
		},
		MaxSeenEra:            state.MaxSeenEra,
		RetiredEraFloor:       state.RetiredEraFloor,
		MaxRootToken:          metawire.RootTailTokenFromAuthorityToken(state.MaxRootToken),
		MaxDescriptorRevision: state.MaxDescriptorRevision,
		MaxFrontier:           metawire.RootDutyBoundToProto(state.MaxFrontier),
	}
}

func authorityVerifierStateFromProto(state *metapb.RootAuthorityVerifierState) AuthorityVerifierState {
	if state == nil {
		return AuthorityVerifierState{}
	}
	key := state.GetKey()
	return AuthorityVerifierState{
		Key: AuthorityVerifierKey{
			ClusterID: key.GetClusterId(),
			DutyID:    rootproto.DutyID(key.GetDutyId()),
			Scope:     metawire.RootDutyScopeFromProto(key.GetScope()),
		},
		MaxSeenEra:            state.GetMaxSeenEra(),
		RetiredEraFloor:       state.GetRetiredEraFloor(),
		MaxRootToken:          metawire.AuthorityTokenFromRootTailToken(state.GetMaxRootToken()),
		MaxDescriptorRevision: state.GetMaxDescriptorRevision(),
		MaxFrontier:           metawire.RootDutyBoundFromProto(state.GetMaxFrontier()),
	}
}
