package peras

import (
	"errors"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
)

var ErrReplayVersionRequired = errors.New("fsmeta peras: replay version required")

// InternalEntryApplier is the storage surface needed to persist materialized
// MVCC replay records.
type InternalEntryApplier interface {
	ApplyInternalEntries(entries []*entrykv.Entry) error
}

// MVCCReplayStore materializes sealed Peras effects into NoKV's existing
// Percolator-readable MVCC column-family layout.
type MVCCReplayStore struct {
	db          InternalEntryApplier
	nextVersion uint64
	remaining   uint64
	bounded     bool
}

// NewMVCCReplayStore binds one replay plan to an explicit MVCC version range.
// Versions are assigned in replay order starting at firstVersion.
func NewMVCCReplayStore(db InternalEntryApplier, firstVersion uint64) (*MVCCReplayStore, error) {
	if db == nil {
		return nil, ErrReplayStoreRequired
	}
	if firstVersion == 0 || firstVersion == entrykv.MaxVersion {
		return nil, ErrReplayVersionRequired
	}
	return &MVCCReplayStore{db: db, nextVersion: firstVersion}, nil
}

// NewMVCCReplayStoreForPlan binds storage to the version range carried by a
// segment replay plan.
func NewMVCCReplayStoreForPlan(db InternalEntryApplier, plan ReplayPlan) (*MVCCReplayStore, error) {
	if db == nil {
		return nil, ErrReplayStoreRequired
	}
	if err := validateReplayPlanForApply(plan); err != nil {
		return nil, err
	}
	if err := plan.Versions.ValidateForOperationCount(ReplayPlanOperationCount(plan)); err != nil {
		return nil, err
	}
	return &MVCCReplayStore{
		db:          db,
		nextVersion: plan.Versions.First,
		remaining:   plan.Versions.Count,
		bounded:     true,
	}, nil
}

func (s *MVCCReplayStore) ApplyPerasReplay(ops []ReplayOperation) error {
	if s == nil || s.db == nil {
		return ErrReplayStoreRequired
	}
	nextVersion := s.nextVersion
	remaining := s.remaining
	entries := make([]*entrykv.Entry, 0, len(ops)*3)
	for _, op := range ops {
		if s.bounded {
			if remaining == 0 {
				releaseMVCCReplayEntries(entries)
				return ErrReplayVersionRequired
			}
			remaining--
		}
		if nextVersion == 0 || nextVersion == entrykv.MaxVersion {
			releaseMVCCReplayEntries(entries)
			return ErrReplayVersionRequired
		}
		opEntries, err := BuildMVCCReplayEntries(op, nextVersion)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return err
		}
		entries = append(entries, opEntries...)
		nextVersion++
	}
	if err := s.db.ApplyInternalEntries(entries); err != nil {
		releaseMVCCReplayEntries(entries)
		return err
	}
	releaseMVCCReplayEntries(entries)
	s.nextVersion = nextVersion
	if s.bounded {
		s.remaining = remaining
	}
	return nil
}

// BuildMVCCReplayEntries converts one committed replay operation into the same
// internal MVCC records that existing Percolator reads already understand. The
// caller owns the returned entries and must DecrRef them after storage apply.
func BuildMVCCReplayEntries(op ReplayOperation, version uint64) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, ErrReplayVersionRequired
	}
	if !op.OpID.Valid() || len(op.Mutations) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	entries := make([]*entrykv.Entry, 0, len(op.Mutations)*3)
	for _, mutation := range op.Mutations {
		mutationEntries, err := buildMutationMVCCReplayEntries(mutation, version)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return nil, err
		}
		entries = append(entries, mutationEntries...)
	}
	return entries, nil
}

// BuildMVCCSegmentInstallEntries materializes one sealed segment as one
// MVCC-visible install version. This is the raftstore Peras install boundary:
// the segment keeps per-operation completion metadata, while the LSM receives
// only the coalesced final key state.
func BuildMVCCSegmentInstallEntries(segment PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	entries := make([]*entrykv.Entry, 0, len(segment.entries)*3+1)
	for _, entry := range segment.entries {
		mutationEntries, err := buildMutationMVCCReplayEntries(ReplayMutation{
			Key:    entry.Key,
			Value:  entry.Value,
			Delete: entry.Delete,
		}, version)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return nil, err
		}
		entries = append(entries, mutationEntries...)
	}
	catalogKey, err := PerasSegmentCatalogKey(segment)
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	catalogValue, err := EncodePerasSegmentCatalogRecord(segment, version)
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, catalogKey, version, catalogValue, 0, 0))
	return entries, nil
}

func buildMutationMVCCReplayEntries(mutation ReplayMutation, version uint64) ([]*entrykv.Entry, error) {
	if len(mutation.Key) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	if mutation.Delete {
		if mutation.Value != nil {
			return nil, ErrInvalidPerasSegment
		}
		write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Delete, StartTs: version})
		return []*entrykv.Entry{
			entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
			entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, write, 0, 0),
		}, nil
	}
	if mutation.Value == nil {
		return nil, ErrInvalidPerasSegment
	}
	write := txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: version}
	if txnmvcc.CanInlineShortValue(kvrpcpb.Mutation_Put, mutation.Value) {
		write.ShortValue = cloneBytes(mutation.Value)
		return []*entrykv.Entry{
			entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, txnmvcc.EncodeWrite(write), 0, 0),
		}, nil
	}
	return []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
		entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, cloneBytes(mutation.Value), 0, 0),
		entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, txnmvcc.EncodeWrite(write), 0, 0),
	}, nil
}

func releaseMVCCReplayEntries(entries []*entrykv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}
