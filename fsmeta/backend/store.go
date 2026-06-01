// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package backend

import "context"

// KV is the minimal ordered key/value tuple fsmeta consumes from backend scans.
type KV struct {
	Family MetadataFamily
	Key    []byte
	Value  []byte
}

// MetadataFamily names a storage-engine-neutral metadata record family. The
// family is part of the backend contract so physical engines can map namespace
// records to native trees without learning fsmeta semantics.
type MetadataFamily uint8

const (
	MetadataFamilyUnspecified MetadataFamily = iota
	MetadataFamilyMount
	MetadataFamilyInode
	MetadataFamilyDentry
	MetadataFamilyParent
	MetadataFamilyChunk
	MetadataFamilySession
	MetadataFamilyQuota
	MetadataFamilySnapshot
	MetadataFamilyPathIndex
	MetadataFamilyWatch
	MetadataFamilyCommandDedupe
	MetadataFamilySegment
)

// KeyRef is a metadata key annotated with its storage-neutral family.
type KeyRef struct {
	Family MetadataFamily
	Key    []byte
}

// MutationOp names a versioned metadata write operation.
type MutationOp uint8

const (
	MutationPut MutationOp = iota
	MutationDelete
)

// Mutation describes one versioned metadata key mutation.
type Mutation struct {
	Family            MetadataFamily
	Op                MutationOp
	Key               []byte
	Value             []byte
	AssertionNotExist bool
	ExpiresAt         uint64
}

// PredicateKind names a storage predicate a backend must validate under the
// same read/order boundary as its one-phase mutation.
type PredicateKind uint8

const (
	PredicateNotExists PredicateKind = iota
	PredicateExists
	PredicateValueEquals
	PredicatePrefixEmpty
)

// Predicate describes a backend-validated read predicate for one-phase
// metadata mutations.
type Predicate struct {
	Family        MetadataFamily
	Key           []byte
	Kind          PredicateKind
	ReadVersion   uint64
	ExpectedValue []byte
}

// MetadataCommand is the semantic metadata commit object fsmeta/exec submits to
// runtimes. It groups the predicates, mutations, and watch projection that must
// be evaluated and applied under one metadata commit boundary.
type MetadataCommand struct {
	RequestID     []byte
	Mount         string
	MountKeyID    uint64
	PrimaryFamily MetadataFamily
	PrimaryKey    []byte
	ReadVersion   uint64
	CommitVersion uint64
	Predicates    []*Predicate
	Mutations     []*Mutation
	WatchKeys     [][]byte
	WatchRefs     []KeyRef
}

// MetadataCommitResult describes the committed data-plane frontier for a
// MetadataCommand. Local runtimes may leave RegionID and Term unset; replicated
// runtimes must return the region and raft log frontier that applied the
// command.
type MetadataCommitResult struct {
	CommitVersion    uint64
	RegionID         uint64
	Term             uint64
	Index            uint64
	AppliedMutations uint64
}

// Store is the minimum versioned metadata backend required by fsmeta execution.
//
// Mutation atomicity is defined over the supplied mutation group. Implementors
// may use a local one-phase write, a mount-scoped Raft command, or another
// equivalent protocol, but operational data movement, physical ingest/export,
// and engine diagnostics are intentionally outside this contract.
type Store interface {
	ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
	Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
	CommitMetadata(ctx context.Context, command MetadataCommand) (MetadataCommitResult, error)
}

// StatsProvider is implemented by lower runtime layers that expose diagnostics
// without making observability part of Store.
type StatsProvider interface {
	Stats() map[string]any
}
