package peras

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
)

var perasSegmentMagic = [4]byte{'N', 'P', 'S', 1}

// SegmentRecordClass is the fsmeta key family stored in one sealed Peras
// segment. The class is diagnostic and query-planning metadata; the storage
// key remains the source of truth.
type SegmentRecordClass uint8

const (
	SegmentRecordOther SegmentRecordClass = iota
	SegmentRecordDentry
	SegmentRecordInode
	SegmentRecordChunk
	SegmentRecordSession
	SegmentRecordUsage
)

func (c SegmentRecordClass) String() string {
	switch c {
	case SegmentRecordDentry:
		return "dentry"
	case SegmentRecordInode:
		return "inode"
	case SegmentRecordChunk:
		return "chunk"
	case SegmentRecordSession:
		return "session"
	case SegmentRecordUsage:
		return "usage"
	default:
		return "other"
	}
}

type SegmentKV struct {
	Class  SegmentRecordClass
	Key    []byte
	Value  []byte
	Delete bool
}

type SegmentCompletion struct {
	OpID          OperationID
	Kind          fsmeta.OperationKind
	Version       uint64
	MutationCount uint32
}

type SegmentStats struct {
	OperationCount       uint64
	InputMutationCount   uint64
	EntryCount           uint64
	CompletionCount      uint64
	CoalescedMutations   uint64
	CompressionRatio     float64
	OperationsPerSegment float64
}

// PerasSegment is the authority-local install unit produced by sealing a
// batch of Peras operations. It is queryable before later materialization into
// the ordinary LSM layout.
type PerasSegment struct {
	EpochID  uint64
	Versions ReplayVersionRange
	Root     [32]byte

	Dentries   []SegmentKV
	Inodes     []SegmentKV
	Chunks     []SegmentKV
	Sessions   []SegmentKV
	Usage      []SegmentKV
	Other      []SegmentKV
	Tombstones []SegmentKV

	Completions []SegmentCompletion

	entries            []SegmentKV
	completionIndex    map[OperationID]int
	inputMutationCount uint64
}

// BuildPerasSegment turns a committed Peras seal into a compact, sorted segment
// that can answer holder-local reads and later be installed as one raftstore
// object.
func BuildPerasSegment(seal PerasSeal) (PerasSegment, error) {
	plan, err := BuildReplayPlan(seal)
	if err != nil {
		return PerasSegment{}, err
	}
	return BuildPerasSegmentFromReplayPlan(plan)
}

func BuildPerasSegmentFromReplayPlan(plan ReplayPlan) (PerasSegment, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return PerasSegment{}, ErrInvalidPerasSeal
	}
	if !plan.Versions.Empty() {
		if err := plan.Versions.ValidateForOperationCount(uint64(len(plan.Operations))); err != nil {
			return PerasSegment{}, err
		}
	}

	latest := make(map[string]SegmentKV)
	completions := make([]SegmentCompletion, 0, len(plan.Operations))
	var mutationCount uint64
	for opOffset, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return PerasSegment{}, ErrInvalidPerasSeal
		}
		version := uint64(0)
		if !plan.Versions.Empty() {
			v, err := plan.Versions.VersionAt(uint64(opOffset))
			if err != nil {
				return PerasSegment{}, err
			}
			version = v
		}
		completions = append(completions, SegmentCompletion{
			OpID:          op.OpID,
			Kind:          op.Kind,
			Version:       version,
			MutationCount: uint32(len(op.Mutations)),
		})
		for _, mutation := range op.Mutations {
			if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
				return PerasSegment{}, ErrInvalidPerasSeal
			}
			kv := SegmentKV{
				Class:  classifySegmentKey(mutation.Key),
				Key:    cloneBytes(mutation.Key),
				Value:  cloneBytes(mutation.Value),
				Delete: mutation.Delete,
			}
			latest[string(mutation.Key)] = kv
			mutationCount++
		}
	}

	entries := make([]SegmentKV, 0, len(latest))
	for _, kv := range latest {
		entries = append(entries, kv)
	}
	slices.SortFunc(entries, compareSegmentKV)

	segment := PerasSegment{
		EpochID:            plan.EpochID,
		Versions:           plan.Versions,
		Completions:        completions,
		entries:            entries,
		completionIndex:    make(map[OperationID]int, len(completions)),
		inputMutationCount: mutationCount,
	}
	for i, completion := range segment.Completions {
		segment.completionIndex[completion.OpID] = i
	}
	segment.assignRuns(entries)
	segment.Root = segmentRoot(segment)
	return segment, nil
}

func (s PerasSegment) Get(key []byte) (value []byte, deleted bool, ok bool) {
	value, deleted, ok = s.GetView(key)
	if !ok {
		return nil, false, false
	}
	return cloneBytes(value), deleted, true
}

// GetView returns the segment-owned value bytes. Callers must not mutate the
// returned slice.
func (s PerasSegment) GetView(key []byte) (value []byte, deleted bool, ok bool) {
	i, found := s.find(key)
	if !found {
		return nil, false, false
	}
	entry := s.entries[i]
	return entry.Value, entry.Delete, true
}

func (s PerasSegment) Scan(start []byte, limit uint32) []SegmentKV {
	return cloneSegmentKVs(s.ScanView(start, limit))
}

// ScanView returns a segment-owned sorted suffix. Callers must not mutate the
// returned slice or the bytes inside it.
func (s PerasSegment) ScanView(start []byte, limit uint32) []SegmentKV {
	if limit == 0 || len(s.entries) == 0 {
		return nil
	}
	i, _ := s.lowerBound(start)
	if i >= len(s.entries) {
		return nil
	}
	end := i + int(limit)
	if end > len(s.entries) {
		end = len(s.entries)
	}
	return s.entries[i:end]
}

func (s PerasSegment) Completion(id OperationID) (SegmentCompletion, bool) {
	if !id.Valid() {
		return SegmentCompletion{}, false
	}
	if s.completionIndex != nil {
		i, ok := s.completionIndex[id]
		if !ok {
			return SegmentCompletion{}, false
		}
		return s.Completions[i], true
	}
	for _, completion := range s.Completions {
		if completion.OpID == id {
			return completion, true
		}
	}
	return SegmentCompletion{}, false
}

func (s PerasSegment) Entries() []SegmentKV {
	return cloneSegmentKVs(s.entries)
}

func (s PerasSegment) Stats() SegmentStats {
	entryCount := uint64(len(s.entries))
	stats := SegmentStats{
		OperationCount:       uint64(len(s.Completions)),
		InputMutationCount:   s.inputMutationCount,
		EntryCount:           entryCount,
		CompletionCount:      uint64(len(s.Completions)),
		OperationsPerSegment: float64(len(s.Completions)),
	}
	if s.inputMutationCount > entryCount {
		stats.CoalescedMutations = s.inputMutationCount - entryCount
	}
	if entryCount == 0 {
		stats.CompressionRatio = 1
	} else {
		stats.CompressionRatio = float64(s.inputMutationCount) / float64(entryCount)
	}
	return stats
}

func (s *PerasSegment) assignRuns(entries []SegmentKV) {
	for _, entry := range entries {
		if entry.Delete {
			s.Tombstones = append(s.Tombstones, entry)
			continue
		}
		switch entry.Class {
		case SegmentRecordDentry:
			s.Dentries = append(s.Dentries, entry)
		case SegmentRecordInode:
			s.Inodes = append(s.Inodes, entry)
		case SegmentRecordChunk:
			s.Chunks = append(s.Chunks, entry)
		case SegmentRecordSession:
			s.Sessions = append(s.Sessions, entry)
		case SegmentRecordUsage:
			s.Usage = append(s.Usage, entry)
		default:
			s.Other = append(s.Other, entry)
		}
	}
}

func (s PerasSegment) find(key []byte) (int, bool) {
	i, equal := s.lowerBound(key)
	return i, equal
}

func (s PerasSegment) lowerBound(key []byte) (int, bool) {
	i, j := 0, len(s.entries)
	for i < j {
		h := int(uint(i+j) >> 1)
		if bytes.Compare(s.entries[h].Key, key) < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < len(s.entries) && bytes.Equal(s.entries[i].Key, key)
}

func compareSegmentKV(left, right SegmentKV) int {
	if cmp := bytes.Compare(left.Key, right.Key); cmp != 0 {
		return cmp
	}
	if left.Delete == right.Delete {
		return 0
	}
	if left.Delete {
		return 1
	}
	return -1
}

func classifySegmentKey(key []byte) SegmentRecordClass {
	kind, err := fsmeta.KeyKindOf(key)
	if err != nil {
		return SegmentRecordOther
	}
	switch kind {
	case fsmeta.KeyKindDentry:
		return SegmentRecordDentry
	case fsmeta.KeyKindInode:
		return SegmentRecordInode
	case fsmeta.KeyKindChunk:
		return SegmentRecordChunk
	case fsmeta.KeyKindSession:
		return SegmentRecordSession
	case fsmeta.KeyKindUsage:
		return SegmentRecordUsage
	default:
		return SegmentRecordOther
	}
}

func segmentRoot(segment PerasSegment) [32]byte {
	h := sha256.New()
	writeFixed(h, perasSegmentMagic[:])
	writeUint64(h, segment.EpochID)
	writeUint64(h, segment.Versions.First)
	writeUint64(h, segment.Versions.Count)
	writeUint64(h, uint64(len(segment.entries)))
	for _, entry := range segment.entries {
		writeUint64(h, uint64(entry.Class))
		writeBytesHash(h, entry.Key)
		writeBool(h, entry.Delete)
		writeBytesHash(h, entry.Value)
	}
	writeUint64(h, uint64(len(segment.Completions)))
	for _, completion := range segment.Completions {
		writeOperationID(h, completion.OpID)
		writeString(h, string(completion.Kind))
		writeUint64(h, completion.Version)
		writeUint64(h, uint64(completion.MutationCount))
	}
	return digestFromHash(h.Sum(nil))
}

func cloneSegmentKV(in SegmentKV) SegmentKV {
	return SegmentKV{
		Class:  in.Class,
		Key:    cloneBytes(in.Key),
		Value:  cloneBytes(in.Value),
		Delete: in.Delete,
	}
}

func cloneSegmentKVs(in []SegmentKV) []SegmentKV {
	if len(in) == 0 {
		return nil
	}
	out := make([]SegmentKV, 0, len(in))
	for _, kv := range in {
		out = append(out, cloneSegmentKV(kv))
	}
	return out
}
