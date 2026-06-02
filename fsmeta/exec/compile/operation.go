// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
	"unsafe"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// CompiledOp is the semantic descriptor for one metadata operation. The
// executor consumes Delta to build MetadataCommand; the remaining fields keep
// compiler-owned diagnostics and projections at the fsmeta boundary.
type CompiledOp struct {
	Delta            SemanticDelta
	DescriptorDigest [32]byte
	Authority        AuthorityPlan
	Footprint        KeyFootprint
	Predicates       []PredicateObligation
	Guards           []GuardObligation
	Effects          []EffectPlan
	Atomicity        AtomicityGroup
	Durability       DurabilityClass
	Watch            []WatchProjection
	Completion       CompletionPlan
}

type FenceMode uint8

const (
	FenceNone FenceMode = iota
	FenceActiveAuthority
)

type AuthorityPlan struct {
	Scope    AuthorityScope
	Required bool
	Fence    FenceMode
}

type KeyAccessMode uint8

const (
	KeyAccessRead KeyAccessMode = iota
	KeyAccessReadPrefix
	KeyAccessWrite
)

type KeyRef struct {
	Mode       KeyAccessMode
	Key        []byte
	Opaque     bool
	MountKeyID model.MountKeyID
	Bucket     layout.AffinityBucket
	Kind       layout.KeyKind
	Parent     model.InodeID
	Inode      model.InodeID
}

type KeyFootprint struct {
	Reads          []KeyRef
	Writes         []KeyRef
	ConflictKeys   []KeyRef
	HasPrefixRead  bool
	HasOpaqueKeys  bool
	EstimatedBytes uint64
}

type PredicateObligation struct {
	Kind             PredicateKind
	Key              []byte
	NeedValue        bool
	NeedAbsent       bool
	Guard            RuntimeGuard
	HasExpectedValue bool
	ExpectHash       [32]byte
}

type GuardObligation struct {
	Guard  RuntimeGuard
	Digest [32]byte
}

type DerivationKind uint8

const (
	DerivationNone DerivationKind = iota
	DerivationRuntimeValue
)

type EffectPlan struct {
	ID         MutationID
	Kind       EffectKind
	Key        []byte
	Value      []byte
	Concrete   bool
	Opaque     bool
	MountKeyID model.MountKeyID
	Bucket     layout.AffinityBucket
	RecordKind layout.KeyKind
	ValueHash  [32]byte
	Derivation DerivationKind
}

type MutationID uint16

type RecoveryRule uint8

const (
	RecoveryReplayAllOrNothing RecoveryRule = iota
)

type AtomicityGroup struct {
	Members    []MutationID
	Splittable bool
	Recovery   RecoveryRule
	Digest     [32]byte
}

type DurabilityClass uint8

const (
	DurabilityVisibleOnly DurabilityClass = iota
	DurabilityNeedsFsyncDir
	DurabilityNeedsCloseSession
	DurabilityNeedsPublishCheckpoint
)

type WatchEmitPoint uint8

const (
	WatchEmitVisible WatchEmitPoint = iota
	WatchEmitSeal
)

type WatchEventKind uint8

const (
	WatchEventUnknown WatchEventKind = iota
	WatchEventCreate
	WatchEventDelete
	WatchEventRename
	WatchEventUpdate
)

type WatchProjection struct {
	EventKind WatchEventKind
	Key       []byte
	Parent    model.InodeID
	Name      string
	Inode     model.InodeID
	EmitAt    WatchEmitPoint
}

type CompletionKind uint8

const (
	CompletionNone CompletionKind = iota
	CompletionVisible
	CompletionDurable
)

type CompletionPlan struct {
	RetainCompletion bool
	Kind             CompletionKind
	MutationCount    uint32
	DescriptorDigest [32]byte
}

func fenceMode(delta SemanticDelta) FenceMode {
	if delta.Eligibility != EligibilityVisibleCommit {
		return FenceNone
	}
	return FenceActiveAuthority
}

func keyRef(mode KeyAccessMode, key []byte) KeyRef {
	out := KeyRef{
		Mode: mode,
		Key:  key,
	}
	parts, ok := layout.InspectKey(key)
	if !ok {
		out.Opaque = len(key) > 0
		return out
	}
	out.MountKeyID = parts.MountKeyID
	out.Bucket = parts.Bucket
	out.Kind = parts.Kind
	out.Parent = parts.Parent
	out.Inode = parts.Inode
	return out
}

func semanticKeyBindingMatches(delta SemanticDelta, actual []byte, binding string) bool {
	if binding == "" || binding == "runtime" {
		return true
	}
	expected, ok := semanticKeyBinding(delta, binding)
	if !ok {
		return false
	}
	return bytes.Equal(actual, expected)
}

func semanticIndexedKeyBindingMatches(delta SemanticDelta, actual []byte, family string, index int) bool {
	if family == "" || family == "runtime" || index < 0 {
		return true
	}
	var expected []byte
	switch family {
	case "read":
		if index >= len(delta.Plan.ReadKeys) {
			return false
		}
		expected = delta.Plan.ReadKeys[index]
	case "read_prefix":
		if index >= len(delta.Plan.ReadPrefixes) {
			return false
		}
		expected = delta.Plan.ReadPrefixes[index]
	case "mutate":
		if index >= len(delta.Plan.MutateKeys) {
			return false
		}
		expected = delta.Plan.MutateKeys[index]
	default:
		return false
	}
	return len(expected) != 0 && bytes.Equal(actual, expected)
}

func semanticKeyBinding(delta SemanticDelta, binding string) ([]byte, bool) {
	switch binding {
	case "primary":
		return delta.Plan.PrimaryKey, len(delta.Plan.PrimaryKey) != 0
	case "owner":
		return semanticOwnerKey(delta)
	}
	if prefix, ok := strings.CutSuffix(binding, "]"); ok {
		name, indexText, ok := strings.Cut(prefix, "[")
		if !ok {
			return nil, false
		}
		index, err := strconv.Atoi(indexText)
		if err != nil || index < 0 {
			return nil, false
		}
		switch name {
		case "read":
			if index >= len(delta.Plan.ReadKeys) {
				return nil, false
			}
			return delta.Plan.ReadKeys[index], len(delta.Plan.ReadKeys[index]) != 0
		case "read_prefix":
			if index >= len(delta.Plan.ReadPrefixes) {
				return nil, false
			}
			return delta.Plan.ReadPrefixes[index], len(delta.Plan.ReadPrefixes[index]) != 0
		case "mutate":
			if index >= len(delta.Plan.MutateKeys) {
				return nil, false
			}
			return delta.Plan.MutateKeys[index], len(delta.Plan.MutateKeys[index]) != 0
		case "predicate":
			if index >= len(delta.ReadPredicates) {
				return nil, false
			}
			return delta.ReadPredicates[index].Key, len(delta.ReadPredicates[index].Key) != 0
		}
	}
	return nil, false
}

func semanticOwnerKey(delta SemanticDelta) ([]byte, bool) {
	var sessionKey []byte
	switch {
	case len(delta.Plan.ReadKeys) > 0:
		sessionKey = delta.Plan.ReadKeys[0]
	case len(delta.Plan.PrimaryKey) > 0:
		sessionKey = delta.Plan.PrimaryKey
	default:
		return nil, false
	}
	parts, ok := layout.InspectKey(sessionKey)
	if !ok || parts.Kind != layout.KeyKindSession || parts.Inode == 0 || delta.Authority.Mount == "" {
		return nil, false
	}
	key, err := layout.EncodeInodeSessionKey(model.MountIdentity{
		MountID:    delta.Authority.Mount,
		MountKeyID: parts.MountKeyID,
	}, parts.Inode)
	return key, err == nil
}

func watchEventKind(delta SemanticDelta, effect WriteEffect) WatchEventKind {
	switch delta.Kind {
	case model.OperationCreate:
		return WatchEventCreate
	case model.OperationRename, model.OperationRenameReplace, model.OperationRenameSubtree:
		return WatchEventRename
	case model.OperationUnlink, model.OperationRemove, model.OperationRemoveDirectory:
		return WatchEventDelete
	}
	switch effect.Kind {
	case EffectDelete, EffectDerivedDelete:
		return WatchEventDelete
	case EffectPut, EffectDerivedPut:
		return WatchEventUpdate
	default:
		return WatchEventUnknown
	}
}

func dentryName(key []byte) string {
	name, ok := layout.DentryNameBytesOfKey(key)
	if !ok || len(name) == 0 {
		return ""
	}
	return unsafe.String(&name[0], len(name))
}

func GuardObligationDigest(guard RuntimeGuard) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(guard))
	return h.sum()
}

func KeyFootprintDigest(footprint KeyFootprint) [32]byte {
	h := newDigestBuilder()
	h.writeUint64(uint64(len(footprint.Reads)))
	for _, ref := range footprint.Reads {
		h.writeKeyRef(ref)
	}
	h.writeUint64(uint64(len(footprint.Writes)))
	for _, ref := range footprint.Writes {
		h.writeKeyRef(ref)
	}
	h.writeUint64(uint64(len(footprint.ConflictKeys)))
	for _, ref := range footprint.ConflictKeys {
		h.writeKeyRef(ref)
	}
	h.writeBool(footprint.HasPrefixRead)
	h.writeBool(footprint.HasOpaqueKeys)
	h.writeUint64(footprint.EstimatedBytes)
	return h.sum()
}

func EffectPlanDigest(effects []EffectPlan) [32]byte {
	if len(effects) == 0 {
		return [32]byte{}
	}
	h := newDigestBuilder()
	h.writeUint64(uint64(len(effects)))
	for _, effect := range effects {
		h.writeUint64(uint64(effect.ID))
		h.writeUint64(uint64(effect.Kind))
		h.writeBytes(effect.Key)
		h.writeBytes(effect.Value)
		h.writeBool(effect.Concrete)
		h.writeBool(effect.Opaque)
		h.writeUint64(uint64(effect.MountKeyID))
		h.writeUint64(uint64(effect.Bucket))
		h.writeUint64(uint64(effect.RecordKind))
		h.writeBytes(effect.ValueHash[:])
		h.writeUint64(uint64(effect.Derivation))
	}
	return h.sum()
}

func descriptorDigest(delta SemanticDelta) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(delta.Kind))
	h.writeString(delta.Eligibility.String())
	h.writeString(string(delta.SlowReason))
	if delta.DurabilityBarrier {
		h.writeUint64(1)
	}
	if delta.WatchAtSeal {
		h.writeUint64(1)
	}
	h.writeUint64(uint64(delta.Authority.MountKeyID))
	for _, bucket := range delta.Authority.Buckets {
		h.writeUint64(uint64(bucket))
	}
	for _, predicate := range delta.ReadPredicates {
		h.writeUint64(uint64(predicate.Kind))
		h.writeBytes(predicate.Key)
		if predicate.HasExpectedValue {
			h.writeBytes(predicate.ExpectedValue)
		}
	}
	for _, effect := range delta.WriteEffects {
		h.writeUint64(uint64(effect.Kind))
		h.writeBytes(effect.Key)
		h.writeBytes(effect.Value)
	}
	for _, guard := range delta.RuntimeGuards {
		h.writeString(string(guard))
	}
	return h.sum()
}

type digestBuilder struct {
	stack [512]byte
	off   int
	heap  []byte
}

func newDigestBuilder() digestBuilder {
	return digestBuilder{}
}

func (b *digestBuilder) writeKeyRef(ref KeyRef) {
	b.writeUint64(uint64(ref.Mode))
	b.writeBytes(ref.Key)
	b.writeBool(ref.Opaque)
	b.writeUint64(uint64(ref.MountKeyID))
	b.writeUint64(uint64(ref.Bucket))
	b.writeUint64(uint64(ref.Kind))
	b.writeUint64(uint64(ref.Parent))
	b.writeUint64(uint64(ref.Inode))
}

func (b *digestBuilder) writeString(value string) {
	b.writeUint64(uint64(len(value)))
	b.writeRawString(value)
}

func (b *digestBuilder) writeBytes(value []byte) {
	b.writeUint64(uint64(len(value)))
	b.writeRaw(value)
}

func (b *digestBuilder) writeRaw(value []byte) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		copy(b.stack[b.off:], value)
		b.off += len(value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) writeUint64(value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	b.writeRaw(buf[:])
}

func (b *digestBuilder) writeBool(value bool) {
	if value {
		b.writeUint64(1)
		return
	}
	b.writeUint64(0)
}

func (b *digestBuilder) sum() [32]byte {
	if b.heap != nil {
		return sha256.Sum256(b.heap)
	}
	return sha256.Sum256(b.stack[:b.off])
}

func (b *digestBuilder) writeRawString(value string) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		b.off += copy(b.stack[b.off:], value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) spill(extra int) {
	needed := b.off + extra
	capacity := max(needed, len(b.stack)*2)
	b.heap = make([]byte, b.off, capacity)
	copy(b.heap, b.stack[:b.off])
}

func cloneDelta(delta SemanticDelta) SemanticDelta {
	return SemanticDelta{
		Kind:              delta.Kind,
		Plan:              clonePlan(delta.Plan),
		Authority:         cloneScope(delta.Authority),
		ReadPredicates:    clonePredicates(delta.ReadPredicates),
		WriteEffects:      cloneEffects(delta.WriteEffects),
		RuntimeGuards:     append([]RuntimeGuard(nil), delta.RuntimeGuards...),
		Eligibility:       delta.Eligibility,
		SlowReason:        delta.SlowReason,
		DurabilityBarrier: delta.DurabilityBarrier,
		WatchAtSeal:       delta.WatchAtSeal,
	}
}
