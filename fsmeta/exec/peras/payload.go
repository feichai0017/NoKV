package peras

import (
	"bytes"
	"crypto/sha256"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

var semanticDeltaPayloadMagic = [4]byte{'N', 'C', 'D', 2}

func EncodeSemanticDeltaPayload(delta compile.SemanticDelta) ([]byte, error) {
	var out bytes.Buffer
	writeFixed(&out, semanticDeltaPayloadMagic[:])
	writeString(&out, string(delta.Kind))
	writeOperationPlan(&out, delta.Plan)
	writeAuthorityScope(&out, delta.Authority)
	writePredicates(&out, delta.ReadPredicates)
	writeEffects(&out, delta.WriteEffects)
	writeRuntimeGuards(&out, delta.RuntimeGuards)
	writeUint64(&out, uint64(delta.Eligibility))
	writeString(&out, string(delta.SlowReason))
	writeBool(&out, delta.DurabilityBarrier)
	writeBool(&out, delta.WatchAtSeal)
	return out.Bytes(), nil
}

func DecodeSemanticDeltaPayload(payload []byte) (compile.SemanticDelta, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(semanticDeltaPayloadMagic); err != nil {
		return compile.SemanticDelta{}, err
	}
	kind, err := r.readString()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	plan, err := r.readOperationPlan()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	authority, err := r.readAuthorityScope()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	predicates, err := r.readPredicates()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	effects, err := r.readEffects()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	guards, err := r.readRuntimeGuards()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	eligibility, err := r.readUint64()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	slowReason, err := r.readString()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	durabilityBarrier, err := r.readBool()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	watchAtSeal, err := r.readBool()
	if err != nil {
		return compile.SemanticDelta{}, err
	}
	if !r.done() {
		return compile.SemanticDelta{}, ErrInvalidWitnessRecord
	}
	return compile.SemanticDelta{
		Kind:              fsmeta.OperationKind(kind),
		Plan:              plan,
		Authority:         authority,
		ReadPredicates:    predicates,
		WriteEffects:      effects,
		RuntimeGuards:     guards,
		Eligibility:       compile.Eligibility(eligibility),
		SlowReason:        compile.SlowReason(slowReason),
		DurabilityBarrier: durabilityBarrier,
		WatchAtSeal:       watchAtSeal,
	}, nil
}

func SemanticDeltaPayloadDigest(payload []byte) ([32]byte, error) {
	if len(payload) == 0 {
		return [32]byte{}, ErrInvalidWitnessRecord
	}
	return sha256.Sum256(payload), nil
}

func writeOperationPlan(out *bytes.Buffer, plan fsmeta.OperationPlan) {
	writeString(out, string(plan.Kind))
	writeString(out, string(plan.Mount))
	writeBytes(out, plan.PrimaryKey)
	writeBytes(out, plan.StartKey)
	writeUint32(out, plan.Limit)
	writeKeySets(out, plan.ReadKeys)
	writeKeySets(out, plan.ReadPrefixes)
	writeKeySets(out, plan.MutateKeys)
}

func writeAuthorityScope(out *bytes.Buffer, scope compile.AuthorityScope) {
	writeString(out, string(scope.Mount))
	writeUint64(out, uint64(scope.MountKeyID))
	writeUint32(out, uint32(len(scope.Buckets)))
	for _, bucket := range scope.Buckets {
		writeUint64(out, uint64(bucket))
	}
	writeInodeIDs(out, scope.Parents)
	writeInodeIDs(out, scope.Inodes)
}

func writePredicates(out *bytes.Buffer, predicates []compile.Predicate) {
	writeUint32(out, uint32(len(predicates)))
	for _, predicate := range predicates {
		writeUint64(out, uint64(predicate.Kind))
		writeBytes(out, predicate.Key)
		writeBool(out, predicate.HasExpectedValue)
		if predicate.HasExpectedValue {
			writeBytes(out, predicate.ExpectedValue)
		}
	}
}

func writeEffects(out *bytes.Buffer, effects []compile.WriteEffect) {
	writeUint32(out, uint32(len(effects)))
	for _, effect := range effects {
		writeUint64(out, uint64(effect.Kind))
		writeBytes(out, effect.Key)
		writeBytes(out, effect.Value)
	}
}

func writeRuntimeGuards(out *bytes.Buffer, guards []compile.RuntimeGuard) {
	writeUint32(out, uint32(len(guards)))
	for _, guard := range guards {
		writeString(out, string(guard))
	}
}

func writeKeySets(out *bytes.Buffer, keys [][]byte) {
	writeUint32(out, uint32(len(keys)))
	for _, key := range keys {
		writeBytes(out, key)
	}
}

func writeInodeIDs(out *bytes.Buffer, ids []fsmeta.InodeID) {
	writeUint32(out, uint32(len(ids)))
	for _, id := range ids {
		writeUint64(out, uint64(id))
	}
}

func (r *witnessReader) readOperationPlan() (fsmeta.OperationPlan, error) {
	kind, err := r.readString()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	mount, err := r.readString()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	primary, err := r.readBytes()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	start, err := r.readBytes()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	limit, err := r.readUint32()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	readKeys, err := r.readKeySets()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	readPrefixes, err := r.readKeySets()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	mutateKeys, err := r.readKeySets()
	if err != nil {
		return fsmeta.OperationPlan{}, err
	}
	return fsmeta.OperationPlan{
		Kind:         fsmeta.OperationKind(kind),
		Mount:        fsmeta.MountID(mount),
		PrimaryKey:   primary,
		StartKey:     start,
		Limit:        limit,
		ReadKeys:     readKeys,
		ReadPrefixes: readPrefixes,
		MutateKeys:   mutateKeys,
	}, nil
}

func (r *witnessReader) readAuthorityScope() (compile.AuthorityScope, error) {
	mount, err := r.readString()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	mountKeyID, err := r.readUint64()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	bucketCount, err := r.readUint32()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	buckets := make([]fsmeta.AffinityBucket, 0, bucketCount)
	for range bucketCount {
		bucket, err := r.readUint64()
		if err != nil {
			return compile.AuthorityScope{}, err
		}
		buckets = append(buckets, fsmeta.AffinityBucket(bucket))
	}
	parents, err := r.readInodeIDs()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	inodes, err := r.readInodeIDs()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	return compile.AuthorityScope{
		Mount:      fsmeta.MountID(mount),
		MountKeyID: fsmeta.MountKeyID(mountKeyID),
		Buckets:    buckets,
		Parents:    parents,
		Inodes:     inodes,
	}, nil
}

func (r *witnessReader) readPredicates() ([]compile.Predicate, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]compile.Predicate, 0, count)
	for range count {
		kind, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		key, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		hasExpectedValue, err := r.readBool()
		if err != nil {
			return nil, err
		}
		var expectedValue []byte
		if hasExpectedValue {
			expectedValue, err = r.readBytes()
			if err != nil {
				return nil, err
			}
		}
		out = append(out, compile.Predicate{
			Kind:             compile.PredicateKind(kind),
			Key:              key,
			ExpectedValue:    expectedValue,
			HasExpectedValue: hasExpectedValue,
		})
	}
	return out, nil
}

func (r *witnessReader) readEffects() ([]compile.WriteEffect, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]compile.WriteEffect, 0, count)
	for range count {
		kind, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		key, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		value, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		out = append(out, compile.WriteEffect{Kind: compile.EffectKind(kind), Key: key, Value: value})
	}
	return out, nil
}

func (r *witnessReader) readRuntimeGuards() ([]compile.RuntimeGuard, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]compile.RuntimeGuard, 0, count)
	for range count {
		guard, err := r.readString()
		if err != nil {
			return nil, err
		}
		out = append(out, compile.RuntimeGuard(guard))
	}
	return out, nil
}

func (r *witnessReader) readKeySets() ([][]byte, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, count)
	for range count {
		key, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, nil
}

func (r *witnessReader) readInodeIDs() ([]fsmeta.InodeID, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]fsmeta.InodeID, 0, count)
	for range count {
		id, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		out = append(out, fsmeta.InodeID(id))
	}
	return out, nil
}
