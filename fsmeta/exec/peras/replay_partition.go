package peras

import (
	"fmt"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type replayBucketKey struct {
	mount  fsmeta.MountKeyID
	bucket fsmeta.AffinityBucket
}

// SplitReplayPlanByFSMetaBucket keeps segment install units inside one fsmeta
// affinity bucket. A logical operation may span buckets; in that case each
// bucket receives the operation's local mutations and the caller must release
// the original holder operation only after every returned plan installs.
func SplitReplayPlanByFSMetaBucket(plan ReplayPlan) ([]ReplayPlan, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		return nil, ErrReplayVersionRequired
	}
	type bucketPlan struct {
		first int
		ops   []ReplayOperation
	}
	groups := make(map[replayBucketKey]*bucketPlan)
	haveFSMeta := false
	haveOpaque := false
	for i, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return nil, ErrInvalidPerasSegment
		}
		opBuckets := make(map[replayBucketKey][]ReplayMutation)
		opBucketOrder := make([]replayBucketKey, 0, len(op.Mutations))
		for _, mutation := range op.Mutations {
			key, ok, err := replayMutationBucket(mutation)
			if err != nil {
				return nil, err
			}
			if !ok {
				haveOpaque = true
				continue
			}
			haveFSMeta = true
			if _, exists := opBuckets[key]; !exists {
				opBucketOrder = append(opBucketOrder, key)
			}
			opBuckets[key] = append(opBuckets[key], cloneReplayMutation(mutation))
		}
		if haveOpaque && haveFSMeta {
			return nil, ErrInvalidPerasSegment
		}
		if len(opBuckets) == 0 {
			continue
		}
		for _, key := range opBucketOrder {
			group := groups[key]
			if group == nil {
				group = &bucketPlan{first: i}
				groups[key] = group
			}
			group.ops = append(group.ops, ReplayOperation{
				OpID:      op.OpID,
				Kind:      op.Kind,
				Mutations: opBuckets[key],
			})
		}
	}
	if haveOpaque {
		return []ReplayPlan{{EpochID: plan.EpochID, Operations: cloneReplayOperations(plan.Operations)}}, nil
	}
	ordered := make([]*bucketPlan, 0, len(groups))
	for _, group := range groups {
		ordered = append(ordered, group)
	}
	slices.SortFunc(ordered, func(a, b *bucketPlan) int {
		if a.first < b.first {
			return -1
		}
		if a.first > b.first {
			return 1
		}
		return 0
	})
	out := make([]ReplayPlan, 0, len(ordered))
	for _, group := range ordered {
		out = append(out, ReplayPlan{
			EpochID:    plan.EpochID,
			Operations: group.ops,
		})
	}
	return out, nil
}

// SplitReplayPlanByMutationBudget keeps each segment install under the local
// write-batch entry budget. The budget is expressed in replay mutations rather
// than encoded bytes because one replay mutation expands to at most three MVCC
// entries plus one segment catalog entry.
func SplitReplayPlanByMutationBudget(plan ReplayPlan, maxMutations int) ([]ReplayPlan, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 || maxMutations <= 0 {
		return nil, ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		return nil, ErrReplayVersionRequired
	}
	out := make([]ReplayPlan, 0, len(plan.Operations))
	current := ReplayPlan{EpochID: plan.EpochID}
	currentMutations := 0
	flush := func() {
		if len(current.Operations) == 0 {
			return
		}
		out = append(out, ReplayPlan{
			EpochID:    current.EpochID,
			Operations: cloneReplayOperations(current.Operations),
		})
		current.Operations = current.Operations[:0]
		currentMutations = 0
	}
	for _, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return nil, ErrInvalidPerasSegment
		}
		mutations := len(op.Mutations)
		if len(current.Operations) > 0 && currentMutations+mutations > maxMutations {
			flush()
		}
		current.Operations = append(current.Operations, cloneReplayOperation(op))
		currentMutations += mutations
		if currentMutations >= maxMutations {
			flush()
		}
	}
	flush()
	return out, nil
}

func DeltaWritesSingleFSMetaBucket(delta compile.SemanticDelta) (bool, error) {
	if len(delta.WriteEffects) == 0 {
		return false, ErrInvalidPerasSegment
	}
	var bucket replayBucketKey
	var haveBucket bool
	var haveOpaque bool
	for _, effect := range delta.WriteEffects {
		switch effect.Kind {
		case compile.EffectPut:
			if len(effect.Key) == 0 || effect.Value == nil {
				return false, ErrInvalidPerasSegment
			}
		case compile.EffectDelete:
			if len(effect.Key) == 0 {
				return false, ErrInvalidPerasSegment
			}
		default:
			return false, ErrInvalidPerasSegment
		}
		parts, ok := fsmeta.InspectKey(effect.Key)
		if !ok {
			if haveBucket {
				return false, nil
			}
			haveOpaque = true
			continue
		}
		if haveOpaque {
			return false, nil
		}
		key := replayBucketKey{mount: parts.MountKeyID, bucket: parts.Bucket}
		if !haveBucket {
			bucket = key
			haveBucket = true
			continue
		}
		if key != bucket {
			return false, nil
		}
	}
	return true, nil
}

func replayOperationBucket(op ReplayOperation) (replayBucketKey, bool, error) {
	if !op.OpID.Valid() || len(op.Mutations) == 0 {
		return replayBucketKey{}, false, ErrInvalidPerasSegment
	}
	var out replayBucketKey
	for i, mutation := range op.Mutations {
		parts, ok := fsmeta.InspectKey(mutation.Key)
		if !ok {
			return replayBucketKey{}, false, nil
		}
		key := replayBucketKey{mount: parts.MountKeyID, bucket: parts.Bucket}
		if i == 0 {
			out = key
			continue
		}
		if key != out {
			return replayBucketKey{}, false, fmt.Errorf("%w: operation spans fsmeta buckets", ErrInvalidPerasSegment)
		}
	}
	return out, true, nil
}

func replayMutationBucket(mutation ReplayMutation) (replayBucketKey, bool, error) {
	if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
		return replayBucketKey{}, false, ErrInvalidPerasSegment
	}
	parts, ok := fsmeta.InspectKey(mutation.Key)
	if !ok {
		return replayBucketKey{}, false, nil
	}
	return replayBucketKey{mount: parts.MountKeyID, bucket: parts.Bucket}, true, nil
}

func cloneReplayMutation(mutation ReplayMutation) ReplayMutation {
	return ReplayMutation{
		Key:    cloneBytes(mutation.Key),
		Value:  cloneBytes(mutation.Value),
		Delete: mutation.Delete,
	}
}
