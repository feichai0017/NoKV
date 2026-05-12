package peras

import (
	"fmt"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
)

type replayBucketKey struct {
	mount  fsmeta.MountKeyID
	bucket fsmeta.AffinityBucket
}

// SplitReplayPlanByFSMetaBucket keeps segment install units inside one fsmeta
// affinity bucket without splitting a logical operation. A cross-bucket
// operation must stay on the ordinary durable path until Peras has a group
// atomic install protocol.
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
		key, ok, err := replayOperationBucket(op)
		if err != nil {
			return nil, err
		}
		if !ok {
			haveOpaque = true
			if haveFSMeta {
				return nil, ErrInvalidPerasSegment
			}
			continue
		}
		haveFSMeta = true
		if haveOpaque {
			return nil, ErrInvalidPerasSegment
		}
		group := groups[key]
		if group == nil {
			group = &bucketPlan{first: i}
			groups[key] = group
		}
		group.ops = append(group.ops, cloneReplayOperation(op))
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

// SplitReplayPlanForCatalogInstall preserves fsmeta operations as one logical
// segment whenever the install path writes only Peras catalog records. The
// raftstore installer will write one bucket-local catalog object per touched
// bucket, so a single segment may safely cover root dentry + workspace inode
// creates without forcing the foreground operation back to the ordinary path.
func SplitReplayPlanForCatalogInstall(plan ReplayPlan) ([]ReplayPlan, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		return nil, ErrReplayVersionRequired
	}
	haveFSMeta := false
	haveOpaque := false
	for _, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return nil, ErrInvalidPerasSegment
		}
		for _, mutation := range op.Mutations {
			if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
				return nil, ErrInvalidPerasSegment
			}
			if _, ok := fsmeta.InspectKey(mutation.Key); ok {
				haveFSMeta = true
			} else {
				haveOpaque = true
			}
			if haveFSMeta && haveOpaque {
				return nil, ErrInvalidPerasSegment
			}
		}
	}
	if haveOpaque {
		return SplitReplayPlanByFSMetaBucket(plan)
	}
	return []ReplayPlan{{EpochID: plan.EpochID, Operations: cloneReplayOperations(plan.Operations)}}, nil
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
