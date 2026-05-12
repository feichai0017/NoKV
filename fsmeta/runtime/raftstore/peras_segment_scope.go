package raftstore

import (
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/runtime/perasauthority"
)

func grantHasPredecessor(grant perasauthority.AuthorityGrant) bool {
	var zero [32]byte
	return grant.EpochID > 1 && grant.PredecessorDigest != zero
}

func perasSegmentWithinScope(segment fsperas.PerasSegment, scope compile.AuthorityScope) bool {
	if perasauthority.ScopeEmpty(scope) {
		return true
	}
	checked := false
	for _, entry := range segment.EntriesView() {
		parts, ok := fsmeta.InspectKey(entry.Key)
		if !ok {
			if checked {
				return false
			}
			continue
		}
		checked = true
		if !perasScopeCoversKeyParts(scope, parts) {
			return false
		}
	}
	return true
}

func perasCatalogBuckets(scope compile.AuthorityScope) []fsmeta.AffinityBucket {
	if scope.MountKeyID == 0 {
		return nil
	}
	if len(scope.Buckets) > 0 {
		buckets := append([]fsmeta.AffinityBucket(nil), scope.Buckets...)
		slices.Sort(buckets)
		return slices.Compact(buckets)
	}
	buckets := make([]fsmeta.AffinityBucket, fsmeta.DefaultAffinityBucketCount)
	for i := range buckets {
		buckets[i] = fsmeta.AffinityBucket(i)
	}
	return buckets
}

func perasScopeCoversKeyParts(scope compile.AuthorityScope, parts fsmeta.KeyParts) bool {
	if scope.MountKeyID == 0 || parts.MountKeyID != scope.MountKeyID {
		return false
	}
	if len(scope.Buckets) > 0 && !slices.Contains(scope.Buckets, parts.Bucket) {
		return false
	}
	switch parts.Kind {
	case fsmeta.KeyKindDentry:
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, parts.Parent)
	case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
		return len(scope.Inodes) == 0 || slices.Contains(scope.Inodes, parts.Inode)
	case fsmeta.KeyKindUsage:
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, parts.UsageScope)
	default:
		return true
	}
}
