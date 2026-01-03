package compact

import (
	"math"
	"sort"
)

// IngestShardView is a lightweight view of an ingest shard for strategy decisions.
type IngestShardView struct {
	Index        int
	TableCount   int
	SizeBytes    int64
	ValueBytes   int64
	MaxAgeSec    float64
	ValueDensity float64
}

// IngestPickInput bundles inputs for ingest shard picking.
type IngestPickInput struct {
	Shards []IngestShardView
}

// PickShardOrder returns shard indices sorted by backlog size (largest first).
func PickShardOrder(in IngestPickInput) []int {
	if len(in.Shards) == 0 {
		return nil
	}
	shards := append([]IngestShardView(nil), in.Shards...)
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].SizeBytes > shards[j].SizeBytes
	})
	out := make([]int, 0, len(shards))
	for _, sh := range shards {
		out = append(out, sh.Index)
	}
	return out
}

// PickShardByBacklog returns the shard index with the highest backlog score.
func PickShardByBacklog(in IngestPickInput) int {
	if len(in.Shards) == 0 {
		return -1
	}
	best := in.Shards[0]
	bestScore := backlogScore(best)
	for i := 1; i < len(in.Shards); i++ {
		score := backlogScore(in.Shards[i])
		if score > bestScore {
			best = in.Shards[i]
			bestScore = score
		}
	}
	return best.Index
}

func backlogScore(sh IngestShardView) float64 {
	score := float64(sh.SizeBytes)
	if sh.MaxAgeSec > 0 {
		score *= 1.0 + math.Min(sh.MaxAgeSec/60.0, 4.0)
	}
	if sh.ValueDensity > 0 {
		score *= 1.0 + math.Min(sh.ValueDensity, 1.0)
	}
	return score
}
