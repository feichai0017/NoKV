package client

import kvrpcpb "github.com/feichai0017/NoKV/pb/kv"

// ReadOptions controls StoreKV read consistency and routing preference.
// The zero value preserves the production default: strong leader-only reads.
type ReadOptions struct {
	Consistency       kvrpcpb.ReadConsistency
	Preference        kvrpcpb.ReadPreference
	MaxStaleReadIndex uint64
	MaxStaleReadMS    uint64
}

// DefaultReadOptions is the regular StoreKV read policy used by Get, BatchGet,
// and Scan. fsmeta mutation plans use read-before-write checks, so the default
// stays on leader lease reads; follower and bounded-stale reads remain explicit
// options for workloads that can absorb their freshness cost or semantics.
func DefaultReadOptions() ReadOptions {
	return ReadOptions{
		Consistency: kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG,
		Preference:  kvrpcpb.ReadPreference_READ_PREFERENCE_LEADER_ONLY,
	}
}

func normalizeReadOptions(opts ReadOptions) ReadOptions {
	if opts.Consistency != kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE {
		opts.Consistency = kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG
	}
	if opts.Preference != kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER {
		opts.Preference = kvrpcpb.ReadPreference_READ_PREFERENCE_LEADER_ONLY
	}
	return opts
}

func strongFollowerPrefer(opts ReadOptions) bool {
	opts = normalizeReadOptions(opts)
	return opts.Consistency == kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG &&
		opts.Preference == kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER
}

func followerPrefer(opts ReadOptions) bool {
	return normalizeReadOptions(opts).Preference == kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER
}
