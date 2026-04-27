package slab

import "testing"

// Document the consumer classes the slab substrate currently has, so a
// future consumer that picks the wrong class causes a test failure
// rather than silently corrupting the recovery story.
func TestConsumerClassRoster(t *testing.T) {
	cases := []struct {
		consumer string
		class    ConsumerClass
		why      string
	}{
		{
			consumer: "engine/vlog",
			class:    ConsumerClassAuthoritative,
			why:      "value-log segments hold KV-separated values referenced by LSM ValuePtrs; corruption is data loss",
		},
		{
			consumer: "engine/slab/negativecache",
			class:    ConsumerClassDerived,
			why:      "negative-result cache built from LSM lookups; total loss only re-warms via Get misses",
		},
		{
			consumer: "engine/slab/dirpage",
			class:    ConsumerClassDerived,
			why:      "directory-page cache built from LSM dentry scans; total loss only forces a re-scan",
		},
	}
	seenAuthoritative := false
	seenDerived := false
	for _, tc := range cases {
		if tc.class == ConsumerClassUnspecified {
			t.Errorf("consumer %s left class unspecified — must be Authoritative / Lifecycle-bound / Derived", tc.consumer)
		}
		switch tc.class {
		case ConsumerClassAuthoritative:
			seenAuthoritative = true
		case ConsumerClassDerived:
			seenDerived = true
		}
	}
	if !seenAuthoritative || !seenDerived {
		t.Fatalf("consumer roster must cover at least one Authoritative + one Derived consumer; got auth=%v derived=%v",
			seenAuthoritative, seenDerived)
	}
}
