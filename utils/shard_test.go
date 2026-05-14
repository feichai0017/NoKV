// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "testing"

func TestShardForUserKey(t *testing.T) {
	tests := []struct {
		name   string
		key    []byte
		shards int
		want   int
	}{
		{name: "empty key", key: nil, shards: 4, want: 0},
		{name: "single shard", key: []byte("alpha"), shards: 1, want: 0},
		{name: "non power of two normalizes down", key: []byte("alpha"), shards: 6, want: 3},
		{name: "stable hash", key: []byte("fsmeta:dentry"), shards: 8, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShardForUserKey(tt.key, tt.shards); got != tt.want {
				t.Fatalf("ShardForUserKey() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNormalizeShardCount(t *testing.T) {
	tests := map[int]int{
		-1: 1,
		0:  1,
		1:  1,
		2:  2,
		3:  2,
		4:  4,
		7:  4,
		8:  8,
	}
	for input, want := range tests {
		if got := NormalizeShardCount(input); got != want {
			t.Fatalf("NormalizeShardCount(%d) = %d, want %d", input, got, want)
		}
	}
}
