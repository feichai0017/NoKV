// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storecontrol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestOperationTypeString(t *testing.T) {
	tests := []struct {
		name string
		in   OperationType
		want string
	}{
		{name: "none", in: OperationNone, want: "none"},
		{name: "leader transfer", in: OperationLeaderTransfer, want: "leader-transfer"},
		{name: "unknown", in: OperationType(255), want: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Fatalf("OperationType.String()=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestStoreStatsJSONContract(t *testing.T) {
	updatedAt := time.Unix(1710000000, 123).UTC()
	in := StoreStats{
		StoreID:           7,
		ClientAddr:        "127.0.0.1:9001",
		RaftAddr:          "127.0.0.1:9101",
		RegionNum:         11,
		LeaderNum:         3,
		LeaderRegionIDs:   []uint64{2, 5, 8},
		Capacity:          1 << 30,
		Available:         1 << 29,
		DroppedOperations: 4,
		UpdatedAt:         updatedAt,
	}

	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out StoreStats
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.StoreID != in.StoreID ||
		out.ClientAddr != in.ClientAddr ||
		out.RaftAddr != in.RaftAddr ||
		out.RegionNum != in.RegionNum ||
		out.LeaderNum != in.LeaderNum ||
		out.Capacity != in.Capacity ||
		out.Available != in.Available ||
		out.DroppedOperations != in.DroppedOperations ||
		!out.UpdatedAt.Equal(in.UpdatedAt) {
		t.Fatalf("StoreStats JSON round trip mismatch: got %#v want %#v", out, in)
	}
	if len(out.LeaderRegionIDs) != len(in.LeaderRegionIDs) {
		t.Fatalf("leader region count=%d, want %d", len(out.LeaderRegionIDs), len(in.LeaderRegionIDs))
	}
	for i := range in.LeaderRegionIDs {
		if out.LeaderRegionIDs[i] != in.LeaderRegionIDs[i] {
			t.Fatalf("leader region ids=%v, want %v", out.LeaderRegionIDs, in.LeaderRegionIDs)
		}
	}
}

func TestStatusJSONContract(t *testing.T) {
	lastErrorAt := time.Unix(1710000010, 0).UTC()
	in := Status{
		Mode:              ModeDegraded,
		Degraded:          true,
		LastError:         "coordinator unavailable",
		LastErrorAt:       lastErrorAt,
		DroppedOperations: 9,
	}

	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Status
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("Status JSON round trip mismatch: got %#v want %#v", out, in)
	}
}
