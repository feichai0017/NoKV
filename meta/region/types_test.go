// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"encoding/json"
	"testing"
)

func TestRegionTypesJSONContract(t *testing.T) {
	peer := Peer{StoreID: 7, PeerID: 701}
	peerData, err := json.Marshal(peer)
	if err != nil {
		t.Fatal(err)
	}
	if string(peerData) != `{"store_id":7,"peer_id":701}` {
		t.Fatalf("Peer JSON=%s", peerData)
	}

	epoch := Epoch{Version: 11, ConfVersion: 3}
	epochData, err := json.Marshal(epoch)
	if err != nil {
		t.Fatal(err)
	}
	if string(epochData) != `{"version":11,"conf_version":3}` {
		t.Fatalf("Epoch JSON=%s", epochData)
	}
}

func TestReplicaStateOrdinalsStayStable(t *testing.T) {
	want := []ReplicaState{
		ReplicaStateNew,
		ReplicaStateRunning,
		ReplicaStateRemoving,
		ReplicaStateTombstone,
	}
	for i, state := range want {
		if state != ReplicaState(i) {
			t.Fatalf("ReplicaState ordinal for index %d is %d", i, state)
		}
	}
}
