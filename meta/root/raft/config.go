package rootraft

import (
	"fmt"
	"strings"
)

const (
	defaultElectionTick  = 10
	defaultHeartbeatTick = 1
	defaultMaxMsgBytes   = 1 << 20
	defaultMaxInflight   = 256
)

// Peer identifies one metadata-root raft voter.
type Peer struct {
	ID      uint64
	Address string
}

// Config configures one metadata-root raft node.
type Config struct {
	NodeID          uint64
	Peers           []Peer
	Bootstrap       bool
	ElectionTick    int
	HeartbeatTick   int
	MaxSizePerMsg   uint64
	MaxInflightMsgs int
}

func (c Config) withDefaults() (Config, error) {
	if c.NodeID == 0 {
		return Config{}, fmt.Errorf("meta/root/raft: node id is required")
	}
	if c.ElectionTick <= 0 {
		c.ElectionTick = defaultElectionTick
	}
	if c.HeartbeatTick <= 0 {
		c.HeartbeatTick = defaultHeartbeatTick
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return Config{}, fmt.Errorf("meta/root/raft: election tick must be greater than heartbeat tick")
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = defaultMaxMsgBytes
	}
	if c.MaxInflightMsgs == 0 {
		c.MaxInflightMsgs = defaultMaxInflight
	}
	if len(c.Peers) == 0 {
		c.Peers = []Peer{{ID: c.NodeID}}
	}
	seen := make(map[uint64]struct{}, len(c.Peers))
	foundSelf := false
	for _, peer := range c.Peers {
		if peer.ID == 0 {
			return Config{}, fmt.Errorf("meta/root/raft: peer id is required")
		}
		if _, ok := seen[peer.ID]; ok {
			return Config{}, fmt.Errorf("meta/root/raft: duplicate peer id %d", peer.ID)
		}
		seen[peer.ID] = struct{}{}
		if peer.ID == c.NodeID {
			foundSelf = true
		}
		_ = strings.TrimSpace(peer.Address)
	}
	if !foundSelf {
		return Config{}, fmt.Errorf("meta/root/raft: local node %d is missing from peer set", c.NodeID)
	}
	return c, nil
}
