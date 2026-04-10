package store

import (
	"fmt"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"time"
)

type commandRuntime struct {
	apply   func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	pipe    *commandPipeline
	timeout time.Duration
}

func (s *Store) applyEntries(entries []myraft.Entry) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if s.cmds == nil || s.cmds.pipe == nil {
		return fmt.Errorf("raftstore: command apply without handler")
	}
	return s.cmds.pipe.applyEntries(entries)
}
