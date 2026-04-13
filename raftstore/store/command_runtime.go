package store

import (
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
		return errNilStore
	}
	if s.cmds == nil || s.cmds.pipe == nil {
		return errCommandApplyWithoutHandler
	}
	return s.cmds.pipe.applyEntries(entries)
}
