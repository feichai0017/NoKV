package store

import (
	"context"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"time"
)

type commandRuntime struct {
	apply   func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)
	pipe    *commandPipeline
	timeout time.Duration
}

func (s *Store) commandPipe() *commandPipeline {
	if s == nil || s.cmds == nil {
		return nil
	}
	return s.cmds.pipe
}

func (s *Store) commandApply() func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if s == nil || s.cmds == nil {
		return nil
	}
	return s.cmds.apply
}

func (s *Store) commandWait() time.Duration {
	if s == nil || s.cmds == nil {
		return 0
	}
	return s.cmds.timeout
}

func (s *Store) runtimeContext() context.Context {
	if s == nil {
		return context.Background()
	}
	return s.ctx
}
