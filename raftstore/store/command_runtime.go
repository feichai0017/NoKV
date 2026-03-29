package store

import (
	"context"
	"time"

	"github.com/feichai0017/NoKV/pb"
)

type commandRuntime struct {
	apply   func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	pipe    *commandPipeline
	timeout time.Duration
}

func (s *Store) commandPipe() *commandPipeline {
	if s == nil || s.cmds == nil {
		return nil
	}
	return s.cmds.pipe
}

func (s *Store) commandApply() func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
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
