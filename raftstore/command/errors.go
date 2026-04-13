package command

import (
	"errors"
	"fmt"
)

var (
	errNilRaftCommand          = errors.New("raftstore: nil raft command")
	errRaftCommandSizeOverflow = errors.New("raftstore: raft command size causes overflow")
)

func IsNilRaftCommand(err error) bool {
	return errors.Is(err, errNilRaftCommand)
}

func errRaftCommandTooLarge(size int) error {
	return fmt.Errorf("raftstore: raft command too large (%d bytes)", size)
}
