package command

import (
	"fmt"

	"github.com/feichai0017/NoKV/pb"
	proto "google.golang.org/protobuf/proto"
)

const (
	// PayloadPrefix marks raft log entries carrying RaftCmdRequest payloads.
	// It must not collide with the admin command prefix defined in peer.go.
	PayloadPrefix byte = 0xCE
)

// Encode serialises the provided RaftCmdRequest and prefixes it with the
// command marker so peers can differentiate it from legacy payloads.
func Encode(req *pb.RaftCmdRequest) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("raftstore: nil raft command")
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(data)+1)
	buf[0] = PayloadPrefix
	copy(buf[1:], data)
	return buf, nil
}

// Decode inspects the provided entry payload. When the command prefix is
// present it unmarshals the embedded RaftCmdRequest and returns it alongside a
// boolean indicating whether the payload contained a command.
func Decode(data []byte) (*pb.RaftCmdRequest, bool, error) {
	if len(data) == 0 || data[0] != PayloadPrefix {
		return nil, false, nil
	}
	var req pb.RaftCmdRequest
	if err := proto.Unmarshal(data[1:], &req); err != nil {
		return nil, true, err
	}
	return &req, true, nil
}
