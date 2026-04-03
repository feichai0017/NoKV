package rootraft

import (
	"encoding/binary"
	"fmt"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/protobuf/proto"
)

type commandKind uint8

const (
	commandKindUnknown commandKind = iota
	commandKindEvent
	commandKindFence
)

type command struct {
	kind  commandKind
	event rootpkg.Event
	fence allocatorFence
}

type allocatorFence struct {
	kind rootpkg.AllocatorKind
	min  uint64
}

func encodeEventCommand(event rootpkg.Event) ([]byte, error) {
	payload, err := proto.Marshal(metacodec.RootEventToProto(event))
	if err != nil {
		return nil, err
	}
	out := make([]byte, 1+len(payload))
	out[0] = byte(commandKindEvent)
	copy(out[1:], payload)
	return out, nil
}

func encodeFenceCommand(kind rootpkg.AllocatorKind, min uint64) []byte {
	out := make([]byte, 10)
	out[0] = byte(commandKindFence)
	out[1] = byte(kind)
	binary.LittleEndian.PutUint64(out[2:10], min)
	return out
}

func decodeCommand(data []byte) (command, error) {
	if len(data) == 0 {
		return command{}, nil
	}
	switch commandKind(data[0]) {
	case commandKindEvent:
		var pbEvent metapb.RootEvent
		if err := proto.Unmarshal(data[1:], &pbEvent); err != nil {
			return command{}, err
		}
		return command{kind: commandKindEvent, event: metacodec.RootEventFromProto(&pbEvent)}, nil
	case commandKindFence:
		if len(data) != 10 {
			return command{}, fmt.Errorf("meta/root/raft: invalid fence command payload")
		}
		return command{kind: commandKindFence, fence: allocatorFence{kind: rootpkg.AllocatorKind(data[1]), min: binary.LittleEndian.Uint64(data[2:10])}}, nil
	default:
		return command{}, fmt.Errorf("meta/root/raft: unknown command kind %d", data[0])
	}
}
