package replicated

import (
	"encoding/binary"
	"fmt"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/protobuf/proto"
)

func marshalCommittedEvent(rec rootstorage.CommittedEvent) ([]byte, error) {
	event, err := proto.Marshal(metacodec.RootEventToProto(rec.Event))
	if err != nil {
		return nil, err
	}
	out := make([]byte, 16+len(event))
	binary.LittleEndian.PutUint64(out[0:8], rec.Cursor.Term)
	binary.LittleEndian.PutUint64(out[8:16], rec.Cursor.Index)
	copy(out[16:], event)
	return out, nil
}

func unmarshalCommittedEvent(data []byte) (rootstorage.CommittedEvent, error) {
	if len(data) < 16 {
		return rootstorage.CommittedEvent{}, fmt.Errorf("meta/root/backend/replicated: invalid committed entry payload")
	}
	var pbEvent metapb.RootEvent
	if err := proto.Unmarshal(data[16:], &pbEvent); err != nil {
		return rootstorage.CommittedEvent{}, err
	}
	return rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{
			Term:  binary.LittleEndian.Uint64(data[0:8]),
			Index: binary.LittleEndian.Uint64(data[8:16]),
		},
		Event: metacodec.RootEventFromProto(&pbEvent),
	}, nil
}
