package replicated

import (
	"encoding/binary"
	"fmt"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/protobuf/proto"
)

func marshalCommittedEvent(rec rootstorage.CommittedEvent) ([]byte, error) {
	event, err := proto.Marshal(metawire.RootEventToProto(rec.Event))
	if err != nil {
		return nil, err
	}
	const headerSize = 16
	maxInt := int(^uint(0) >> 1)
	if len(event) > maxInt-headerSize {
		return nil, fmt.Errorf("meta/root/backend/replicated: committed event payload too large")
	}
	out := make([]byte, headerSize+len(event))
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
		Event: metawire.RootEventFromProto(&pbEvent),
	}, nil
}
