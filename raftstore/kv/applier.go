package kv

import (
	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/utils"
	proto "google.golang.org/protobuf/proto"
)

// NewEntryApplier returns an ApplyFunc that decodes raft log entries and
// applies them to the provided DB using the MVCC helpers. Legacy KV payloads
// (without raft commands) are still honoured for compatibility with earlier
// tests.
func NewEntryApplier(db *NoKV.DB) peer.ApplyFunc {
	return func(entries []myraft.Entry) error {
		for _, entry := range entries {
			if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			req, ok, err := command.Decode(entry.Data)
			if err != nil {
				return err
			}
			if ok {
				if _, err := Apply(db, req); err != nil {
					return err
				}
				continue
			}
			var legacy pb.KV
			if err := proto.Unmarshal(entry.Data, &legacy); err != nil {
				return err
			}
			if len(legacy.GetValue()) == 0 {
				if err := db.DelCF(utils.CFDefault, legacy.GetKey()); err != nil {
					return err
				}
				continue
			}
			if err := db.SetCF(utils.CFDefault, legacy.GetKey(), legacy.GetValue()); err != nil {
				return err
			}
		}
		return nil
	}
}
