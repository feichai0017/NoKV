package kv

import (
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// NewEntryApplier returns an ApplyFunc that decodes raft log entries and
// applies them to the provided DB using the MVCC helpers.
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
			return fmt.Errorf("raftstore/kv: unsupported legacy raft payload")
		}
		return nil
	}
}
