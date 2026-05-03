package raftlog

import (
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// DBLog adapts the embedded DB's replicated control-log WAL shards into
// raftstore peer storage.
type DBLog struct {
	db *NoKV.DB
}

func NewDBLog(db *NoKV.DB) DBLog {
	return DBLog{db: db}
}

func (l DBLog) Open(groupID uint64, meta *localmeta.Store) (PeerStorage, error) {
	if l.db == nil {
		return nil, fmt.Errorf("raftstore/raftlog: db is required")
	}
	walMgr, err := l.db.OpenControlWAL(groupID)
	if err != nil {
		return nil, err
	}
	return OpenWALStorage(WALStorageConfig{
		GroupID:   groupID,
		WAL:       walMgr,
		LocalMeta: meta,
	})
}
