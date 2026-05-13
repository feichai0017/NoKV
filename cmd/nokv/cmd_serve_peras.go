package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	"github.com/feichai0017/NoKV/raftstore/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
)

const perasWitnessControlWALBase uint64 = 1 << 63

type controlWALOpener interface {
	OpenControlWAL(uint64) (*wal.Manager, error)
}

func startServePerasWitness(ctx context.Context, storeID uint64, coord runtimeperas.RootAuthoritySource, db controlWALOpener, durability wal.DurabilityPolicy) (kv.PerasWitness, *runtimeperas.ActiveAuthorities, *runtimeperas.RootAuthorityFeed, error) {
	if storeID == 0 || coord == nil || db == nil {
		return nil, nil, nil, fmt.Errorf("serve: peras witness requires store id, coordinator, and db")
	}
	manager, err := db.OpenControlWAL(perasWitnessControlGroupID(storeID))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("serve: open peras witness WAL: %w", err)
	}
	log, err := rsperas.NewWALWitnessLog(manager, durability)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("serve: open peras witness log: %w", err)
	}
	authorities := runtimeperas.NewActiveAuthorities()
	feed := runtimeperas.StartRootAuthorityFeed(ctx, coord, authorities, time.Second)
	witness, err := rsperas.NewWitnessNode(rsperas.WitnessNodeConfig{
		NodeID:           fmt.Sprintf("store-%d", storeID),
		Log:              log,
		AuthorityTable:   authorities,
		AuthorityRefresh: feed.Refresh,
	})
	if err != nil {
		if feed != nil {
			_ = feed.Close()
		}
		return nil, nil, nil, err
	}
	return witness, authorities, feed, nil
}

func perasWitnessControlGroupID(storeID uint64) uint64 {
	return perasWitnessControlWALBase | storeID
}

func parsePerasWitnessDurability(value string) (wal.DurabilityPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "fsync-batched", "fsync_batched", "batched":
		return wal.DurabilityFsyncBatched, nil
	case "fsync":
		return wal.DurabilityFsync, nil
	case "flushed":
		return wal.DurabilityFlushed, nil
	case "buffered":
		return wal.DurabilityBuffered, nil
	default:
		return 0, fmt.Errorf("invalid peras witness durability %q", value)
	}
}
