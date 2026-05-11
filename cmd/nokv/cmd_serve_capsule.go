package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	capsuleauth "github.com/feichai0017/NoKV/fsmeta/runtime/capsuleauth"
	rscapsule "github.com/feichai0017/NoKV/raftstore/capsule"
	"github.com/feichai0017/NoKV/raftstore/kv"
)

const capsuleWitnessControlWALBase uint64 = 1 << 63

type controlWALOpener interface {
	OpenControlWAL(uint64) (*wal.Manager, error)
}

func startServeCapsuleWitness(ctx context.Context, storeID uint64, coord capsuleauth.MirrorSource, db controlWALOpener, durability wal.DurabilityPolicy) (kv.CapsuleWitness, *capsuleauth.Mirror, error) {
	if storeID == 0 || coord == nil || db == nil {
		return nil, nil, fmt.Errorf("serve: capsule witness requires store id, coordinator, and db")
	}
	manager, err := db.OpenControlWAL(capsuleWitnessControlGroupID(storeID))
	if err != nil {
		return nil, nil, fmt.Errorf("serve: open capsule witness WAL: %w", err)
	}
	log, err := fscapsule.NewWALWitnessLog(manager, durability)
	if err != nil {
		return nil, nil, fmt.Errorf("serve: open capsule witness log: %w", err)
	}
	authorities := capsuleauth.NewActiveAuthorities()
	mirror := capsuleauth.StartMirror(ctx, coord, authorities, time.Second)
	witness, err := rscapsule.NewWitnessNode(rscapsule.WitnessNodeConfig{
		NodeID:      fmt.Sprintf("store-%d", storeID),
		Log:         log,
		Authorities: authorities,
	})
	if err != nil {
		if mirror != nil {
			_ = mirror.Close()
		}
		return nil, nil, err
	}
	return witness, mirror, nil
}

func capsuleWitnessControlGroupID(storeID uint64) uint64 {
	return capsuleWitnessControlWALBase | storeID
}

func parseCapsuleWitnessDurability(value string) (wal.DurabilityPolicy, error) {
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
		return 0, fmt.Errorf("invalid capsule witness durability %q", value)
	}
}
