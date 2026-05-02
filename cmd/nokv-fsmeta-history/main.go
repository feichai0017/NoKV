package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC address")
		mount   = flag.String("mount", "default", "registered mount ID")
		seeds   = flag.Int("seeds", 1, "number of deterministic seeds to run")
		start   = flag.Int64("seed-start", 1, "first deterministic seed")
		steps   = flag.Int("steps", 64, "generated operations per seed before external filtering")
		batch   = flag.Int("batch", 3, "concurrent history batch size")
		timeout = flag.Duration("timeout", 60*time.Second, "overall command timeout")
		scope   = flag.String("scope-prefix", "history", "unique root directory prefix for isolating each generated history")
	)
	flag.Parse()
	if *seeds <= 0 || *start <= 0 || *steps <= 0 {
		log.Fatalf("seeds, seed-start, and steps must be positive")
	}
	if *batch <= 0 {
		log.Fatalf("batch must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(ctx, *addr)
	if err != nil {
		log.Fatalf("dial fsmeta %s: %v", *addr, err)
	}
	defer func() { _ = cli.Close() }()

	mountID := fsmeta.MountID(*mount)
	for seed := *start; seed < *start+int64(*seeds); seed++ {
		model := fsmetacontract.NewModel(mountID)
		unique := time.Now().UnixNano()
		scopeName := fmt.Sprintf("%s-%06d-%d", *scope, seed, unique)
		scopeInode := fsmeta.InodeID(9_000_000_000 + seed*1_000_000 + unique%1_000_000)
		ops := externalHistoryOps(fsmetacontract.GenerateScript(seed, *steps), mountID, scopeName, scopeInode)
		if len(ops) == 0 {
			log.Fatalf("seed %d generated no external-safe operations", seed)
		}
		if err := fsmetacontract.RunConcurrentBatches(ctx, cli, model, ops, *batch); err != nil {
			fmt.Fprintf(os.Stderr, "fsmeta history failed seed=%d steps=%d filtered_ops=%d\n", seed, *steps, len(ops))
			log.Fatal(err)
		}
		log.Printf("fsmeta history passed seed=%d filtered_ops=%d", seed, len(ops))
	}
}

func externalHistoryOps(in []fsmetacontract.Operation, mount fsmeta.MountID, scopeName string, scopeInode fsmeta.InodeID) []fsmetacontract.Operation {
	out := make([]fsmetacontract.Operation, 0, len(in)+1)
	out = append(out, fsmetacontract.Operation{
		Kind:   fsmetacontract.OpCreate,
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   scopeName,
		Inode:  scopeInode,
		Type:   fsmeta.InodeTypeDirectory,
		Mode:   0o755,
	})
	for _, op := range in {
		switch op.Kind {
		case fsmetacontract.OpOpenWriteSession,
			fsmetacontract.OpHeartbeatSession,
			fsmetacontract.OpCloseSession,
			fsmetacontract.OpExpireSessions,
			fsmetacontract.OpAdvanceTime:
			continue
		default:
			op.Mount = mount
			if op.Parent == fsmeta.RootInode {
				op.Parent = scopeInode
			}
			if op.FromParent == fsmeta.RootInode {
				op.FromParent = scopeInode
			}
			if op.ToParent == fsmeta.RootInode {
				op.ToParent = scopeInode
			}
			out = append(out, op)
		}
	}
	return out
}
