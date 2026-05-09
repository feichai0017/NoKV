package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC address")
		mount     = flag.String("mount", "default", "registered mount ID")
		duration  = flag.Duration("duration", 24*time.Hour, "soak duration")
		steps     = flag.Int("steps", 80, "generated namespace operations per round before external filtering")
		batch     = flag.Int("batch", 3, "concurrent history batch size")
		seedStart = flag.Int64("seed-start", 1, "first deterministic seed")
	)
	flag.Parse()
	if *duration <= 0 || *steps <= 0 || *batch <= 0 || *seedStart <= 0 {
		log.Fatalf("duration, steps, batch, and seed-start must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	mountID := fsmeta.MountID(*mount)
	deadline := time.Now().Add(*duration)
	for seed := *seedStart; time.Now().Before(deadline); seed++ {
		if err := runRound(ctx, *addr, mountID, seed, *steps, *batch); err != nil {
			log.Fatalf("soak round failed seed=%d: %v", seed, err)
		}
		log.Printf("soak round passed seed=%d remaining=%s", seed, time.Until(deadline).Round(time.Second))
	}
}

func runRound(ctx context.Context, addr string, mount fsmeta.MountID, seed int64, steps, batch int) error {
	roundCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(roundCtx, addr)
	if err != nil {
		return fmt.Errorf("dial fsmeta: %w", err)
	}
	defer func() { _ = cli.Close() }()

	model := fsmetacontract.NewModel(mount)
	unique := time.Now().UnixNano()
	scopeName := fmt.Sprintf("soak-history-%06d-%d", seed, unique)
	scopeInode := fsmeta.InodeID(8_000_000_000 + seed*1_000_000 + unique%1_000_000)
	ops := soakHistoryOps(fsmetacontract.GenerateScript(seed, steps), mount, scopeName, scopeInode)
	if len(ops) == 0 {
		return fmt.Errorf("generated no namespace operations")
	}
	if err := fsmetacontract.RunConcurrentBatches(roundCtx, cli, model, ops, batch, fsmetacontract.HistoryOptions{}); err != nil {
		return fmt.Errorf("namespace history: %w", err)
	}
	if err := runSessionProbe(roundCtx, cli, mount, seed); err != nil {
		return fmt.Errorf("session probe: %w", err)
	}
	if err := runSnapshotProbe(roundCtx, cli, mount); err != nil {
		return fmt.Errorf("snapshot probe: %w", err)
	}
	if err := runWatchProbe(roundCtx, cli, mount, seed); err != nil {
		return fmt.Errorf("watch probe: %w", err)
	}
	return nil
}

func soakHistoryOps(in []fsmetacontract.Operation, mount fsmeta.MountID, scopeName string, scopeInode fsmeta.InodeID) []fsmetacontract.Operation {
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

func runSessionProbe(ctx context.Context, cli *fsmetaclient.GRPCClient, mount fsmeta.MountID, seed int64) error {
	unique := time.Now().UnixNano()
	name := fmt.Sprintf("soak-session-%06d-%d", seed, unique)
	result, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   name,
		Attrs: fsmeta.CreateAttrs{
			Type: fsmeta.InodeTypeFile,
			Size: uint64(seed),
			Mode: 0o644,
		},
	})
	if err != nil && !errors.Is(err, fsmeta.ErrExists) {
		return err
	}
	inode := result.Inode.Inode

	session := fsmeta.SessionID(fmt.Sprintf("soak-writer-%06d-%d", seed, unique))
	if _, err := cli.OpenWriteSession(ctx, fsmeta.OpenWriteSessionRequest{
		Mount:   mount,
		Inode:   inode,
		Session: session,
		TTL:     2 * time.Minute,
	}); err != nil {
		return err
	}
	if _, err := cli.HeartbeatWriteSession(ctx, fsmeta.HeartbeatWriteSessionRequest{
		Mount:   mount,
		Inode:   inode,
		Session: session,
		TTL:     3 * time.Minute,
	}); err != nil {
		return err
	}
	if err := cli.CloseWriteSession(ctx, fsmeta.CloseWriteSessionRequest{
		Mount:   mount,
		Inode:   inode,
		Session: session,
	}); err != nil {
		return err
	}
	_, err = cli.ExpireWriteSessions(ctx, fsmeta.ExpireWriteSessionsRequest{Mount: mount, Limit: 32})
	return err
}

func runSnapshotProbe(ctx context.Context, cli *fsmetaclient.GRPCClient, mount fsmeta.MountID) error {
	token, err := cli.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{
		Mount:     mount,
		RootInode: fsmeta.RootInode,
	})
	if err != nil {
		return err
	}
	if _, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:           mount,
		Parent:          fsmeta.RootInode,
		Limit:           16,
		SnapshotVersion: token.ReadVersion,
	}); err != nil {
		return err
	}
	return cli.RetireSnapshotSubtree(ctx, token)
}

func runWatchProbe(ctx context.Context, cli *fsmetaclient.GRPCClient, mount fsmeta.MountID, seed int64) error {
	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := cli.WatchSubtree(watchCtx, fsmeta.WatchRequest{
		Mount:              mount,
		RootInode:          fsmeta.RootInode,
		BackPressureWindow: 8,
	})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	unique := time.Now().UnixNano()
	name := fmt.Sprintf("soak-watch-%06d-%d", seed, unique)
	if _, err := cli.Create(watchCtx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   name,
		Attrs: fsmeta.CreateAttrs{
			Type: fsmeta.InodeTypeFile,
			Size: 1,
			Mode: 0o644,
		},
	}); err != nil && !errors.Is(err, fsmeta.ErrExists) {
		return err
	}
	for {
		evt, err := stream.Recv()
		if err != nil {
			return err
		}
		if got, ok := fsmeta.DentryNameOfKey(evt.Key); !ok || got != name {
			_ = stream.Ack(evt.Cursor)
			continue
		}
		return stream.Ack(evt.Cursor)
	}
}
