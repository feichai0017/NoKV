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
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
		mode    = flag.String("mode", "watch", "demo mode: watch|snapshot")
		mount   = flag.String("mount", "demo", "fsmeta mount id")
		files   = flag.Int("files", 16, "number of files for watch mode")
		timeout = flag.Duration("timeout", 15*time.Second, "demo timeout")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(ctx, *addr)
	if err != nil {
		log.Fatalf("dial fsmeta: %v", err)
	}
	defer func() { _ = cli.Close() }()

	switch *mode {
	case "watch":
		if err := runWatchDemo(ctx, cli, fsmeta.MountID(*mount), *files); err != nil {
			log.Fatalf("watch demo: %v", err)
		}
	case "snapshot":
		if err := runSnapshotDemo(ctx, cli, fsmeta.MountID(*mount)); err != nil {
			log.Fatalf("snapshot demo: %v", err)
		}
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

func runWatchDemo(ctx context.Context, cli fsmetaclient.Client, mount fsmeta.MountID, files int) error {
	prefix, err := fsmeta.EncodeDentryPrefix(mount, fsmeta.RootInode)
	if err != nil {
		return err
	}
	stream, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: uint32(max(files, 1)),
	})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	events := make(chan fsmeta.WatchEvent, files)
	errs := make(chan error, 1)
	go func() {
		for range files {
			evt, err := stream.Recv()
			if err != nil {
				errs <- err
				return
			}
			if err := stream.Ack(evt.Cursor); err != nil {
				errs <- err
				return
			}
			events <- evt
		}
		errs <- nil
	}()

	start := time.Now()
	for i := range files {
		name := fmt.Sprintf("watch-%06d", i)
		inode := fsmeta.InodeID(10_000 + i)
		if err := cli.Create(ctx, fsmeta.CreateRequest{
			Mount:  mount,
			Parent: fsmeta.RootInode,
			Name:   name,
			Inode:  inode,
		}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}); err != nil {
			return err
		}
	}
	if err := <-errs; err != nil {
		return err
	}
	elapsed := time.Since(start)
	fmt.Printf("watch demo: files=%d events=%d elapsed=%s\n", files, len(events), elapsed)
	return nil
}

func runSnapshotDemo(ctx context.Context, cli fsmetaclient.Client, mount fsmeta.MountID) error {
	if err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "before-snapshot",
		Inode:  20_001,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}); err != nil && !errors.Is(err, fsmeta.ErrExists) {
		return err
	}
	token, err := cli.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{
		Mount:     mount,
		RootInode: fsmeta.RootInode,
	})
	if err != nil {
		return err
	}
	if err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "after-snapshot",
		Inode:  20_002,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}); err != nil && !errors.Is(err, fsmeta.ErrExists) {
		return err
	}
	snapshot, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:           mount,
		Parent:          fsmeta.RootInode,
		Limit:           32,
		SnapshotVersion: token.ReadVersion,
	})
	if err != nil {
		return err
	}
	latest, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Limit:  32,
	})
	if err != nil {
		return err
	}
	fmt.Printf("snapshot demo: read_version=%d snapshot_entries=%d latest_entries=%d\n", token.ReadVersion, len(snapshot), len(latest))
	return nil
}
