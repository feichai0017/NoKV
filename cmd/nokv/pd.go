package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/pd/core"
	pdserver "github.com/feichai0017/NoKV/pd/server"
	"github.com/feichai0017/NoKV/pd/tso"
	"google.golang.org/grpc"
)

var pdNotifyContext = signal.NotifyContext
var pdListen = net.Listen

func runPDCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("pd", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2379", "listen address for PD-lite gRPC service")
	idStart := fs.Uint64("id-start", 1, "initial ID allocator value")
	tsStart := fs.Uint64("ts-start", 1, "initial TSO value")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	lis, err := pdListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("pd listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := core.NewCluster()
	ids := core.NewIDAllocator(*idStart)
	tsAlloc := tso.NewAllocator(*tsStart)
	svc := pdserver.NewService(cluster, ids, tsAlloc)

	grpcServer := grpc.NewServer()
	pb.RegisterPDServer(grpcServer, svc)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()

	_, _ = fmt.Fprintf(w, "PD-lite service listening on %s\n", lis.Addr().String())
	ctx, cancel := pdNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	select {
	case serveErr := <-serveErrCh:
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	case <-ctx.Done():
		grpcServer.GracefulStop()
		serveErr := <-serveErrCh
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	}
}
