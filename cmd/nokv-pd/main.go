package main

import (
	"flag"
	"log"
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

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:2379", "listen address for PD-lite gRPC service")
		idStart = flag.Uint64("id-start", 1, "initial ID allocator value")
		tsStart = flag.Uint64("ts-start", 1, "initial TSO value")
	)
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := core.NewCluster()
	ids := core.NewIDAllocator(*idStart)
	tsAlloc := tso.NewAllocator(*tsStart)
	svc := pdserver.NewService(cluster, ids, tsAlloc)

	grpcServer := grpc.NewServer()
	pb.RegisterPDServer(grpcServer, svc)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(lis)
	}()

	log.Printf("PD-lite listening on %s", *addr)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
	case serveErr := <-errCh:
		if serveErr != nil {
			log.Printf("grpc serve exited with error: %v", serveErr)
		}
	}
	grpcServer.GracefulStop()
}
