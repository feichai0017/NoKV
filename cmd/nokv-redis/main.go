package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	NoKV "github.com/feichai0017/NoKV"
)

var exit = os.Exit

var fatalf = func(format string, args ...any) {
	log.Printf(format, args...)
	exit(1)
}

var newDefaultOptions = NoKV.NewDefaultOptions

var listen = net.Listen
var signalNotify = signal.Notify

func main() {
	var (
		workDir     = flag.String("workdir", "./work_redis", "database work directory")
		addr        = flag.String("addr", "127.0.0.1:6380", "listen address for RESP server")
		metricsAddr = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
		raftConfig  = flag.String("raft-config", "", "optional JSON config describing raftstore cluster endpoints")
		coordAddr   = flag.String("coordinator-addr", "", "optional coordinator gRPC endpoint override in raft mode; defaults to config.coordinator (e.g. 127.0.0.1:2379)")
		addrScope   = flag.String("addr-scope", "host", "store address scope to use (host|docker)")
	)
	flag.Parse()

	opt := newDefaultOptions()
	opt.WorkDir = *workDir

	var (
		db      *NoKV.DB
		backend redisBackend
	)
	backendCtx, cancelBackend := context.WithCancel(context.Background())
	defer cancelBackend()

	if *raftConfig != "" {
		var err error
		backend, err = newRaftBackend(backendCtx, *raftConfig, *coordAddr, *addrScope)
		if err != nil {
			fatalf("raft backend init: %v", err)
		}
	} else {
		var err error
		db, err = NoKV.Open(opt)
		if err != nil {
			fatalf("open embedded db: %v", err)
		}
		backend = newEmbeddedBackend(db)
	}
	defer func() {
		if backend != nil {
			_ = backend.Close()
		}
		if db != nil {
			if err := db.Close(); err != nil {
				log.Printf("close db: %v", err)
			}
		}
	}()

	ln, err := listen("tcp", *addr)
	if err != nil {
		fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	server := newServer(backend)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ln)
	}()

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/debug/vars", expvar.Handler())
		go func() {
			log.Printf("expvar metrics listening on http://%s/debug/vars", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				log.Printf("metrics server error: %v", err)
			}
		}()
	}

	log.Printf("NoKV Redis gateway listening on %s (workdir=%s)", *addr, *workDir)

	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
	case err := <-errCh:
		if err != nil {
			log.Printf("serve loop exited: %v", err)
		}
	}
	cancelBackend()

	if err := ln.Close(); err != nil {
		log.Printf("listener close: %v", err)
	}
	server.Wait()
	fmt.Println("bye")
}
