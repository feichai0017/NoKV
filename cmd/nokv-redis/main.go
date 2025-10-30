package main

import (
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

func main() {
	var (
		workDir     = flag.String("workdir", "./work_redis", "database work directory")
		addr        = flag.String("addr", "127.0.0.1:6380", "listen address for RESP server")
		metricsAddr = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
		raftConfig  = flag.String("raft-config", "", "optional JSON config describing raftstore cluster endpoints")
		tsoURL      = flag.String("tso-url", "", "optional HTTP endpoint for external TSO (e.g. http://127.0.0.1:9494)")
	)
	flag.Parse()

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = *workDir
	if opt.MaxBatchCount <= 0 {
		opt.MaxBatchCount = int64(opt.WriteBatchMaxCount)
		if opt.MaxBatchCount <= 0 {
			opt.MaxBatchCount = 1024
		}
	}
	if opt.MaxBatchSize <= 0 {
		opt.MaxBatchSize = opt.WriteBatchMaxSize
		if opt.MaxBatchSize <= 0 {
			opt.MaxBatchSize = 16 << 20
		}
	}

	var (
		db      *NoKV.DB
		backend redisBackend
	)

	if *raftConfig != "" {
		var err error
		backend, err = newRaftBackend(*raftConfig, *tsoURL)
		if err != nil {
			log.Fatalf("raft backend init: %v", err)
		}
	} else {
		db = NoKV.Open(opt)
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

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer ln.Close()

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
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
	case err := <-errCh:
		if err != nil {
			log.Printf("serve loop exited: %v", err)
		}
	}

	if err := ln.Close(); err != nil {
		log.Printf("listener close: %v", err)
	}
	server.Wait()
	fmt.Println("bye")
}
