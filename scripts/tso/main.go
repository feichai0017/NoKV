package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	listenAddr = flag.String("addr", "127.0.0.1:9494", "listen address for the TSO service")
	startValue = flag.Uint64("start", 1, "initial timestamp to hand out")
)

var counter uint64

type tsoResponse struct {
	Timestamp uint64 `json:"timestamp"`
	Count     uint64 `json:"count"`
}

func main() {
	flag.Parse()
	if *startValue == 0 {
		log.Fatal("start must be >= 1")
	}
	atomic.StoreUint64(&counter, *startValue-1)

	mux := http.NewServeMux()
	mux.HandleFunc("/tso", handleTSO)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("TSO service listening on %s (start=%d)", *listenAddr, *startValue)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

func handleTSO(w http.ResponseWriter, r *http.Request) {
	batch, err := parseBatchParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	latest := atomic.AddUint64(&counter, batch)
	first := latest - batch + 1

	resp := tsoResponse{
		Timestamp: first,
		Count:     batch,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("encode response: %v", err), http.StatusInternalServerError)
	}
}

func parseBatchParam(r *http.Request) (uint64, error) {
	if r == nil {
		return 1, nil
	}
	value := r.URL.Query().Get("batch")
	if value == "" {
		return 1, nil
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid batch: %w", err)
	}
	if n == 0 {
		return 0, fmt.Errorf("batch must be >= 1")
	}
	return n, nil
}
