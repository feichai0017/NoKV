package main

import (
	"errors"
	"expvar"
	"log"
	"net"
	"net/http"
	"strings"
)

// startExpvarServer starts an optional HTTP endpoint exposing /debug/vars.
// It returns nil,nil when addr is empty.
func startExpvarServer(addr string) (net.Listener, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	go func() {
		if err := http.Serve(ln, mux); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("expvar metrics server error: %v", err)
		}
	}()
	return ln, nil
}
