package metrics

import (
	"expvar"
	"log"
	"net"
	"net/http"
)

// StartExpvarServer starts an optional HTTP endpoint exposing /debug/vars.
// An empty address disables the server and returns nil.
func StartExpvarServer(addr string) (net.Listener, error) {
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
		if err := http.Serve(ln, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("expvar metrics server error: %v", err)
		}
	}()
	return ln, nil
}
