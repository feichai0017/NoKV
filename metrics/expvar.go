package metrics

import (
	"expvar"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
)

// StartExpvarServer starts an optional HTTP endpoint exposing /debug/vars and
// /debug/pprof. The pprof surface is intentionally tied to the same opt-in
// diagnostics listener as expvar so production deployments can keep both
// disabled by leaving --metrics-addr empty.
// An empty address disables the server and returns nil.
//
// CORS is permissive so a browser-based dashboard served from a different
// local port (e.g. a python http.server on :8080 rendering a static HTML
// that polls this endpoint) can fetch the JSON. The payload is read-only
// diagnostic state and already treats any caller as untrusted — there is no
// authentication here, so "anyone who can reach this port can read it" is
// the existing threat model. Do not publish this port to the public
// internet without an upstream gateway / proxy.
func StartExpvarServer(addr string) (net.Listener, error) {
	if addr == "" {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", corsReadOnly(expvar.Handler()))
	registerPprofHandlers(mux)
	go func() {
		if err := http.Serve(ln, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("expvar metrics server error: %v", err)
		}
	}()
	return ln, nil
}

func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", corsReadOnlyFunc(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", corsReadOnlyFunc(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", corsReadOnlyFunc(pprof.Profile))
	mux.HandleFunc("/debug/pprof/symbol", corsReadOnlyFunc(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", corsReadOnlyFunc(pprof.Trace))
	for _, name := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
		mux.Handle("/debug/pprof/"+name, corsReadOnly(pprof.Handler(name)))
	}
}

// corsReadOnly allows cross-origin GETs. Writes are not exposed on this
// surface, so wildcard Origin is safe for dashboards / Grafana panels.
func corsReadOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func corsReadOnlyFunc(next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return corsReadOnly(http.HandlerFunc(next)).ServeHTTP
}
