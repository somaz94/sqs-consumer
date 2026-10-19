// Package health serves the liveness and readiness endpoints.
package health

import (
	"net/http"
	"sync/atomic"
)

// State tracks readiness. Shutting down is sticky: once set, the pod never reports ready again.
type State struct {
	ready    atomic.Bool
	stopping atomic.Bool
}

// MarkReady records that the queue has been reached at least once.
func (s *State) MarkReady() { s.ready.Store(true) }

// MarkShuttingDown makes readiness fail for the rest of the process lifetime.
func (s *State) MarkShuttingDown() { s.stopping.Store(true) }

// Ready reports whether the pod should receive traffic.
func (s *State) Ready() bool { return s.ready.Load() && !s.stopping.Load() }

// Handler returns the mux serving /healthz and /readyz.
func Handler(s *State) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.Ready() {
			write(w, http.StatusOK, "ready")
			return
		}
		write(w, http.StatusServiceUnavailable, "not ready")
	})
	return mux
}

func write(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body + "\n"))
}
