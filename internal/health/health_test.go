package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEndpoints(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(*State)
		wantReady int
	}{
		{name: "before the first receive", prepare: func(*State) {}, wantReady: http.StatusServiceUnavailable},
		{name: "after the first receive", prepare: func(s *State) { s.MarkReady() }, wantReady: http.StatusOK},
		{name: "after shutdown starts", prepare: func(s *State) { s.MarkReady(); s.MarkShuttingDown() }, wantReady: http.StatusServiceUnavailable},
		{name: "shutdown is sticky", prepare: func(s *State) { s.MarkShuttingDown(); s.MarkReady() }, wantReady: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &State{}
			tt.prepare(s)
			h := Handler(s)

			for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": tt.wantReady} {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
				if rec.Code != want {
					t.Errorf("%s: got %d, want %d", path, rec.Code, want)
				}
			}
		})
	}
}

func TestUnknownPath(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler(&State{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("got %d, want 404", rec.Code)
	}
}
