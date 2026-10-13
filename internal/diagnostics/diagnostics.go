// Package diagnostics serves bounded local health independently of collectors and exports.
package diagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

type Snapshot struct {
	Started  time.Time          `json:"started"`
	Counters map[string]uint64  `json:"counters"`
	Gauges   map[string]float64 `json:"gauges"`
	States   map[string]string  `json:"states"`
}
type State struct {
	mu sync.Mutex
	v  Snapshot
}

func New() *State {
	return &State{v: Snapshot{Started: time.Now(), Counters: map[string]uint64{}, Gauges: map[string]float64{}, States: map[string]string{}}}
}
func (s *State) Add(k string, n uint64)    { s.mu.Lock(); defer s.mu.Unlock(); s.v.Counters[k] += n }
func (s *State) Gauge(k string, n float64) { s.mu.Lock(); defer s.mu.Unlock(); s.v.Gauges[k] = n }
func (s *State) Set(k, v string)           { s.mu.Lock(); defer s.mu.Unlock(); s.v.States[k] = v }
func (s *State) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{s.v.Started, maps.Clone(s.v.Counters), maps.Clone(s.v.Gauges), maps.Clone(s.v.States)}
}
func (s *State) Serve(path string) (func(), error) {
	if st, e := os.Lstat(path); e == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("status path is not a socket")
		}
		if e = os.Remove(path); e != nil {
			return nil, e
		}
	}
	l, e := net.Listen("unix", path)
	if e != nil {
		return nil, e
	}
	if e = os.Chmod(path, 0600); e != nil {
		l.Close()
		return nil, e
	}
	srv := &http.Server{ReadHeaderTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 4096, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Snapshot())
	})}
	go srv.Serve(l)
	return func() { srv.Close(); os.Remove(path) }, nil
}
func Read(ctx context.Context, path string) (Snapshot, error) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	defer tr.CloseIdleConnections()
	c := http.Client{Transport: tr, Timeout: 2 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://localhost/status", nil)
	r, e := c.Do(req)
	if e != nil {
		return Snapshot{}, errors.New("daemon status unavailable")
	}
	defer r.Body.Close()
	var s Snapshot
	e = json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&s)
	return s, e
}
