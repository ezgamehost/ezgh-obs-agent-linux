package app

import (
	"context"
	"encoding/pem"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	exporter "github.com/ezgamehost/ezgh-obs-agent-linux/internal/export"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/spool"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDaemonFileDeliveryAndShutdown(t *testing.T) {
	d, err := os.MkdirTemp("/tmp", "agent-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	received := make(chan bool, 100)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/logs" {
			t.Errorf("path %s", r.URL.Path)
		}
		received <- true
	}))
	defer server.Close()
	ca := filepath.Join(d, "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
	key := filepath.Join(d, "key")
	os.WriteFile(key, []byte("ezgh_ik_"+strings.Repeat("a", 43)), 0600)
	log := filepath.Join(d, "app.log")
	os.WriteFile(log, []byte("hello\n"), 0600)
	c, e := config.Parse(strings.NewReader("version: 1\ntarget: test\nenvironment: test\nendpoint: " + server.URL + "\ncredential_file: " + key + "\nstate_dir: " + d + "/state\nca_file: " + ca + "\nmetrics:\n  enabled: false\nlogs:\n  journal:\n    enabled: false\n  files:\n    - paths: [" + log + "]\n      service: test\n      start_at: beginning\n"))
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, c, nil) }()
	select {
	case <-received:
	case e := <-done:
		t.Fatalf("early exit %v", e)
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery")
	}
	if _, e = diagnostics.Read(ctx, filepath.Join(c.StateDir, "status.sock")); e != nil {
		t.Fatal(e)
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow shutdown")
	}
}
func TestStateLock(t *testing.T) {
	d, err := os.MkdirTemp("/tmp", "agent-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	f, e := lockState(d)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if second, e := lockState(d); e == nil {
		second.Close()
		t.Fatal("second daemon acquired lock")
	}
}

// An API key in credential_file names its project. Queues written before project existed keep
// their binding, so upgrading with pending data still starts; changing project with pending data
// is refused like changing the target.
func TestFileCredentialModes(t *testing.T) {
	d, err := os.MkdirTemp("/tmp", "agent-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	headers := make(chan http.Header, 100)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { headers <- r.Header.Clone() }))
	defer server.Close()
	ca := filepath.Join(d, "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
	key := filepath.Join(d, "key")
	log := filepath.Join(d, "app.log")
	os.WriteFile(log, []byte("hello\n"), 0600)
	state := filepath.Join(d, "state")
	os.MkdirAll(state, 0700)
	const id = "host-1"
	doc := func(project string) config.Config {
		c, e := config.Parse(strings.NewReader("version: 1\ntarget: test\nenvironment: test\nendpoint: " + server.URL + "\ncredential_file: " + key + project + "\nhost_id: " + id + "\nstate_dir: " + state + "\nca_file: " + ca + "\nmetrics:\n  enabled: false\nlogs:\n  journal:\n    enabled: false\n  files:\n    - paths: [" + log + "]\n      service: test\n      start_at: beginning\n"))
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	seed := func(binding string) {
		q, e := spool.Open(filepath.Join(state, "logs.db"), binding, 256<<20, 0)
		if e != nil {
			t.Fatal(e)
		}
		defer q.Close()
		if e = q.Enqueue([][]byte{{}}, nil); e != nil {
			t.Fatal(e)
		}
	}
	once := func(c config.Config) {
		t.Helper()
		for len(headers) > 0 {
			<-headers // the run before
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- Run(ctx, c, nil) }()
		select {
		case h := <-headers:
			if got, want := h.Get("x-ezgh-project"), c.Project; got != want {
				t.Fatalf("x-ezgh-project %q, want %q", got, want)
			}
		case e := <-done:
			t.Fatalf("early exit: %v", e)
		case <-time.After(10 * time.Second):
			t.Fatal("no delivery")
		}
		cancel()
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}

	// A v0.1 queue with a batch pending, under an ingest key.
	seed("test|" + server.URL + "|test|" + id)
	os.WriteFile(key, []byte("ezgh_ik_"+strings.Repeat("a", 43)), 0600)
	once(doc(""))

	// An API key names its project, and pending data is bound to it.
	bound := "test|" + server.URL + "|test|" + id + "|prj_g7h8i9j0k1l2"
	seed(bound)
	os.WriteFile(key, []byte("ezgh_"+strings.Repeat("a", 40)), 0600)
	once(doc("\nproject: prj_g7h8i9j0k1l2"))
	seed(bound)
	if e := Run(context.Background(), doc("\nproject: prj_aaaaaaaaaaaa"), nil); e == nil || !strings.Contains(e.Error(), "pending") {
		t.Fatalf("moved pending data to another project: %v", e)
	}
}

// A 404 in file mode pauses the signal; a reload ends the pause and the batch goes out again.
func TestEndpointPauseEndsOnReload(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	q := queue(t, "logs", t.TempDir(), logBatch(t))
	client := exporter.NewWithClient(server.URL, server.Client(), "ezgh_key")
	state := diagnostics.New()
	var generation atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { deliver(ctx, "logs", q, client, state, &generation) })
	eventually(t, "paused", func() bool { return state.Snapshot().States["logs"] == "paused:endpoint" })
	generation.Add(1)
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if calls.Load() < 2 {
		t.Fatalf("no second send after reload, %d calls", calls.Load())
	}
}
