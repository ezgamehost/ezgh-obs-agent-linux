package app

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/credential"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	exporter "github.com/ezgamehost/ezgh-obs-agent-linux/internal/export"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/spool"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/telemetry"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

const (
	testInstance = "ins_a1b2c3d4e5f6"
	testProject  = "prj_g7h8i9j0k1l2"
)

// instance is a fake metadata service on a fake clock, with an ingest endpoint beside it. Each key
// fetch mints a new key, as the real service does.
type instance struct {
	t              *testing.T
	dir, log       string
	config         config.Config
	metadata       *httptest.Server
	ingest         *httptest.Server
	clock          atomic.Int64
	noBot, down    atomic.Bool
	credsDown      atomic.Bool // only /ezgh/v1/credentials is failing
	project        atomic.Value
	minted         atomic.Int32
	ingestStatus   atomic.Int32 // by default 200
	mu             sync.Mutex
	authorizations []string
	bodies         [][]byte
	headers        []http.Header
}

func newInstance(t *testing.T) *instance {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "agent-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	in := &instance{t: t, dir: d, log: filepath.Join(d, "app.log")}
	in.clock.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	in.project.Store(testProject)
	in.metadata = httptest.NewServer(http.HandlerFunc(in.serveMetadata))
	t.Cleanup(in.metadata.Close)
	in.ingest = httptest.NewTLSServer(http.HandlerFunc(in.serveIngest))
	t.Cleanup(in.ingest.Close)
	ca := filepath.Join(d, "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: in.ingest.Certificate().Raw}), 0600)
	os.WriteFile(in.log, nil, 0600)
	in.config, err = config.Parse(strings.NewReader("version: 1\ntarget: test\nenvironment: test\nendpoint: " + in.ingest.URL + "\ncredential: metadata\nstate_dir: " + d + "/state\nca_file: " + ca + "\nmetrics:\n  enabled: false\nlogs:\n  journal:\n    enabled: false\n  files:\n    - paths: [" + in.log + "]\n      service: test\n      start_at: beginning\n"))
	if err != nil {
		t.Fatal(err)
	}
	old := keyPoll
	keyPoll = 10 * time.Millisecond
	oldDrain := drainWindow
	drainWindow = 500 * time.Millisecond
	t.Cleanup(func() { keyPoll, drainWindow = old, oldDrain })
	return in
}

func (in *instance) now() time.Time { return time.Unix(0, in.clock.Load()) }

// advance moves the fake clock, so the provider refreshes its key or asks about the Bot again.
func (in *instance) advance(d time.Duration) { in.clock.Add(int64(d)) }

func (in *instance) serveMetadata(w http.ResponseWriter, r *http.Request) {
	switch {
	case in.down.Load():
		w.WriteHeader(http.StatusServiceUnavailable)
	case r.URL.Path == "/ezgh/v1/instance":
		json.NewEncoder(w).Encode(map[string]string{"id": testInstance, "project": in.project.Load().(string)})
	case r.URL.Path == "/ezgh/v1/token":
		io.WriteString(w, "tok")
	case r.URL.Path == "/ezgh/v1/credentials" && in.credsDown.Load():
		w.WriteHeader(http.StatusServiceUnavailable)
	case r.URL.Path == "/ezgh/v1/credentials" && in.noBot.Load():
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":{"code":"no_bot"}}`)
	case r.URL.Path == "/ezgh/v1/credentials":
		n := in.minted.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"apiKey": fmt.Sprintf("ezgh_%040d", n), "expiresAt": in.now().Add(10 * time.Minute), "bot": "bot_x"})
	default:
		http.NotFound(w, r)
	}
}

func (in *instance) serveIngest(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	in.mu.Lock()
	in.authorizations = append(in.authorizations, r.Header.Get("Authorization"))
	in.bodies = append(in.bodies, b)
	in.headers = append(in.headers, r.Header.Clone())
	in.mu.Unlock()
	if s := in.ingestStatus.Load(); s != 0 {
		w.WriteHeader(int(s))
	}
}

func (in *instance) requests() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return len(in.bodies)
}

// run starts the agent and returns a function that stops it and waits for it to go.
func (in *instance) run() func() {
	in.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	m := &credential.Metadata{Endpoint: in.metadata.URL, ProductNamePath: filepath.Join(in.dir, "product_name"), Now: in.now}
	os.WriteFile(m.ProductNamePath, []byte("EZGH Compute\n"), 0600)
	go func() { done <- run(ctx, in.config, nil, m) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case e := <-done:
				if e != nil {
					in.t.Errorf("run: %v", e)
				}
			case <-time.After(25 * time.Second):
				in.t.Error("slow shutdown")
			}
		})
	}
	in.t.Cleanup(stop)
	return stop
}

func (in *instance) status() diagnostics.Snapshot {
	s, _ := diagnostics.Read(context.Background(), filepath.Join(in.config.StateDir, "status.sock"))
	return s
}

func (in *instance) logLine(s string) {
	f, err := os.OpenFile(in.log, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		in.t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintln(f, s)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (in *instance) stateFiles() []string {
	entries, _ := os.ReadDir(in.config.StateDir)
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestRunRefusesOffCompute(t *testing.T) {
	in := newInstance(t)
	m := &credential.Metadata{Endpoint: in.metadata.URL, ProductNamePath: filepath.Join(in.dir, "missing")}
	if e := run(context.Background(), in.config, nil, m); e == nil || !strings.Contains(e.Error(), "EZGH Compute") {
		t.Fatalf("err = %v", e)
	}
}

func TestMetadataUnansweredDoesNotExit(t *testing.T) {
	in := newInstance(t)
	in.logLine("written before the first key")
	in.down.Store(true)
	stop := in.run()
	eventually(t, "the waiting state", func() bool { return in.status().States["credential"] == "waiting:metadata" })
	time.Sleep(50 * time.Millisecond)
	if in.requests() != 0 {
		t.Fatal("sent without a key")
	}
	in.down.Store(false)
	eventually(t, "the first delivery", func() bool { return in.requests() > 0 })
	stop()
	in.mu.Lock()
	defer in.mu.Unlock()
	if h := in.headers[0]; h.Get("x-ezgh-project") != testProject || !strings.HasPrefix(h.Get("Authorization"), "Bearer ezgh_") {
		t.Fatalf("%v", h)
	}
	for _, want := range []string{"host.id", "service.instance.id", testInstance} {
		if !strings.Contains(string(in.bodies[0]), want) {
			t.Fatalf("batch lacks %s", want)
		}
	}
}

func TestNoBotCollectsNothing(t *testing.T) {
	in := newInstance(t)
	in.logLine("before the Bot")
	in.noBot.Store(true)
	stop := in.run()
	eventually(t, "the no-Bot state", func() bool { return in.status().States["credential"] == "no_bot" })
	time.Sleep(50 * time.Millisecond)
	if got := in.stateFiles(); !slices.Equal(got, []string{"agent.lock", "status.sock"}) || in.requests() != 0 {
		t.Fatalf("state %v, %d requests", got, in.requests())
	}

	// Attaching a Bot is enough, within the retry.
	in.noBot.Store(false)
	in.advance(credential.NoBotRetry)
	eventually(t, "the first delivery", func() bool { return in.requests() > 0 })
	stop()
	if _, err := os.Stat(filepath.Join(in.config.StateDir, testInstance, "logs.db")); err != nil {
		t.Fatal(err)
	}
}

func TestOlderProjectStaysIdle(t *testing.T) {
	in := newInstance(t)
	in.project.Store("2b5f2a64-1f6b-4d7e-9b53-3a1f8a8c5b11")
	in.run()
	eventually(t, "the idle state", func() bool { return in.status().States["credential"] == "idle:project_not_prefixed" })
	if got := in.stateFiles(); !slices.Equal(got, []string{"agent.lock", "status.sock"}) {
		t.Fatalf("state %v", got)
	}
}

// A 401 on a key that just rotated out keeps the batch: the new key delivers it.
func TestRotationKeepsBatchRefusedWith401(t *testing.T) {
	in := newInstance(t)
	in.ingestStatus.Store(401)
	in.logLine("kept")
	stop := in.run()
	eventually(t, "the refusal", func() bool { return in.requests() > 0 })
	eventually(t, "the pause", func() bool { return in.status().States["logs"] == "paused:credential" })
	in.ingestStatus.Store(200)
	in.advance(6 * time.Minute)
	eventually(t, "the delivery with the next key", func() bool { return in.status().Counters["logs_exported"] > 0 })
	if s := in.status().Counters["logs_rejected"]; s != 0 {
		t.Fatalf("%d rejected", s)
	}
	stop()
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.bodies) < 2 || string(in.bodies[0]) != string(in.bodies[len(in.bodies)-1]) || in.authorizations[0] == in.authorizations[len(in.authorizations)-1] {
		t.Fatalf("%d requests: the batch wasn't resent with a new key", len(in.bodies))
	}
}

func TestMetadataOutageSpools(t *testing.T) {
	in := newInstance(t)
	in.logLine("one")
	in.run()
	eventually(t, "the first delivery", func() bool { return in.requests() == 1 })

	// The key expires with no answer from the service: nothing is sent and nothing is dropped.
	in.down.Store(true)
	in.advance(11 * time.Minute)
	eventually(t, "the outage state", func() bool { return in.status().States["credential"] == "waiting:metadata" })
	in.logLine("two")
	eventually(t, "the batch in the spool", func() bool { return in.status().Gauges["logs_queue_batches"] == 1 })
	if s := in.status(); s.Counters["logs_rejected"] != 0 || in.requests() != 1 {
		t.Fatalf("%+v", s)
	}

	in.down.Store(false)
	in.advance(time.Minute)
	eventually(t, "the backlog", func() bool { return in.requests() == 2 })
}

// A detached Bot's data goes nowhere: no requests, an empty spool, and checkpoints that advance,
// so attaching again sends only what comes after.
func TestDetachedDoesNotGrowSpool(t *testing.T) {
	in := newInstance(t)
	in.logLine("attached")
	in.run()
	eventually(t, "the first delivery", func() bool { return in.requests() == 1 })

	in.noBot.Store(true)
	in.advance(6 * time.Minute)
	eventually(t, "the no-Bot state", func() bool { return in.status().States["credential"] == "no_bot" })
	in.logLine("detached")
	eventually(t, "the drop", func() bool { return in.status().Counters["logs_rejected"] == 1 })
	eventually(t, "a drop that outlasts the queue going idle", func() bool { return in.status().States["logs"] == "rejected:no_bot" })
	if s := in.status(); s.Gauges["logs_queue_batches"] != 0 || in.requests() != 1 {
		t.Fatalf("%+v, %d requests", s, in.requests())
	}

	in.noBot.Store(false)
	in.advance(credential.NoBotRetry)
	eventually(t, "the new key", func() bool { return in.status().States["credential"] == "loaded" })
	in.logLine("attached again")
	eventually(t, "the next delivery", func() bool { return in.requests() == 2 })
	in.mu.Lock()
	defer in.mu.Unlock()
	if b := string(in.bodies[1]); strings.Contains(b, "detached") || !strings.Contains(b, "attached again") {
		t.Fatal("the detached line reached ingest")
	}
}

// Stopped while detached, the agent must keep moving its checkpoints when it comes back still
// detached: otherwise attaching a Bot would send, and bill, everything since the detach.
func TestRestartWhileDetachedLeavesNoBacklog(t *testing.T) {
	in := newInstance(t)
	in.logLine("attached")
	stop := in.run()
	eventually(t, "the first delivery", func() bool { return in.requests() == 1 })
	stop()

	in.noBot.Store(true)
	in.logLine("written while detached")
	stop = in.run()
	eventually(t, "the drop", func() bool { return in.status().Counters["logs_rejected"] == 1 })
	stop()
	if in.requests() != 1 {
		t.Fatalf("%d requests", in.requests())
	}

	in.noBot.Store(false)
	in.logLine("attached again")
	in.run()
	eventually(t, "the next delivery", func() bool { return in.requests() == 2 })
	in.mu.Lock()
	defer in.mu.Unlock()
	if strings.Contains(string(in.bodies[1]), "written while detached") {
		t.Fatal("the detached line reached ingest")
	}
}

// Batches spooled by a Bot that has since gone are dropped, not kept for the next one.
func TestRestartWhileDetachedDropsPendingBatches(t *testing.T) {
	in := newInstance(t)
	dir := filepath.Join(in.config.StateDir, testInstance)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	binding := "test|" + in.ingest.URL + "|test|" + testInstance + "|" + testProject
	q, err := spool.Open(filepath.Join(dir, "logs.db"), binding, 256<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Enqueue([][]byte{logBatch(t)}, nil); err != nil {
		t.Fatal(err)
	}
	q.Close()

	in.noBot.Store(true)
	in.run()
	eventually(t, "the drop", func() bool { return in.status().Counters["logs_rejected"] == 1 })
	if in.requests() != 0 {
		t.Fatal("sent with no Bot")
	}
}

// A restart during a credentials outage is not a detach: sources run, batches spool, and they go
// out with the next key.
func TestRestartDuringCredentialsOutageSpools(t *testing.T) {
	in := newInstance(t)
	in.logLine("before the restart")
	stop := in.run()
	eventually(t, "the first delivery", func() bool { return in.requests() == 1 })
	stop()

	in.credsDown.Store(true)
	in.logLine("during the outage")
	in.run()
	eventually(t, "the batch in the spool", func() bool { return in.status().Gauges["logs_queue_batches"] == 1 })
	if s := in.status(); s.States["credential"] != "waiting:metadata" || s.States["logs"] != "paused:credential" || s.Counters["logs_rejected"] != 0 || in.requests() != 1 {
		t.Fatalf("%+v, %d requests", s, in.requests())
	}

	in.credsDown.Store(false)
	in.advance(time.Minute)
	eventually(t, "the backlog", func() bool { return in.requests() == 2 })
	in.mu.Lock()
	defer in.mu.Unlock()
	if !strings.Contains(string(in.bodies[1]), "during the outage") {
		t.Fatal("the spooled line wasn't sent")
	}
}

// A first boot with credentials failing still waits: nothing has been sent, so nothing is written.
func TestFirstBootDuringCredentialsOutageStaysIdle(t *testing.T) {
	in := newInstance(t)
	in.credsDown.Store(true)
	in.run()
	eventually(t, "the waiting state", func() bool { return in.status().States["credential"] == "waiting:metadata" })
	if got := in.stateFiles(); !slices.Equal(got, []string{"agent.lock", "status.sock"}) {
		t.Fatalf("state %v", got)
	}
}

func TestEndpointRefusalRecovers(t *testing.T) {
	in := newInstance(t)
	in.ingestStatus.Store(404)
	in.logLine("kept")
	in.run()
	eventually(t, "the refusal", func() bool { return in.requests() > 0 })
	in.ingestStatus.Store(200)
	eventually(t, "the delivery", func() bool { return in.status().Counters["logs_exported"] == 1 })
}

func TestEndpointRefusalThenDetachDrops(t *testing.T) {
	in := newInstance(t)
	in.ingestStatus.Store(404)
	in.logLine("refused")
	in.run()
	eventually(t, "the refusal", func() bool { return in.requests() > 0 })
	in.noBot.Store(true)
	in.advance(6 * time.Minute)
	eventually(t, "the drop", func() bool { return in.status().Counters["logs_rejected"] == 1 })
	if s := in.status(); s.States["logs"] != "rejected:no_bot" {
		t.Fatalf("%+v", s)
	}
}

// A revoked key doesn't wait for its rotation: the 401 fetches another now.
func Test401RefetchesAtOnce(t *testing.T) {
	in := newInstance(t)
	keyPoll = time.Hour
	in.ingestStatus.Store(401)
	in.logLine("kept")
	in.run()
	eventually(t, "the refusal", func() bool { return in.requests() > 0 })
	in.ingestStatus.Store(200)
	eventually(t, "the second key", func() bool { return in.minted.Load() == 2 })
	eventually(t, "the delivery with it", func() bool { return in.status().Counters["logs_exported"] == 1 })
}

// Ingest refusing every key must not turn into minting keys in a tight loop.
func TestEveryKeyRefusedDoesNotLoop(t *testing.T) {
	in := newInstance(t)
	keyPoll = time.Hour
	in.ingestStatus.Store(401)
	in.logLine("kept")
	in.run()
	eventually(t, "the second key's refusal", func() bool { return in.requests() >= 2 })
	time.Sleep(100 * time.Millisecond)
	if s, n := in.status(), in.minted.Load(); n != 2 || s.Gauges["logs_queue_batches"] > 1 || s.Counters["logs_rejected"] != 0 {
		t.Fatalf("%d keys minted, %+v", n, s)
	}
}

// What a clone inherited can never be sent, so it goes as soon as the ID is known, even idle.
func TestStaleInstanceStateRemoved(t *testing.T) {
	in := newInstance(t)
	state := in.config.StateDir
	for _, p := range []string{"ins_000000000000", testInstance, "ins_short", "backup"} {
		if err := os.MkdirAll(filepath.Join(state, p), 0700); err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(state, p, "keep"), nil, 0600)
	}
	os.WriteFile(filepath.Join(state, "ins_111111111111"), []byte("a file"), 0600)
	target := filepath.Join(in.dir, "elsewhere")
	os.MkdirAll(target, 0700)
	os.WriteFile(filepath.Join(target, "keep"), nil, 0600)
	os.Symlink(target, filepath.Join(state, "ins_222222222222"))
	// A v0.1 agent's queues, which aren't an instance's to clean up.
	os.WriteFile(filepath.Join(state, "logs.db"), []byte("x"), 0600)
	os.WriteFile(filepath.Join(state, "metrics.db"), []byte("x"), 0600)

	in.down.Store(true)
	in.run()
	eventually(t, "the waiting state", func() bool { return in.status().States["credential"] == "waiting:metadata" })
	if _, err := os.Stat(filepath.Join(state, "ins_000000000000")); err != nil {
		t.Fatal("removed before the ID was known")
	}
	// Answering only the ID: this instance's own directory must survive with no key yet.
	in.down.Store(false)
	in.noBot.Store(true)
	eventually(t, "the removal", func() bool {
		_, err := os.Stat(filepath.Join(state, "ins_000000000000"))
		return os.IsNotExist(err)
	})
	for _, p := range []string{filepath.Join(testInstance, "keep"), filepath.Join("ins_short", "keep"), filepath.Join("backup", "keep"), "ins_111111111111", "ins_222222222222", "logs.db", "metrics.db"} {
		if _, err := os.Lstat(filepath.Join(state, p)); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
	if _, err := os.Stat(filepath.Join(target, "keep")); err != nil {
		t.Fatal("followed a symlink")
	}
}

func queue(t *testing.T, signal string, dir string, payloads ...[]byte) *spool.Queue {
	t.Helper()
	q, err := spool.Open(filepath.Join(dir, signal+".db"), "binding", 8<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	if err = q.Enqueue(payloads, nil); err != nil {
		t.Fatal(err)
	}
	return q
}

// A Bot allowed metrics but not logs sends metrics and drops logs, each signal on its own.
func TestOneSignalRefusedOtherFlows(t *testing.T) {
	var metrics atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/logs" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		metrics.Add(1)
	}))
	defer server.Close()
	d := t.TempDir()
	logs, metric := logBatch(t), metricBatch(t)
	lq := queue(t, "logs", d, logs, logs, logs)
	mq := queue(t, "metrics", d, metric, metric, metric)
	client := exporter.NewWithClient(server.URL, server.Client(), "ezgh_key")
	client.DropForbidden = true
	state := diagnostics.New()
	var generation atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for signal, q := range map[string]*spool.Queue{"logs": lq, "metrics": mq} {
		wg.Go(func() { deliver(ctx, signal, q, client, state, &generation) })
	}
	eventually(t, "every batch handled", func() bool {
		a, _ := lq.Stats()
		b, _ := mq.Stats()
		return a.Batches+b.Batches == 0
	})
	// The refusal stays readable after the queue goes idle.
	eventually(t, "the named permission", func() bool { return state.Snapshot().States["logs"] == "rejected:permission:obs.logs.ingest" })
	time.Sleep(250 * time.Millisecond)
	cancel()
	wg.Wait()
	s := state.Snapshot()
	if metrics.Load() != 3 || s.Counters["logs_rejected"] != 3 || s.Counters["metrics_exported"] == 0 || s.Counters["metrics_rejected"] != 0 {
		t.Fatalf("%d metrics requests, %+v", metrics.Load(), s)
	}
	if got := s.States["logs"]; got != "rejected:permission:obs.logs.ingest" {
		t.Fatalf("state %q", got)
	}
}

// A refused signal asks ingest, and so org-api, at most once per cooldown, and the other signal
// doesn't wait for it.
func TestRefusedSignalCoolsDown(t *testing.T) {
	if refusedCooldown != 10*time.Second {
		t.Fatalf("cooldown %s", refusedCooldown)
	}
	var now atomic.Int64
	now.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	clock = func() time.Time { return time.Unix(0, now.Load()) }
	t.Cleanup(func() { clock = time.Now })
	var logRequests, metricRequests atomic.Int32
	var logStatus atomic.Int32
	logStatus.Store(http.StatusForbidden)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/logs" {
			logRequests.Add(1)
			w.WriteHeader(int(logStatus.Load()))
			return
		}
		metricRequests.Add(1)
	}))
	defer server.Close()
	d := t.TempDir()
	logs, metric := logBatch(t), metricBatch(t)
	lq := queue(t, "logs", d, logs, logs, logs)
	mq := queue(t, "metrics", d, metric)
	client := exporter.NewWithClient(server.URL, server.Client(), "ezgh_key")
	client.DropForbidden = true
	state := diagnostics.New()
	var generation atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for signal, q := range map[string]*spool.Queue{"logs": lq, "metrics": mq} {
		wg.Go(func() { deliver(ctx, signal, q, client, state, &generation) })
	}
	defer func() { cancel(); wg.Wait() }()
	rejected := func(n uint64) func() bool {
		return func() bool { return state.Snapshot().Counters["logs_rejected"] == n }
	}
	push := func(q *spool.Queue, b []byte) {
		t.Helper()
		if err := q.Enqueue([][]byte{b}, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Three batches, one request. The rest are acked and counted without being sent.
	eventually(t, "the first three dropped", rejected(3))
	if n := logRequests.Load(); n != 1 {
		t.Fatalf("%d log requests", n)
	}
	const refused = "rejected:permission:obs.logs.ingest"
	eventually(t, "the refusal state", func() bool { return state.Snapshot().States["logs"] == refused })

	// Still cooling down at the last instant, and metrics flow meanwhile.
	now.Add(int64(refusedCooldown - time.Nanosecond))
	push(lq, logs)
	eventually(t, "the local drop", rejected(4))
	push(mq, metric)
	eventually(t, "metrics during the cooldown", func() bool { return metricRequests.Load() == 2 })
	if n := logRequests.Load(); n != 1 {
		t.Fatalf("%d log requests inside the window", n)
	}
	if got := state.Snapshot(); got.States["logs"] != refused || got.Counters["metrics_rejected"] != 0 {
		t.Fatalf("%+v", got)
	}

	// The window over, the next batch is sent, and another 403 starts another window.
	now.Add(int64(time.Nanosecond))
	push(lq, logs)
	eventually(t, "the next send", func() bool { return logRequests.Load() == 2 })
	eventually(t, "the refusal", rejected(5))
	push(lq, logs)
	eventually(t, "the local drop", rejected(6))
	if n := logRequests.Load(); n != 2 {
		t.Fatalf("%d log requests", n)
	}

	// Permission granted: a 200 after the window, then every batch goes out.
	logStatus.Store(http.StatusOK)
	now.Add(int64(refusedCooldown))
	push(lq, logs)
	eventually(t, "the first batch after the grant", func() bool { return logRequests.Load() == 3 })
	eventually(t, "its acknowledgement", func() bool { return state.Snapshot().Counters["logs_exported"] == 1 })
	push(lq, logs)
	eventually(t, "the next batch sent at once", func() bool { return logRequests.Load() == 4 })
	eventually(t, "the state clearing", func() bool { return state.Snapshot().States["logs"] != refused })
	if n := state.Snapshot().Counters["logs_rejected"]; n != 6 {
		t.Fatalf("%d rejected after the grant", n)
	}
}

func logBatch(t *testing.T) []byte {
	t.Helper()
	p, err := telemetry.EncodeLogs([]*telemetry.LogItem{{Resource: telemetry.Resource("h", "h", "e", "s"), Record: telemetry.Log("line", time.Now(), 0, nil)}}, telemetry.MaxBatch)
	if err != nil || len(p) != 1 {
		t.Fatal(err)
	}
	return p[0]
}

func metricBatch(t *testing.T) []byte {
	t.Helper()
	f := &dto.MetricFamily{Name: proto.String("up"), Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: proto.Float64(1)}}}}
	p, err := telemetry.EncodeMetrics([]*dto.MetricFamily{f}, telemetry.Resource("h", "h", "e", "s"), time.Now(), 10, 1<<20)
	if err != nil || len(p) != 1 {
		t.Fatal(err)
	}
	return p[0]
}
