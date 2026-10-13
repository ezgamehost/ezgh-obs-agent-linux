package app

import (
	"context"
	"encoding/pem"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/config"
	"github.com/ezgamehost/ezgh-obs-agent-linux/internal/diagnostics"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
